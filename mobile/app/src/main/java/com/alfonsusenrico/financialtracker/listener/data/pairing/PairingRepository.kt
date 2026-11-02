package com.alfonsusenrico.financialtracker.listener.data.pairing

import android.content.Context
import com.alfonsusenrico.financialtracker.listener.data.network.ApiClient
import com.alfonsusenrico.financialtracker.listener.data.network.PairRequest
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.data.prefs.ServerContact
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.io.IOException
import com.google.gson.JsonParseException
import java.util.UUID

/** Only safe, actionable messages cross from transport into the UI. */
data class PairingOutcome(val connected: Boolean, val message: String, val keySaved: Boolean = false)

class PairingRepository(context: Context) {
    private val prefs = AppPreferences(context)
    private val client = ApiClient(context)
    private var previousKey: String? = null
    private var previousOwner: String? = null
    private var previousKeyId: String? = null

    suspend fun exchange(code: PairingCode, deviceName: String): PairingOutcome = withContext(Dispatchers.IO) {
        try {
            rememberPreviousKey()
            val response = client.pairingService().exchange(PairRequest(code.value, prefs.deviceId, deviceName))
            val key = response.body()?.apiKey
            if (!response.isSuccessful || response.body()?.ok != true || key.isNullOrBlank()) {
                return@withContext failure(response.code())
            }
            if (!Regex("cfk_[A-Za-z0-9_-]{16,200}").matches(key)) {
                return@withContext PairingOutcome(false, "Jawaban server tidak valid. Buat kode QR baru lalu pindai lagi.")
            }
            prefs.apiKey = key
            verify()
        } catch (cancel: CancellationException) {
            throw cancel
        } catch (_: JsonParseException) {
            PairingOutcome(false, "Jawaban tracker tidak valid. Coba lagi dari Pengaturan web.", prefs.isConfigured)
        } catch (_: IOException) {
            PairingOutcome(false, "Koneksi terputus saat memasangkan. Buat kode QR baru di web lalu pindai lagi.")
        } catch (_: IllegalStateException) {
            PairingOutcome(false, "Kunci belum bisa disimpan. Buat kode QR baru lalu coba lagi.")
        }
    }

    suspend fun connectManual(rawKey: String): PairingOutcome = withContext(Dispatchers.IO) {
        val key = rawKey.trim()
        if (!Regex("cfk_[A-Za-z0-9_-]{16,200}").matches(key)) {
            return@withContext PairingOutcome(false, "API key tidak valid. Salin kunci lengkap dari Pengaturan web.")
        }
        try {
            val check = client.pairingService(key).identity()
            if (!check.isSuccessful || check.body()?.ok != true || check.body()?.user?.id.isNullOrBlank()) {
                return@withContext failure(check.code())
            }
            rememberPreviousKey()
            prefs.apiKey = key
            prefs.recordServerContact(ServerContact.OK)
            prefs.clearHealthRevoked()
            val revoked = revokePreviousKey(check.body()!!.user!!.id!!)
            PairingOutcome(true, if (revoked) "Terhubung. Kunci diterima oleh tracker."
                else "Terhubung. Kunci lama belum dicabut; buka Pengaturan web → Perangkat terhubung → Cabut.", true)
        } catch (cancel: CancellationException) {
            throw cancel
        } catch (_: JsonParseException) {
            PairingOutcome(false, "Jawaban tracker tidak valid. Coba lagi dari Pengaturan web.", prefs.isConfigured)
        } catch (_: IOException) {
            PairingOutcome(false, "Belum terhubung. Periksa internet lalu coba Tes Koneksi lagi.")
        } catch (_: IllegalStateException) {
            PairingOutcome(false, "Kunci belum bisa disimpan. Coba lagi.")
        }
    }

    suspend fun verify(): PairingOutcome = withContext(Dispatchers.IO) {
        if (prefs.apiKey.isBlank()) return@withContext PairingOutcome(false, "Hubungkan ponsel terlebih dahulu.")
        try {
            val response = client.pairingService(prefs.apiKey).identity()
            val owner = response.body()?.user?.id
            if (!response.isSuccessful || response.body()?.ok != true || owner.isNullOrBlank()) {
                if (response.code() in setOf(401, 403)) prefs.recordServerContact(ServerContact.REFUSED)
                return@withContext failure(response.code()).copy(keySaved = true)
            }
            prefs.recordServerContact(ServerContact.OK)
            prefs.clearHealthRevoked()
            val revoked = revokePreviousKey(owner)
            PairingOutcome(true, if (revoked) "Terhubung. Kunci diterima oleh tracker."
                else "Terhubung. Kunci lama belum dicabut; buka Pengaturan web → Perangkat terhubung → Cabut.", true)
        } catch (cancel: CancellationException) {
            throw cancel
        } catch (_: JsonParseException) {
            PairingOutcome(false, "Jawaban tracker tidak valid. Coba lagi dari Pengaturan web.", prefs.isConfigured)
        } catch (_: IOException) {
            PairingOutcome(false, "Kunci sudah tersimpan. Periksa internet lalu coba Tes Koneksi lagi.", true)
        }
    }

    private suspend fun rememberPreviousKey() {
        val old = prefs.apiKey
        if (old.isBlank() || previousKey != null) return
        try {
            val service = client.pairingService(old)
            val identity = service.identity()
            val owner = identity.body()?.user?.id
            if (!identity.isSuccessful || identity.body()?.ok != true || owner.isNullOrBlank()) return
            val devices = service.devices()
            if (!devices.isSuccessful || devices.body()?.ok != true) return
            val id = devices.body()?.devices?.singleOrNull { it.prefix == old.take(8) }?.id ?: return
            UUID.fromString(id)
            previousKey = old
            previousOwner = owner
            previousKeyId = id
        } catch (cancel: CancellationException) {
            throw cancel
        } catch (_: IOException) {
            // Exchange can proceed offline from the previous account; web settings retains its key.
        } catch (_: IllegalArgumentException) {
            // An unrecognizable device id is never sent to a revoke endpoint.
        }
    }

    private suspend fun revokePreviousKey(owner: String): Boolean {
        val old = previousKey ?: return true
        if (previousOwner != owner || old == prefs.apiKey) return true
        val id = previousKeyId ?: return false
        return try {
            val response = client.pairingService(prefs.apiKey).revoke(id)
            val done = response.isSuccessful || response.code() == 404
            response.body()?.close()
            if (done) {
                previousKey = null
                previousOwner = null
                previousKeyId = null
            }
            done
        } catch (cancel: CancellationException) {
            throw cancel
        } catch (_: IOException) {
            false
        }
    }

    private fun failure(status: Int) = PairingOutcome(false, when (status) {
        410 -> "Kode QR sudah kedaluwarsa atau dipakai. Buat kode baru di web lalu Pindai lagi."
        429 -> "Terlalu banyak percobaan. Tunggu sebentar sebelum mencoba lagi."
        401, 403 -> "Kunci ditolak server. Buat kode QR baru di Pengaturan web lalu pindai lagi."
        else -> "Tracker belum bisa dihubungi. Periksa internet lalu coba lagi."
    })
}
