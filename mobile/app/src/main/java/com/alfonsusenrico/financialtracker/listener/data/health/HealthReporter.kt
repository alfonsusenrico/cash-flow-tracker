package com.alfonsusenrico.financialtracker.listener.data.health

import android.content.Context
import android.os.PowerManager
import android.util.Log
import androidx.core.app.NotificationManagerCompat
import com.alfonsusenrico.financialtracker.listener.BuildConfig
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.network.ApiClient
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.data.prefs.ServerContact
import com.alfonsusenrico.financialtracker.listener.service.FinancialNotificationListenerService
import kotlinx.coroutines.CancellationException
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.RequestBody.Companion.toRequestBody

/**
 * Sends the phone's health to the tracker (`POST /api/devices/health`). The server resolves the device from
 * the Bearer key, so the body carries no device id. A tracker without the endpoint answers 404: that is an
 * older server, not a fault, and it is never shown to the owner.
 */
class HealthReporter(
    private val context: Context,
    private val prefs: AppPreferences = AppPreferences(context),
    private val collect: suspend () -> HealthSnapshot = { HealthSnapshots.collect(context) },
    private val now: () -> Long = System::currentTimeMillis
) {
    enum class Outcome {
        /** The tracker stored the report. */
        SENT,
        /** An older tracker without the endpoint (404); asked again later with backoff. */
        UNSUPPORTED,
        /** Unreachable, rate limited (429, after its `Retry-After`) or a server error: try again with backoff. */
        RETRY,
        /** The key was refused (401 `device_key_invalid`): no more reports until the phone is paired again. */
        REFUSED,
        /** The tracker refused the body (422); a bug on this side, not worth repeating. */
        REJECTED,
        NOT_PAIRED
    }

    suspend fun send(): Outcome {
        if (!prefs.isConfigured) return Outcome.NOT_PAIRED
        // A revoked key stays revoked; the hub shows the re-pair problem, and pairing again resumes reports.
        if (prefs.isHealthRevoked) return Outcome.REFUSED
        if (now() < prefs.healthNotBefore) return Outcome.RETRY
        val pairing = prefs.pairingContext
        val body = collect().toJson().toRequestBody(JSON)
        var retryAfter: Long? = null
        val outcome = try {
            val response = ApiClient(context).createService(pairing).reportHealth(body)
            response.body()?.close()
            response.errorBody()?.close()
            if (response.code() == 429) retryAfter = retryAfterMillis(response.headers()["Retry-After"])
            when (response.code()) {
                in 200..299 -> Outcome.SENT
                404 -> Outcome.UNSUPPORTED
                401, 403 -> Outcome.REFUSED
                422 -> Outcome.REJECTED
                else -> Outcome.RETRY
            }.also { if (it == Outcome.RETRY) Log.w(TAG, "Health report deferred: status=${response.code()}") }
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Exception) {
            Outcome.RETRY
        }
        if (prefs.pairingContext != pairing) return outcome
        when (outcome) {
            Outcome.SENT -> {
                prefs.healthReportedAt = now()
                prefs.healthUnsupportedAt = 0L
                prefs.recordServerContact(ServerContact.OK)
            }
            Outcome.UNSUPPORTED -> prefs.healthUnsupportedAt = now()
            Outcome.REFUSED -> {
                prefs.markHealthRevoked()
                prefs.recordServerContact(ServerContact.REFUSED)
            }
            // A rate limit is not an unreachable server; it only waits.
            Outcome.RETRY -> if (retryAfter != null) prefs.healthNotBefore = now() + retryAfter!!
                else if (prefs.serverContact != ServerContact.REFUSED) prefs.recordServerContact(ServerContact.UNREACHABLE)
            Outcome.REJECTED -> Log.w(TAG, "Health report refused: reason=validation")
            Outcome.NOT_PAIRED -> Unit
        }
        return outcome
    }

    /** `Retry-After` in seconds (the tracker sends 60); a missing or odd value waits a minute, at most an hour. */
    private fun retryAfterMillis(header: String?): Long =
        (header?.trim()?.toLongOrNull()?.takeIf { it > 0 } ?: 60L).coerceAtMost(3600L) * 1000L

    private companion object {
        const val TAG = "HealthReporter"
        val JSON = "application/json; charset=utf-8".toMediaType()
    }
}

/** Reads the phone's current health from Android, the preferences and the local queue. */
object HealthSnapshots {
    suspend fun collect(context: Context): HealthSnapshot {
        val prefs = AppPreferences(context)
        val db = AppDatabase.getDatabase(context)
        val waiting = db.notificationDao().countUnsubmitted(prefs.pairingContext) + db.textCaptureDao().countWaiting()
        return HealthSnapshot(
            appVersion = BuildConfig.VERSION_NAME,
            listenerConnected = FinancialNotificationListenerService.isConnected.value,
            notificationAccess = FinancialNotificationListenerService.isPermissionGranted(context),
            postNotifications = NotificationManagerCompat.from(context).areNotificationsEnabled(),
            batteryUnrestricted = batteryUnrestricted(context),
            selfTest = SelfTestState(prefs.selfTestStatus, prefs.selfTestFailedStep, prefs.selfTestAt),
            lastCaptureAt = prefs.lastCaptureAt,
            queuePending = waiting
        )
    }

    fun batteryUnrestricted(context: Context): Boolean =
        (context.getSystemService(Context.POWER_SERVICE) as PowerManager).isIgnoringBatteryOptimizations(context.packageName)
}
