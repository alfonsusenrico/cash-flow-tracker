package com.alfonsusenrico.financialtracker.listener

import androidx.test.platform.app.InstrumentationRegistry
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.model.RawNotificationEntity
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.google.gson.Gson
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Assume.assumeTrue
import org.junit.Test

/**
 * Fills the verification app with invented rows in every state, for screenshots and upgrade checks on a
 * device. Runs only with `-e seedSynthetic true` and only in the `.verification` app. The server address
 * is a loopback HTTP address, which the network policy refuses, so nothing leaves the phone.
 */
class SyntheticDeviceSeedTest {
    private val gson = Gson()

    @Test fun seedsSyntheticRowsAndPairing() = runBlocking {
        assumeTrue(InstrumentationRegistry.getArguments().getString("seedSynthetic") == "true")
        val context = InstrumentationRegistry.getInstrumentation().targetContext
        check(context.packageName.endsWith(".verification"))
        val prefs = AppPreferences(context)
        // The address first, so the synthetic key is never paired with the production server.
        TestEndpoint.set(context, "http://127.0.0.1:9")
        prefs.apiKey = "cfk_synthetic_device_check_only"
        val pairing = prefs.pairingContext
        // Non-default choices, so an upgrade check can tell kept settings from defaults.
        prefs.setPackageEnabled("com.bca", true)
        prefs.setPackageEnabled("com.stockbit.android", false)
        prefs.isAutoSyncEnabled = false
        val dao = AppDatabase.getDatabase(context).notificationDao()
        val now = System.currentTimeMillis()
        val minute = 60_000L
        val day = 24 * 60 * minute
        var inserted = 0

        suspend fun row(
            tag: String, packageName: String, appLabel: String, title: String, body: String, postTime: Long,
            synced: Boolean = true, status: String? = null, error: String? = null, result: Map<String, Any?>? = null,
            outcome: String = "send", reason: String? = null, purposeQuestionId: String? = null,
            pocketOptions: Map<String, Any?>? = null, amount: Double? = null, direction: String? = null,
            eventClass: String? = null, counterparty: String? = null
        ) {
            val eventId = result?.get("event_id") as String?
            val id = dao.insertOrIgnore(
                RawNotificationEntity(
                    deviceId = "synthetic-device", packageName = packageName, appLabel = appLabel,
                    title = title, bodyText = body, postTime = postTime, capturedAt = postTime,
                    payloadHash = "synthetic-$tag", isSynced = synced, syncedAt = if (synced) postTime else null,
                    backendEventId = eventId, processingStatus = status, processingError = error,
                    recordKey = result?.get("record_key") as String?,
                    editableTransactionId = (result?.get("transaction_id") as String?)
                        ?.takeIf { result?.get("type") in setOf("expense", "income") },
                    pairingContext = if (synced) pairing else null,
                    resultJson = result?.let(gson::toJson),
                    sourcePocketOptionsJson = pocketOptions?.let(gson::toJson),
                    captureOutcome = outcome, captureReason = reason, purposeQuestionId = purposeQuestionId,
                    isFinancial = amount?.let { true }, expectedAmount = amount, expectedDirection = direction,
                    eventClass = eventClass, expectedCounterparty = counterparty
                )
            )
            if (id > 0) inserted++
        }

        fun uuid(n: Int) = "00000000-0000-4000-8000-%012d".format(n)
        fun recorded(n: Int, type: String, description: String, amount: Long, account: String, category: String?, target: String? = null) =
            mapOf(
                "event_id" to uuid(n), "status" to "recorded", "record_key" to "synthetic:$n",
                "transaction_id" to uuid(n + 500), "type" to type, "description" to description,
                "source" to if (type == "income") null else account, "target" to if (type == "income") account else target,
                "category" to category, "amount" to amount, "currency" to "IDR"
            )

        val purpose = recorded(1, "expense", "Toko Sinar Abadi", 187_500, "BCA Harian", "Belanja Harian") + mapOf(
            "question" to mapOf(
                "type" to "purpose", "question_id" to uuid(901), "transaction_id" to uuid(501),
                "direction" to "expense", "amount" to 187_500, "currency" to "IDR",
                "prompt" to "Apa ini? Rp187.500 ke Toko Sinar Abadi dari BCA Harian",
                "allows_text" to true, "text_max_length" to 200,
                "suggestions" to listOf(
                    mapOf("id" to "s1", "kind" to "history", "label" to "Belanja harian", "category_name" to "Belanja Harian"),
                    mapOf("id" to "s2", "kind" to "history", "label" to "Makan keluarga", "category_name" to "Makan & Minum"),
                    mapOf("id" to "s3", "kind" to "current", "label" to "Perlengkapan rumah", "category_name" to "Rumah Tangga")
                )
            )
        )
        row("purpose", "com.bca.mybca.omni.android", "myBCA", "myBCA",
            "Transaksi QRIS Rp187.500,00 ke TOKO SINAR ABADI dari rek ••4821 berhasil.", now - 20 * minute,
            status = "recorded", result = purpose, purposeQuestionId = uuid(901))
        row("coffee", "com.gojek.gopay", "GoPay", "Pembayaran berhasil",
            "Kamu baru aja bayar Rp32.000 ke Kopi Senja pake GoPay.", now - 50 * minute,
            status = "recorded", result = recorded(2, "expense", "Kopi Senja", 32_000, "GoPay", "Makan & Minum"))
        row("income", "com.bca.mybca.omni.android", "myBCA", "myBCA",
            "Dana masuk Rp250.000,00 dari TOKO CONTOH ke rek ••4821.", now - 70 * minute,
            status = "recorded", result = recorded(3, "income", "Refund Toko Contoh", 250_000, "BCA Harian", "Pemasukan Lain"))
        row("move", "com.jago.digitalbanking", "Bank Jago", "Jago",
            "Rp500.000 dipindahkan dari Kantong Utama ke Dana Darurat.", now - 90 * minute,
            status = "recorded", result = recorded(4, "internal_movement", "Pindah ke Dana Darurat", 500_000,
                "Bank Jago · Kantong Utama", null, target = "Bank Jago · Dana Darurat"))
        row("sender", "com.bca.mybca.omni.android", "myBCA", "myBCA",
            "Transfer masuk Rp112.590,00 dari ***VEL ****ISS ke rek ••4821.", now - 110 * minute,
            status = "needs_confirmation", result = mapOf(
                "event_id" to uuid(5), "status" to "needs_confirmation",
                "confirmation" to mapOf(
                    "type" to "sender", "question_id" to uuid(905), "masked_sender" to "***VEL ****ISS",
                    "institution" to "bca", "receiving_account" to mapOf("id" to uuid(705), "label" to "BCA Harian"),
                    "amount" to 112_590, "currency" to "IDR"
                )
            ))
        row("pocket", "com.jago.digitalbanking", "Bank Jago", "Jago",
            "Kamu sudah transfer Rp75.000 ke ANDRA.", now - 130 * minute,
            status = "needs_confirmation", result = mapOf(
                "event_id" to uuid(6), "status" to "needs_confirmation",
                "confirmation" to mapOf(
                    "type" to "source_pocket", "question_id" to uuid(906), "institution" to "jago",
                    "jago_account" to mapOf("id" to uuid(706), "label" to "Bank Jago"),
                    "amount" to 75_000, "currency" to "IDR", "destination_label" to "Andra"
                )
            ), pocketOptions = mapOf(
                "event_id" to uuid(6), "question_id" to uuid(906),
                "options" to listOf(
                    mapOf("id" to uuid(716), "label" to "Bank Jago · Kantong Utama"),
                    mapOf("id" to uuid(726), "label" to "Bank Jago · Dana Darurat")
                )
            ))
        row("mapping", "com.bca.mybca.omni.android", "myBCA", "myBCA",
            "Transfer Rp300.000,00 dari rek ••7781 berhasil.", now - day - 60 * minute,
            status = "needs_confirmation", error = "mapping_confirmation_required", result = mapOf(
                "event_id" to uuid(7), "status" to "needs_confirmation", "error_code" to "mapping_confirmation_required",
                "mapping_proposal" to mapOf(
                    "name" to "BCA ••7781", "role" to "source", "confidence" to 0.72,
                    "proposed" to mapOf("id" to uuid(707), "label" to "BCA Tabungan"),
                    "alternatives" to listOf(mapOf("id" to uuid(717), "label" to "BCA Harian"))
                )
            ))
        row("review", "com.shopeepay.id", "ShopeePay", "Pesanan Shopee",
            "Pembayaran pesanan berhasil. Total Rp89.000 termasuk biaya layanan Rp1.000.", now - day - 120 * minute,
            status = "needs_review", error = "ambiguous_amount",
            result = mapOf("event_id" to uuid(8), "status" to "needs_review", "error_code" to "ambiguous_amount"))
        row("yesterday", "com.gojek.gopay", "GoPay", "Pembayaran berhasil",
            "Kamu baru aja bayar Rp24.000 untuk GoRide.", now - day - 180 * minute,
            status = "recorded", result = recorded(9, "expense", "GoRide", 24_000, "GoPay", "Transportasi"))
        row("twodays", "com.bca.mybca.omni.android", "myBCA", "myBCA",
            "Transaksi debit Rp58.000,00 di APOTEK SEHAT dari rek ••4821.", now - 2 * day - 60 * minute,
            status = "recorded", result = recorded(10, "expense", "Apotek Sehat", 58_000, "BCA Harian", "Kesehatan"))
        row("phone", "com.gojek.gopay", "GoPay", "Promo GoFood",
            "Diskon 50% GoFood hari ini, klaim vouchernya!", now - 40 * minute,
            synced = false, outcome = "keep_local", reason = "promotional")
        row("ignored", "com.stockbit.android", "Stockbit", "Info pasar",
            "IHSG ditutup menguat 0,8% hari ini.", now - 150 * minute,
            status = "ignored", result = mapOf("event_id" to uuid(11), "status" to "ignored"))
        row("unsent", "com.gojek.gopay", "GoPay", "Pembayaran berhasil",
            "Kamu baru aja bayar Rp45.000 ke Kedai Contoh pake GoPay.", now - 10 * minute,
            synced = false, amount = 45_000.0, direction = "out", eventClass = "expense", counterparty = "Kedai Contoh")

        assertEquals(13, inserted)
    }
}
