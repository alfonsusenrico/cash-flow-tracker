package com.alfonsusenrico.financialtracker.listener.ui

import com.alfonsusenrico.financialtracker.listener.data.model.RawNotificationEntity
import com.google.gson.Gson
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZoneId

/** Invented rows in every state the companion shows; amounts, merchants and accounts are fictional. */
object UiFixtures {
    const val PAIRING = "fictional-pairing"
    val zone: ZoneId = ZoneId.systemDefault()
    val today: LocalDate = LocalDate.of(2026, 10, 29)

    fun uuid(n: Int) = "00000000-0000-4000-8000-%012d".format(n)

    fun at(date: LocalDate, hour: Int, minute: Int = 0): Long =
        date.atTime(LocalTime.of(hour, minute)).atZone(zone).toInstant().toEpochMilli()

    private val gson = Gson()

    private fun row(
        id: Long, title: String, body: String, postTime: Long, app: String = "myBCA",
        packageName: String = "com.bca.mybca.omni.android", synced: Boolean = true, status: String? = null,
        error: String? = null, result: Map<String, Any?>? = null, outcome: String = "send", reason: String? = null,
        purposeQuestionId: String? = null, amount: Double? = null, direction: String? = null, eventClass: String? = null,
        counterparty: String? = null, pocketOptions: Map<String, Any?>? = null, pairing: String = PAIRING
    ) = RawNotificationEntity(
        id = id, deviceId = "fictional-device", packageName = packageName, appLabel = app, title = title,
        bodyText = body, postTime = postTime, payloadHash = "fictional-$id", isSynced = synced,
        backendEventId = result?.get("event_id") as String?, processingStatus = status, processingError = error,
        recordKey = result?.get("record_key") as String?,
        editableTransactionId = (result?.get("transaction_id") as String?)?.takeIf { result?.get("type") in setOf("expense", "income") },
        pairingContext = if (synced) pairing else null, resultJson = result?.let(gson::toJson),
        sourcePocketOptionsJson = pocketOptions?.let(gson::toJson), captureOutcome = outcome, captureReason = reason,
        purposeQuestionId = purposeQuestionId, isFinancial = amount?.let { true }, expectedAmount = amount,
        expectedDirection = direction, eventClass = eventClass, expectedCounterparty = counterparty
    )

    fun recordedResult(n: Int, type: String, description: String, amount: Long, account: String, category: String?, target: String? = null) =
        mapOf(
            "event_id" to uuid(n), "status" to "recorded", "record_key" to "fictional:$n",
            "transaction_id" to uuid(n + 500), "type" to type, "description" to description,
            "source" to if (type == "income") null else account,
            "target" to if (type == "income") account else target,
            "category" to category, "amount" to amount, "currency" to "IDR"
        )

    fun purpose(id: Long = 1, date: LocalDate = today, pairing: String = PAIRING) = row(
        id, "myBCA", "Transaksi QRIS Rp187.500,00 ke TOKO CONTOH ABADI dari rek ••0001 berhasil.", at(date, 10, 15),
        status = "recorded", pairing = pairing, purposeQuestionId = uuid(901),
        result = recordedResult(1, "expense", "Toko Contoh Abadi", 187_500, "BCA Contoh", "Belanja Harian") + mapOf(
            "question" to mapOf(
                "type" to "purpose", "question_id" to uuid(901), "transaction_id" to uuid(501),
                "direction" to "expense", "amount" to 187_500, "currency" to "IDR",
                "prompt" to "Apa ini? Rp187.500 ke Toko Contoh Abadi dari BCA Contoh",
                "allows_text" to true, "text_max_length" to 200,
                "suggestions" to listOf(
                    mapOf("id" to "s1", "kind" to "history", "label" to "Belanja harian", "category_name" to "Belanja Harian"),
                    mapOf("id" to "s2", "kind" to "history", "label" to "Makan keluarga", "category_name" to "Makan & Minum")
                )
            )
        )
    )

    fun recorded(id: Long, description: String, amount: Long, date: LocalDate = today, hour: Int = 9, category: String? = "Makan & Minum", type: String = "expense") = row(
        id, "Pembayaran berhasil", "Kamu baru aja bayar Rp$amount ke $description.", at(date, hour),
        app = "GoPay", packageName = "com.gojek.gopay", status = "recorded",
        result = recordedResult(id.toInt() + 10, type, description, amount, "GoPay Contoh", category)
    )

    /** An expense the tracker recorded from the owner's habits instead of asking (tracker 1.8.0 and later). */
    fun habit(id: Long = 24, date: LocalDate = today) = row(
        id, "Pembayaran berhasil", "Kamu baru aja bayar Rp27.000 ke Warung Contoh.", at(date, 12, 10),
        app = "GoPay", packageName = "com.gojek.gopay", status = "recorded",
        result = recordedResult(id.toInt() + 10, "expense", "Warung Contoh", 27_000, "GoPay Contoh", "Makan & Minum") + mapOf(
            "review_status" to "reviewed",
            "habit" to mapOf("label" to "Dicatat dari kebiasaan: Makan & Minum", "basis" to "3 dari 3 jawaban sebelumnya")
        )
    )

    fun movement(id: Long = 3) = row(
        id, "Jago", "Rp500.000 dipindahkan dari Kantong Utama ke Dana Contoh.", at(today, 8),
        app = "Bank Jago", packageName = "com.jago.digitalbanking", status = "recorded",
        result = recordedResult(3, "internal_movement", "Pindah ke Dana Contoh", 500_000, "Jago · Kantong Utama", null, target = "Jago · Dana Contoh")
    )

    fun sender(id: Long = 4) = row(
        id, "myBCA", "Transfer masuk Rp112.590,00 dari ***LA ****NI ke rek ••0001.", at(today, 9, 15),
        status = "needs_confirmation", result = mapOf(
            "event_id" to uuid(4), "status" to "needs_confirmation",
            "confirmation" to mapOf(
                "type" to "sender", "question_id" to uuid(904), "masked_sender" to "***LA ****NI",
                "institution" to "bca", "receiving_account" to mapOf("id" to uuid(704), "label" to "BCA Contoh"),
                "amount" to 112_590, "currency" to "IDR"
            )
        )
    )

    fun pocket(id: Long = 5, suggested: String? = null) = row(
        id, "Jago", "Kamu sudah transfer Rp75.000 ke CONTOH.", at(today, 8, 55),
        app = "Bank Jago", packageName = "com.jago.digitalbanking", status = "needs_confirmation", result = mapOf(
            "event_id" to uuid(5), "status" to "needs_confirmation",
            "confirmation" to mapOf(
                "type" to "source_pocket", "question_id" to uuid(905), "institution" to "jago",
                "jago_account" to mapOf("id" to uuid(705), "label" to "Bank Jago"),
                "amount" to 75_000, "currency" to "IDR", "destination_label" to "Contoh"
            ) + (suggested?.let { mapOf("suggested_account_id" to it) } ?: emptyMap())
        ),
        pocketOptions = mapOf(
            "event_id" to uuid(5), "question_id" to uuid(905),
            "options" to listOf(mapOf("id" to uuid(715), "label" to "Bank Jago · Kantong Utama"), mapOf("id" to uuid(725), "label" to "Bank Jago · Dana Contoh"))
        )
    )

    fun mapping(id: Long = 6) = row(
        id, "myBCA", "Transfer Rp300.000,00 dari rek ••0002 berhasil.", at(today.minusDays(1), 10, 5),
        status = "needs_confirmation", error = "mapping_confirmation_required", result = mapOf(
            "event_id" to uuid(6), "status" to "needs_confirmation", "error_code" to "mapping_confirmation_required",
            "mapping_proposal" to mapOf(
                "name" to "BCA ••0002", "role" to "source", "confidence" to 0.72,
                "proposed" to mapOf("id" to uuid(706), "label" to "BCA Tabungan Contoh"),
                "alternatives" to listOf(mapOf("id" to uuid(716), "label" to "BCA Contoh"))
            )
        )
    )

    fun review(id: Long = 7) = row(
        id, "Pesanan Contoh", "Pembayaran berhasil. Total Rp89.000 termasuk biaya layanan Rp1.000.", at(today.minusDays(1), 9, 5),
        app = "ShopeePay", packageName = "com.shopeepay.id", status = "needs_review", error = "ambiguous_amount",
        result = mapOf("event_id" to uuid(7), "status" to "needs_review", "error_code" to "ambiguous_amount")
    )

    fun phone(id: Long = 8) = row(
        id, "Promo Contoh", "Diskon 50% hari ini, klaim vouchernya!", at(today, 11),
        app = "GoPay", packageName = "com.gojek.gopay", synced = false, outcome = "keep_local", reason = "promotional"
    )

    fun ignored(id: Long = 9) = row(
        id, "Info pasar", "Indeks ditutup menguat hari ini.", at(today, 7),
        app = "Stockbit", packageName = "com.stockbit.android", status = "ignored",
        result = mapOf("event_id" to uuid(9), "status" to "ignored")
    )

    fun unsent(id: Long = 10) = row(
        id, "Pembayaran berhasil", "Kamu baru aja bayar Rp45.000 ke Kedai Contoh pake GoPay.", at(today, 11, 30),
        app = "GoPay", packageName = "com.gojek.gopay", synced = false, amount = 45_000.0, direction = "out",
        eventClass = "expense", counterparty = "Kedai Contoh"
    )
}
