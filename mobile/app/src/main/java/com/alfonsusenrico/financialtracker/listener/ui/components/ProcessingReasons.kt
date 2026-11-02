package com.alfonsusenrico.financialtracker.listener.ui.components

/**
 * Indonesian explanations for backend and companion processing codes.
 *
 * Server-side retryable codes mirror RETRYABLE_FAILURE_CODES in the backend
 * notification_processing module: those events are retried by the server without owner action.
 */
object ProcessingReasons {

    val SERVER_RETRYABLE_CODES = setOf(
        "provider_timeout", "provider_unavailable", "database_unavailable", "provider_access_denied",
        "provider_credential_missing", "provider_model_not_permitted", "provider_reasoning_not_permitted",
        "provider_request_rejected", "ai_disabled", "processor_configuration_invalid",
        "worker_attempts_exhausted"
    )

    private const val MAPPING = "Rekening atau kategori tidak bisa dipastikan otomatis. Pilih rekening dan kategori lalu catat manual."
    private const val POCKET = "Kantong atau rekening di notifikasi belum terdaftar di tracker. Tambahkan kantong dengan nama yang sama di Rekening & Kantong (web), lalu ketuk Coba Proses Ulang."
    private const val EVIDENCE = "Isi notifikasi tidak cukup jelas untuk dicatat otomatis. Periksa nominal dan arah transaksi lalu catat manual."
    private const val MODEL = "Penafsir otomatis tidak yakin dengan notifikasi ini. Coba ulang, atau pilih rekening dan kategori lalu catat manual."
    private const val MOVEMENT = "Ada lebih dari satu kemungkinan pasangan pemindahan saldo. Catatan tetap disimpan; gabungkan manual di Buku Kas web bila perlu."
    private const val SERVER_RETRY = "Server sedang tidak dapat memproses notifikasi. Server akan mengulang otomatis; tidak perlu tindakan."
    private const val BROKER = "Notifikasi saham tidak cocok dengan posisi atau rekening dana di tracker. Periksa pengaturan akun investasi."

    private val MESSAGES: Map<String, String> = buildMap {
        listOf(
            "uncertain_institution_mapping", "invalid_default_pocket", "ambiguous_main_pocket",
            "main_pocket_unconfigured", "uncertain_pocket_mapping", "same_movement_endpoint"
        ).forEach { put(it, POCKET) }
        listOf(
            "uncertain_category_mapping", "movement_category_missing", "incomplete_movement",
            "conflicting_movement_endpoints", "conflicting_observed_account", "invalid_account_reference",
            "invalid_category_reference", "invalid_candidate_reference", "invalid_current_category",
            "invalid_current_account", "invalid_current_reference"
        ).forEach { put(it, MAPPING) }
        listOf(
            "message_too_large", "timestamp_timezone_required", "invalid_amount", "fractional_amount",
            "direction_unproven", "conflicting_direction", "conflicting_amount"
        ).forEach { put(it, EVIDENCE) }
        listOf(
            "model_uncertain", "provider_invalid_output", "invalid_interpretation", "fabricated_amount_evidence",
            "fabricated_direction_evidence", "fabricated_endpoint_evidence", "income_kakeibo_not_permitted"
        ).forEach { put(it, MODEL) }
        listOf("ambiguous_existing_movement", "movement_role_consumed", "counterpart_changed", "ambiguous_movement")
            .forEach { put(it, MOVEMENT) }
        SERVER_RETRYABLE_CODES.forEach { put(it, SERVER_RETRY) }
        listOf(
            "unsupported_broker_event", "conflicting_trade_price", "conflicting_trade_quantity",
            "invalid_trade_amount", "broker_funding_missing", "ambiguous_broker_position", "broker_position_missing"
        ).forEach { put(it, BROKER) }
        put("ambiguous_amount", "Notifikasi memuat lebih dari satu nominal (misalnya biaya dan total). Isi nominal yang benar lalu catat manual.")
        put("missing_amount", "Notifikasi tidak memuat nominal. Isi nominal lalu catat manual bila ini transaksi.")
        put("settlement_unproven", "Notifikasi belum menyatakan transaksi berhasil. Catat manual hanya jika transaksi sudah terjadi.")
        put("unsupported_currency", "Mata uang selain Rupiah belum didukung. Catat manual dalam Rupiah bila perlu.")
        put("unsafe_instructions", "Notifikasi berisi teks mencurigakan sehingga tidak diproses otomatis. Periksa lalu catat manual bila benar.")
        put("conflicting_mobile_amount", "Nominal yang dibaca aplikasi berbeda dengan server. Periksa nominal lalu catat manual.")
        put("conflicting_mobile_direction", "Arah transaksi yang dibaca aplikasi berbeda dengan server. Periksa lalu catat manual.")
        put("mapping_confirmation_required", "Nama rekening di notifikasi belum dikenal. Konfirmasi rekening yang diusulkan atau pilih rekening lain; pilihan Anda diingat untuk notifikasi berikutnya.")
        put("invalid_mapping_proposal", MAPPING)
        put("incomplete_context", "Data rekening terlalu banyak untuk diproses otomatis. Catat manual.")
        put("legacy_review_required", "Notifikasi lama ini dicatat sebelum pemrosesan baru. Periksa lalu catat manual bila belum ada.")
        put("auth_rejected", "Kunci API ditolak server. Perbarui kunci API di Pengaturan.")
        put("invalid_request", "Server menolak data notifikasi ini. Kirim ulang setelah aplikasi diperbarui.")
        put("server_unavailable", "Server tidak dapat dihubungi. Aplikasi akan mencoba lagi.")
        put("network_unavailable", "Tidak ada koneksi internet. Aplikasi akan mencoba lagi.")
        put("event_not_found", "Notifikasi tidak ditemukan di server. Kirim ulang dari aplikasi.")
        listOf("missing_result", "result_mismatch", "invalid_result", "missing_event_id", "invalid_recorded_result")
            .forEach { put(it, "Balasan server tidak lengkap. Ketuk Sinkron untuk memeriksa ulang.") }
    }

    fun isServerRetrying(code: String?): Boolean = code != null && code in SERVER_RETRYABLE_CODES

    fun hasMessage(code: String): Boolean = code in MESSAGES

    fun describe(code: String?): String? {
        if (code.isNullOrBlank()) return null
        return MESSAGES[code] ?: "Perlu diperiksa (kode: $code)."
    }
}

/** Indonesian explanations for why the companion kept a notification on the phone. */
object CaptureReasons {
    fun describeKept(reason: String?): String = when (reason) {
        "promotional" -> "Terlihat seperti promo atau penawaran."
        "market_info" -> "Terlihat seperti info pasar atau berita saham."
        "headline_figures" -> "Hanya memuat angka ringkas seperti Rp1,5 T, bukan nominal transaksi."
        "no_amount" -> "Tidak memuat nominal Rupiah."
        "hard_noise" -> "Menyebut transaksi gagal, tertunda, atau belum dibayar."
        "settlement_unproven" -> "Tidak menyatakan transaksi berhasil atau arah uangnya."
        else -> "Tidak dikenali sebagai transaksi."
    }
}
