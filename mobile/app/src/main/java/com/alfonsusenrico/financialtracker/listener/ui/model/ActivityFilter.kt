package com.alfonsusenrico.financialtracker.listener.ui.model

import com.alfonsusenrico.financialtracker.listener.data.model.RawNotificationEntity

/** Aktivitas filters: the five status filters first, then the per-app filters the companion always had. */
enum class ActivityFilter(val label: String, val byApp: Boolean = false) {
    ALL("Semua"),
    NEEDS_ANSWER("Perlu dijawab"),
    PENDING("Perlu cek"),
    LOCAL("Di ponsel"),
    IGNORED("Diabaikan"),
    MYBCA("myBCA", byApp = true),
    BCA_MOBILE("BCA mobile", byApp = true),
    BANK_JAGO("Bank Jago", byApp = true),
    GOPAY("GoPay", byApp = true),
    SHOPEEPAY("ShopeePay", byApp = true),
    STOCKBIT("Stockbit", byApp = true);

    /**
     * The rows of this filter. [visible] are the send rows that are neither ignored nor dismissed, as the
     * DAO lists them; [questions] are the open questions.
     */
    fun select(
        visible: List<RawNotificationEntity>,
        ignored: List<RawNotificationEntity>,
        local: List<RawNotificationEntity>,
        questions: List<RawNotificationEntity>
    ): List<RawNotificationEntity> = when (this) {
        ALL -> visible
        NEEDS_ANSWER -> questions
        PENDING -> visible.filter(::needsCheck)
        LOCAL -> local
        IGNORED -> ignored
        MYBCA -> visible.filter { it.packageName.contains("mybca", ignoreCase = true) }
        BCA_MOBILE -> visible.filter { it.packageName == "com.bca" }
        BANK_JAGO -> visible.filter { it.packageName.contains("jago", ignoreCase = true) }
        GOPAY -> visible.filter { it.packageName.contains("gopay", ignoreCase = true) || it.packageName == "com.gojek.app" }
        SHOPEEPAY -> visible.filter { it.packageName.contains("shopee", ignoreCase = true) }
        STOCKBIT -> visible.filter { it.packageName.contains("stockbit", ignoreCase = true) }
    }

    /**
     * The typed captures of this filter. They come from no monitored app and ask in their own sheet, so
     * only Semua, Perlu cek (not yet recorded) and Di ponsel (not yet accepted by the tracker) list them.
     */
    fun selectCaptures(captures: List<CapturePresentation>): List<CapturePresentation> = when (this) {
        ALL -> captures
        PENDING -> captures.filter { it.isUnsettled }
        LOCAL -> captures.filter { it.isOnPhoneOnly }
        else -> emptyList()
    }

    companion object {
        private val UNSETTLED = setOf("queued", "processing", "needs_review", "needs_confirmation", "failed")

        /** "Perlu cek": not yet sent, not yet settled by the tracker, or settled with an error. */
        fun needsCheck(row: RawNotificationEntity): Boolean =
            !row.isSynced || row.processingStatus in UNSETTLED || row.processingError != null
    }
}
