package com.alfonsusenrico.financialtracker.listener.ui.model

import com.alfonsusenrico.financialtracker.listener.data.model.RawNotificationEntity
import com.alfonsusenrico.financialtracker.listener.data.sync.validUuid
import com.alfonsusenrico.financialtracker.listener.ui.components.ProcessingReasons

/** What the owner may do with a row on this phone; the same rules the detail sheet applied before 2.0. */
data class RowAbilities(
    /** A recorded expense or income of the current pairing: "Ubah transaksi" patches it. */
    val canEditRecorded: Boolean,
    /** Not sent yet: the form stores labels and sends it. */
    val canSubmit: Boolean,
    /** Accepted but unresolved under the current pairing: "Coba Proses Ulang". */
    val canRetry: Boolean,
    /** Unresolved and not a pocket move or a question: "Catat manual". */
    val canResolve: Boolean,
    val editableTransactionId: String?,
    /** Why the row needs the owner, shown in the state box; null when nothing is wrong. */
    val reason: String?,
    val serverRetrying: Boolean
) {
    companion object {
        private val UNRESOLVED = setOf("needs_review", "needs_confirmation", "failed")

        fun of(item: RowPresentation, currentPairing: String): RowAbilities {
            val row: RawNotificationEntity = item.row
            val samePairing = row.pairingContext == currentPairing
            val editableId = row.editableTransactionId?.takeIf(::validUuid)
            val unresolved = row.processingStatus in UNRESOLVED
            val asksQuestion = row.processingStatus == "needs_confirmation" && item.question != null
            val canRetry = row.isSynced && unresolved && validUuid(row.backendEventId) && samePairing
            val reason = if (!asksQuestion && (unresolved || row.processingError != null)) {
                ProcessingReasons.describe(row.processingError)
                    ?: "Notifikasi belum bisa dicatat otomatis. Coba ulang atau catat manual."
            } else null
            return RowAbilities(
                canEditRecorded = row.isSynced && row.processingStatus == "recorded" && row.processingError == null &&
                    editableId != null && item.recorded?.type in setOf("expense", "income") && samePairing,
                canSubmit = !row.isSynced,
                canRetry = canRetry,
                // A pocket-to-pocket move is not an expense or income; recording one leg manually would misstate balances.
                canResolve = canRetry && row.expectedDirection != "internal" && !asksQuestion,
                editableTransactionId = editableId,
                reason = reason,
                serverRetrying = ProcessingReasons.isServerRetrying(row.processingError)
            )
        }
    }
}
