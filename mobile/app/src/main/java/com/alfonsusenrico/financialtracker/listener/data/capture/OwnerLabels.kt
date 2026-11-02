package com.alfonsusenrico.financialtracker.listener.data.capture

import com.alfonsusenrico.financialtracker.listener.data.db.NotificationDao

/**
 * The owner's corrections from the inbox sheet, kept as labels on the captured row.
 *
 * The sheet edits only the type, amount and note. Every other captured label, such as the
 * counterparty, keeps its captured value.
 */
class OwnerLabels(
    private val dao: NotificationDao,
    private val now: () -> Long = System::currentTimeMillis
) {

    /**
     * Labels a row that has not been sent and joins it to the send pipeline, since labelling a row
     * kept on the phone is the owner's decision to send it. Returns false when the row was sent.
     */
    suspend fun labelForSend(rowId: Long, amount: Long, type: String, notes: String?): Boolean {
        val row = dao.getById(rowId) ?: return false
        if (row.isSynced) return false
        dao.sendLocal(rowId)
        save(rowId, amount, type, notes, row.expectedDirection)
        return true
    }

    /** Mirrors a ledger edit the server accepted onto the row's labels. */
    suspend fun labelEdited(rowId: Long, amount: Long, type: String, notes: String?) {
        val row = dao.getById(rowId) ?: return
        save(rowId, amount, type, notes, row.expectedDirection)
    }

    private suspend fun save(rowId: Long, amount: Long, type: String, notes: String?, capturedDirection: String?) {
        dao.saveOwnerLabel(
            id = rowId,
            eventClass = type,
            expectedAmount = amount.toDouble(),
            expectedDirection = directionFor(type, capturedDirection),
            labelNotes = notes,
            labelledAt = now()
        )
    }

    companion object {
        /**
         * The sheet only chooses expense or income. Any other type is the captured one, such as a
         * pocket move, so its captured direction stays; the backend flags a direction that
         * disagrees with the notification for review.
         */
        fun directionFor(type: String, capturedDirection: String?): String? = when (type) {
            "income" -> "in"
            "expense" -> "out"
            else -> capturedDirection
        }
    }
}
