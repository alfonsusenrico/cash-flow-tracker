package com.alfonsusenrico.financialtracker.listener.data.model

import androidx.room.ColumnInfo
import androidx.room.Entity
import androidx.room.Index
import androidx.room.PrimaryKey

/**
 * A transaction the owner typed in Catat. The row is written before the first send, so [clientCaptureId]
 * makes every resend idempotent on the server. [state] is one of [TextCaptureState]; server fields are
 * null until the tracker accepts the capture.
 */
@Entity(
    tableName = "text_captures",
    indices = [Index(value = ["client_capture_id"], unique = true), Index("state")]
)
data class TextCaptureEntity(
    @PrimaryKey(autoGenerate = true) val id: Long = 0,
    @ColumnInfo(name = "client_capture_id") val clientCaptureId: String,
    val text: String,
    /** The device time sent with the capture, ISO 8601 with offset; the server dates "kemarin" from it. */
    @ColumnInfo(name = "device_time") val deviceTime: String,
    @ColumnInfo(name = "created_at") val createdAt: Long,
    val state: String = TextCaptureState.QUEUED,
    @ColumnInfo(name = "server_id") val serverId: String? = null,
    /** The pairing whose key the server accepted the capture with; answers and undo need the same one. */
    @ColumnInfo(name = "pairing_context") val pairingContext: String? = null,
    @ColumnInfo(name = "question_json") val questionJson: String? = null,
    /** The server's capture view (summary, transaction and movement) as last received. */
    @ColumnInfo(name = "summary_json") val summaryJson: String? = null,
    @ColumnInfo(name = "transaction_id") val transactionId: String? = null,
    /** The server's undo deadline as sent; [undoDeadline] is the same moment on this phone's clock. */
    @ColumnInfo(name = "undo_until") val undoUntil: String? = null,
    @ColumnInfo(name = "undo_deadline") val undoDeadline: Long? = null,
    /** An answer stored before it is sent, with its reply id, so a resend is the same answer. */
    @ColumnInfo(name = "pending_answer_json") val pendingAnswerJson: String? = null,
    val attempts: Int = 0,
    @ColumnInfo(name = "claimed_at") val claimedAt: Long? = null,
    @ColumnInfo(name = "error_code") val errorCode: String? = null,
    /** The owner changed the recorded transaction, which ends the undo window on the server. */
    val edited: Boolean = false
)

object TextCaptureState {
    /** Stored, not yet accepted by the tracker. */
    const val QUEUED = "queued"
    /** A send is in flight; a claim older than the send timeout is treated as queued again. */
    const val SENDING = "sending"
    const val RECORDED = "recorded"
    const val NEEDS_DETAIL = "needs_detail"
    const val UNDONE = "undone"
    /** The tracker refused the capture itself; it is never resent on its own. */
    const val FAILED = "failed"
    /** Its transaction was deleted from the tracker with "Hapus dari tracker". */
    const val DELETED = "deleted"

    /** States the tracker reports for a capture view. */
    fun fromServer(status: String?): String? = when (status) {
        "recorded" -> RECORDED
        "needs_detail" -> NEEDS_DETAIL
        "undone" -> UNDONE
        else -> null
    }
}
