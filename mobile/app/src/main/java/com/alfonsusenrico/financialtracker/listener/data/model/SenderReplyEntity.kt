package com.alfonsusenrico.financialtracker.listener.data.model

import androidx.room.ColumnInfo
import androidx.room.Entity
import androidx.room.ForeignKey
import androidx.room.Index
import androidx.room.PrimaryKey

@Entity(
    tableName = "sender_replies",
    foreignKeys = [ForeignKey(
        entity = RawNotificationEntity::class, parentColumns = ["id"],
        childColumns = ["row_id"], onDelete = ForeignKey.CASCADE
    )],
    indices = [Index("row_id"), Index(value = ["pairing_context", "event_id", "question_id"], unique = true)]
)
data class SenderReplyEntity(
    @PrimaryKey @ColumnInfo(name = "reply_id") val replyId: String,
    @ColumnInfo(name = "row_id") val rowId: Long,
    @ColumnInfo(name = "pairing_context") val pairingContext: String,
    @ColumnInfo(name = "event_id") val eventId: String,
    @ColumnInfo(name = "question_id") val questionId: String,
    val action: String,
    val name: String?,
    val state: String = "queued",
    val attempt: Int = 0,
    @ColumnInfo(name = "next_attempt_at") val nextAttemptAt: Long = 0,
    @ColumnInfo(name = "error_code") val errorCode: String? = null,
    @ColumnInfo(name = "answer_kind", defaultValue = "'sender'") val answerKind: String = "sender",
    @ColumnInfo(name = "account_id") val accountId: String? = null,
    @ColumnInfo(name = "suggestion_id") val suggestionId: String? = null,
    /** A purpose answer typed by the owner; cleared once the server accepts it. */
    @ColumnInfo(name = "answer_text") val answerText: String? = null
)
