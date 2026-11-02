package com.alfonsusenrico.financialtracker.listener.data.model

import androidx.room.ColumnInfo
import androidx.room.Entity
import androidx.room.Index
import androidx.room.PrimaryKey

/**
 * One step of an attempt to store an answer, kept so a lost answer can be diagnosed from the phone's
 * database. Reason codes and server identifiers only: never a name, typed text or an amount.
 */
@Entity(tableName = "answer_attempts", indices = [Index(value = ["event_id", "question_id"])])
data class AnswerAttemptEntity(
    @PrimaryKey(autoGenerate = true) val id: Long = 0,
    @ColumnInfo(name = "created_at") val createdAt: Long,
    /** Where the answer came from: `notification` or `app`. */
    val channel: String,
    @ColumnInfo(name = "answer_kind") val answerKind: String,
    @ColumnInfo(name = "row_id") val rowId: Long?,
    @ColumnInfo(name = "event_id") val eventId: String?,
    @ColumnInfo(name = "question_id") val questionId: String?,
    val outcome: String,
    @ColumnInfo(name = "app_version") val appVersion: String
)
