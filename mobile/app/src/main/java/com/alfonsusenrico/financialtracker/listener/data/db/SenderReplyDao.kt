package com.alfonsusenrico.financialtracker.listener.data.db

import androidx.room.Dao
import androidx.room.Insert
import androidx.room.OnConflictStrategy
import androidx.room.Query
import com.alfonsusenrico.financialtracker.listener.data.model.SenderReplyEntity
import kotlinx.coroutines.flow.Flow

@Dao
interface SenderReplyDao {
    @Insert(onConflict = OnConflictStrategy.IGNORE)
    suspend fun insert(reply: SenderReplyEntity): Long

    @Query("SELECT * FROM sender_replies WHERE reply_id = :replyId")
    suspend fun get(replyId: String): SenderReplyEntity?

    @Query("SELECT * FROM sender_replies WHERE pairing_context = :pairing AND event_id = :event AND question_id = :question")
    suspend fun forQuestion(pairing: String, event: String, question: String): SenderReplyEntity?

    @Query("SELECT * FROM sender_replies WHERE state != 'accepted'")
    fun pendingFlow(): Flow<List<SenderReplyEntity>>

    @Query("SELECT * FROM sender_replies WHERE pairing_context = :pairing AND state IN ('queued', 'waiting_auth', 'waiting_pairing')")
    suspend fun recoverable(pairing: String): List<SenderReplyEntity>

    @Query("UPDATE sender_replies SET state = :state, attempt = :attempt, next_attempt_at = :nextAt, error_code = :error WHERE reply_id = :id AND state != 'accepted'")
    suspend fun defer(id: String, state: String, attempt: Int, nextAt: Long, error: String?)

    /**
     * Settles unsent purpose answers of a row whose question the server no longer asks, so they are
     * never posted. [openQuestion] is the row's current question, if any; [answering] is the reply
     * whose own acceptance produced the result.
     */
    @Query("""
        UPDATE sender_replies SET state = 'closed', error_code = 'question_closed'
        WHERE row_id = :rowId AND pairing_context = :pairing AND event_id = :event AND answer_kind = 'purpose'
          AND state IN ('queued', 'waiting_auth', 'waiting_pairing')
          AND (:openQuestion IS NULL OR question_id != :openQuestion)
          AND (:answering IS NULL OR reply_id != :answering)
    """)
    suspend fun closeUnsentPurpose(rowId: Long, pairing: String, event: String, openQuestion: String?, answering: String?): Int

    @Query("UPDATE sender_replies SET state = 'accepted', name = NULL, error_code = NULL WHERE reply_id = :id")
    suspend fun accept(id: String)

    @Query("UPDATE sender_replies SET state = 'accepted', answer_text = NULL, error_code = NULL WHERE reply_id = :id")
    suspend fun acceptPurpose(id: String)

    @Query("DELETE FROM sender_replies WHERE reply_id = :id AND state = 'failed'")
    suspend fun removeFailed(id: String)

    @Query("DELETE FROM sender_replies WHERE reply_id = :id AND state = 'invalid'")
    suspend fun removeInvalid(id: String)

    @Query("DELETE FROM sender_replies WHERE reply_id = :id AND state IN ('invalid', 'stale')")
    suspend fun removeRejected(id: String)
}
