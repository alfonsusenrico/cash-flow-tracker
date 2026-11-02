package com.alfonsusenrico.financialtracker.listener.data.db

import androidx.room.Dao
import androidx.room.Insert
import androidx.room.Query
import com.alfonsusenrico.financialtracker.listener.data.model.AnswerAttemptEntity
import kotlinx.coroutines.flow.Flow

@Dao
interface AnswerAttemptDao {
    @Insert
    suspend fun insert(attempt: AnswerAttemptEntity): Long

    @Query("DELETE FROM answer_attempts WHERE id NOT IN (SELECT id FROM answer_attempts ORDER BY id DESC LIMIT :keep)")
    suspend fun keepNewest(keep: Int)

    /** [question] is null for an account proposal that predates question ids. */
    @Query("SELECT * FROM answer_attempts WHERE event_id = :event AND question_id IS :question ORDER BY id")
    suspend fun forQuestion(event: String, question: String?): List<AnswerAttemptEntity>

    /** Settled steps, newest first; `received` only marks that an attempt started. */
    @Query("SELECT * FROM answer_attempts WHERE outcome != 'received' ORDER BY id DESC LIMIT 100")
    fun recentOutcomesFlow(): Flow<List<AnswerAttemptEntity>>
}
