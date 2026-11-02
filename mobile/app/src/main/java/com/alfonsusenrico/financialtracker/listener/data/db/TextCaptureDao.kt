package com.alfonsusenrico.financialtracker.listener.data.db

import androidx.room.Dao
import androidx.room.Insert
import androidx.room.OnConflictStrategy
import androidx.room.Query
import androidx.room.Update
import com.alfonsusenrico.financialtracker.listener.data.model.TextCaptureEntity
import kotlinx.coroutines.flow.Flow

@Dao
interface TextCaptureDao {
    @Insert(onConflict = OnConflictStrategy.ABORT)
    suspend fun insert(capture: TextCaptureEntity): Long

    @Update
    suspend fun update(capture: TextCaptureEntity): Int

    @Query("SELECT * FROM text_captures WHERE id = :id")
    suspend fun getById(id: Long): TextCaptureEntity?

    @Query("SELECT * FROM text_captures WHERE id = :id")
    fun observeById(id: Long): Flow<TextCaptureEntity?>

    /** Captures that still describe something: undone and deleted ones left nothing in the tracker. */
    @Query("SELECT * FROM text_captures WHERE state NOT IN ('undone', 'deleted') ORDER BY created_at DESC, id DESC")
    fun observeVisible(): Flow<List<TextCaptureEntity>>

    /**
     * Takes a capture for one send. Only a capture the tracker has not accepted is sendable; a claim older
     * than [staleBefore] belongs to a send that died with its process.
     */
    @Query("""
        UPDATE text_captures SET state = 'sending', attempts = attempts + 1, claimed_at = :now, error_code = NULL
        WHERE id = :id AND server_id IS NULL
          AND (state = 'queued' OR (state = 'sending' AND claimed_at < :staleBefore))
    """)
    suspend fun claim(id: Long, now: Long, staleBefore: Long): Int

    /** Returns a capture whose send did not reach a verdict to the queue, with the reason. */
    @Query("UPDATE text_captures SET state = 'queued', claimed_at = NULL, error_code = :error WHERE id = :id AND state = 'sending'")
    suspend fun release(id: Long, error: String): Int

    /** Why a queued capture is waiting, such as no connected tracker. */
    @Query("UPDATE text_captures SET error_code = :error WHERE id = :id AND state = 'queued'")
    suspend fun noteQueued(id: Long, error: String): Int

    @Query("UPDATE text_captures SET state = 'failed', claimed_at = NULL, error_code = :error WHERE id = :id AND state = 'sending'")
    suspend fun fail(id: Long, error: String): Int

    @Query("""
        SELECT * FROM text_captures WHERE server_id IS NULL
          AND (state = 'queued' OR (state = 'sending' AND claimed_at < :staleBefore))
        ORDER BY created_at, id
    """)
    suspend fun sendable(staleBefore: Long): List<TextCaptureEntity>

    /** At process start no send can be running, so every claim is from a process that died. */
    @Query("UPDATE text_captures SET state = 'queued', claimed_at = NULL WHERE state = 'sending'")
    suspend fun releaseInterrupted(): Int

    /** A refused capture goes back to the queue only when the owner asks. */
    @Query("UPDATE text_captures SET state = 'queued', error_code = NULL WHERE id = :id AND state = 'failed'")
    suspend fun requeue(id: Long): Int

    /** Removes a capture that never reached the tracker: never sent, or refused. */
    @Query("DELETE FROM text_captures WHERE id = :id AND server_id IS NULL AND ((state = 'queued' AND attempts = 0) OR state = 'failed')")
    suspend fun discard(id: Long): Int

    /** Typed captures still on this phone, waiting to reach the tracker (the health report's queue). */
    @Query("SELECT COUNT(*) FROM text_captures WHERE server_id IS NULL AND state IN ('queued', 'sending')")
    suspend fun countWaiting(): Int
}
