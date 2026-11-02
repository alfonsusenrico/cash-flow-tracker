package com.alfonsusenrico.financialtracker.listener.data.sync

import android.util.Log
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.withTimeoutOrNull

/**
 * The steps every answer given in a notification action takes, whatever its question: it is traced as
 * received, a locked phone or a missing inline reply is refused, the answer is stored within the broadcast's
 * time budget, and an answer that is not stored leaves its question answerable through [offerAgain].
 */
class NotificationAnswer(
    private val db: AppDatabase,
    private val kind: String,
    private val rowId: Long?,
    private val eventId: String,
    private val questionId: String?,
    private val messages: Messages,
    // Broadcast receivers must finish well within the system's ~10 s budget for goAsync().
    private val timeoutMillis: Long = 8_000
) {
    /** What the notification says when the answer is refused before its own store gives a reason. */
    class Messages(val notSaved: String, val locked: String = notSaved, val missingInput: String = notSaved)

    suspend fun note(outcome: String) =
        AnswerAttempts.record(db, AnswerAttempts.NOTIFICATION, kind, rowId, eventId, questionId, outcome)

    /** [store] returns the stored outcome with its [AnswerAttempts] code, which is recorded here. */
    suspend fun run(
        locked: Boolean,
        missingInput: Boolean,
        store: suspend () -> SenderReplies.Outcome,
        offerAgain: suspend (problem: String) -> Unit
    ) {
        note(AnswerAttempts.RECEIVED)
        if (locked) return refuse(AnswerAttempts.LOCKED, messages.locked, offerAgain)
        if (missingInput) return refuse(AnswerAttempts.MISSING_INPUT, messages.missingInput, offerAgain)
        val outcome = try {
            withTimeoutOrNull(timeoutMillis) { store() } ?: return refuse(AnswerAttempts.TIMEOUT, messages.notSaved, offerAgain)
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Exception) {
            return refuse(AnswerAttempts.ERROR, messages.notSaved, offerAgain)
        }
        outcome.code?.let { note(it) }
        if (!outcome.queued) offer(outcome.message ?: messages.notSaved, offerAgain)
    }

    private suspend fun refuse(outcome: String, message: String, offerAgain: suspend (String) -> Unit) {
        note(outcome)
        offer(message, offerAgain)
    }

    private suspend fun offer(message: String, offerAgain: suspend (String) -> Unit) {
        try {
            offerAgain(message)
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (failure: Exception) {
            // Reason code only; the attempt's own outcome is already recorded.
            Log.w(TAG, "Question not offered again: kind=$kind error=${failure.javaClass.simpleName}")
        }
    }

    private companion object {
        const val TAG = "NotificationAnswer"
    }
}
