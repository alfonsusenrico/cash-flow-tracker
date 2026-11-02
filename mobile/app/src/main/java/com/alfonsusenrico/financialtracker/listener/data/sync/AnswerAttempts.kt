package com.alfonsusenrico.financialtracker.listener.data.sync

import android.util.Log
import androidx.room.withTransaction
import com.alfonsusenrico.financialtracker.listener.BuildConfig
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.model.AnswerAttemptEntity
import kotlinx.coroutines.CancellationException

/**
 * Records each step of an owner's answer as a reason code, so a lost answer shows where it stopped:
 * no row means the attempt never reached the app, `received` alone means it stopped mid-way, and any
 * other code names the outcome.
 */
object AnswerAttempts {
    const val NOTIFICATION = "notification"
    const val APP = "app"

    const val SENDER = "sender"
    const val PURPOSE = "purpose"
    const val ACCOUNT = "account"

    const val RECEIVED = "received"
    const val QUEUED = "queued"
    /** An account confirmation the server accepted at once; it has no outbox. */
    const val CONFIRMED = "confirmed"
    const val MALFORMED = "malformed"
    const val LOCKED = "locked"
    const val MISSING_INPUT = "missing_input"
    const val INVALID_NAME = "invalid_name"
    const val INVALID_ANSWER = "invalid_answer"
    const val NOT_PAIRED = "not_paired"
    const val QUESTION_CHANGED = "question_changed"
    const val ALREADY_ANSWERED = "already_answered"
    const val SERVER_REFUSED = "server_refused"
    const val INVALID_RESULT = "invalid_result"
    const val NETWORK_UNAVAILABLE = "network_unavailable"
    const val TIMEOUT = "timeout"
    const val ERROR = "error"

    private val SETTLED_WITHOUT_NOTICE = setOf(RECEIVED, QUEUED, CONFIRMED)

    /** Enough for weeks of answers; the record is for diagnosis, not history. */
    private const val KEEP = 200
    private const val TAG = "AnswerAttempts"

    suspend fun record(
        db: AppDatabase, channel: String, kind: String, rowId: Long?, eventId: String?, questionId: String?, outcome: String,
        now: Long = System.currentTimeMillis()
    ) {
        try {
            db.withTransaction {
                db.answerAttemptDao().insert(
                    AnswerAttemptEntity(
                        createdAt = now, channel = channel, answerKind = kind, rowId = rowId, eventId = eventId,
                        questionId = questionId, outcome = outcome, appVersion = BuildConfig.VERSION_NAME
                    )
                )
                db.answerAttemptDao().keepNewest(KEEP)
            }
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (failure: Exception) {
            // The record only explains answers; failing to write it must not block the answer itself.
            Log.w(TAG, "Answer attempt not recorded: outcome=$outcome error=${failure.javaClass.simpleName}")
        }
    }

    /**
     * What Tanya says about a question whose newest settled attempt, among [attempts] (newest first), was a
     * notification answer that was not stored; null when there is nothing to say.
     */
    fun noticeFor(attempts: List<AnswerAttemptEntity>, eventId: String?, questionId: String?): String? {
        val newest = attempts.firstOrNull { it.eventId == eventId && it.questionId == questionId } ?: return null
        if (newest.channel != NOTIFICATION || newest.outcome in SETTLED_WITHOUT_NOTICE) return null
        val subject = when (newest.answerKind) {
            SENDER -> "Balasan"
            ACCOUNT -> "Konfirmasi"
            else -> "Jawaban"
        }
        val reason = if (newest.outcome == LOCKED) " karena ponsel terkunci" else ""
        return "$subject dari notifikasi belum tersimpan$reason. Jawab di sini."
    }
}
