package com.alfonsusenrico.financialtracker.listener.data.sync

import android.content.Context
import androidx.work.CoroutineWorker
import androidx.work.WorkerParameters
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.network.ApiClient
import com.alfonsusenrico.financialtracker.listener.data.network.model.SenderConfirmRequest
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.service.SenderConfirmationNotifier
import com.alfonsusenrico.financialtracker.listener.service.SourcePocketConfirmationNotifier
import com.alfonsusenrico.financialtracker.listener.data.network.model.SourcePocketConfirmRequest
import kotlinx.coroutines.CancellationException
import kotlin.math.min

internal fun senderRetryDelay(attempt: Int): Long = min(3_600_000L, 30_000L * (1L shl attempt.coerceIn(0, 7)))

// Keep this class identity so persisted WorkManager jobs survive the typed-outbox upgrade.
class SenderReplyWorker(context: Context, params: WorkerParameters) : CoroutineWorker(context, params) {
    companion object {
        const val REPLY_ID = "reply_id"
    }

    override suspend fun doWork(): Result {
        val db = AppDatabase.getDatabase(applicationContext)
        val dao = db.senderReplyDao()
        val reply = dao.get(inputData.getString(REPLY_ID) ?: return Result.success()) ?: return Result.success()
        if (reply.answerKind == PurposeAnswers.KIND) {
            PurposeAnswers(applicationContext, db).deliver(reply.replyId)
            return Result.success()
        }
        if (reply.state !in setOf("queued", "waiting_auth")) return Result.success()
        val prefs = AppPreferences(applicationContext)
        suspend fun stop(state: String, error: String) {
            dao.defer(reply.replyId, state, reply.attempt, 0, error)
            if (reply.answerKind == "source_pocket") {
                SourcePocketConfirmationNotifier.showState(applicationContext, reply.eventId, reply.rowId, sourcePocketReplyMessage(state))
            } else {
                SenderConfirmationNotifier.showState(applicationContext, reply.eventId, reply.rowId, senderReplyMessage(state))
            }
        }
        if (prefs.pairingContext != reply.pairingContext) {
            stop("waiting_pairing", "pairing_changed")
            return Result.success()
        }
        if (!prefs.isConfigured) {
            stop("waiting_auth", "auth_rejected")
            return Result.success()
        }
        val row = db.notificationDao().getById(reply.rowId) ?: return Result.success()
        if (row.backendEventId != reply.eventId || row.pairingContext != reply.pairingContext) {
            stop("stale", "result_mismatch")
            return Result.success()
        }
        // Even if a poll has moved this event on, replay the same persisted reply ID to recover a lost acknowledgement.
        suspend fun retry(error: String) {
            val delay = senderRetryDelay(reply.attempt)
            dao.defer(reply.replyId, "queued", reply.attempt + 1, System.currentTimeMillis() + delay, error)
            SyncScheduler.enqueueSenderReply(applicationContext, reply.replyId, delay, continuation = true)
        }
        try {
            val service = ApiClient(applicationContext).createService(reply.pairingContext)
            val response = when (reply.answerKind) {
                "sender" -> service.confirmSender(reply.eventId, SenderConfirmRequest(reply.questionId, reply.replyId, reply.action, reply.name))
                "source_pocket" -> {
                    if (!validUuid(reply.accountId) || reply.action != "select" || reply.name != null) {
                        stop("invalid", "invalid_reply")
                        return Result.success()
                    }
                    service.confirmSourcePocket(reply.eventId, SourcePocketConfirmRequest(reply.questionId, reply.replyId, reply.accountId!!))
                }
                else -> {
                    stop("invalid", "invalid_reply")
                    return Result.success()
                }
            }
            // Pairing may have changed while the request was in flight. Never consume into a different pairing.
            if (prefs.pairingContext != reply.pairingContext) {
                stop("waiting_pairing", "pairing_changed")
                return Result.success()
            }
            when {
                response.code() in setOf(401, 403) -> stop("waiting_auth", "auth_rejected")
                response.code() in setOf(404, 409) -> {
                    if (reply.answerKind == "source_pocket") {
                        db.notificationDao().saveSourcePocketOptions(row.id, reply.pairingContext, reply.eventId, null)
                    }
                    stop("stale", "stale_confirmation")
                    SyncScheduler.enqueueCompletion(applicationContext, reply.eventId, 0, refresh = true)
                }
                response.code() in setOf(400, 422) -> stop("invalid", if (reply.answerKind == "sender") "invalid_sender_name" else "invalid_source_pocket")
                response.code() == 429 || response.code() >= 500 -> retry("server_unavailable")
                !response.isSuccessful -> stop("invalid", "invalid_reply")
                else -> {
                    val body = response.body()
                    val item = body?.result
                    val results = NotificationResults(applicationContext, db)
                    if (body?.ok != true || item?.eventId != reply.eventId || item.payloadHash != row.payloadHash) {
                        retry("invalid_result")
                    } else {
                        // Persist the result first. A crash before clearing the outbox only causes an idempotent replay.
                        val accepted = results.consume(row, item, reply.pairingContext)
                        if (!accepted) {
                            retry("invalid_result")
                        } else {
                            dao.accept(reply.replyId)
                            val saved = db.notificationDao().getById(row.id)
                            if (saved?.processingStatus in setOf("queued", "processing")) {
                                SyncScheduler.enqueueCompletion(applicationContext, reply.eventId, 0, continuation = true)
                            }
                            results.deliverPendingAlerts(reply.pairingContext)
                        }
                    }
                }
            }
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Exception) {
            if (prefs.pairingContext != reply.pairingContext) {
                stop("waiting_pairing", "pairing_changed")
            } else {
                retry("network_unavailable")
            }
        }
        return Result.success()
    }
}
