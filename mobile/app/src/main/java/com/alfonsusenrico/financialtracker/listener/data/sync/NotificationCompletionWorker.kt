package com.alfonsusenrico.financialtracker.listener.data.sync

import android.content.Context
import android.util.Log
import androidx.work.CoroutineWorker
import androidx.work.WorkerParameters
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.network.ApiClient
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

class NotificationCompletionWorker(appContext: Context, workerParams: WorkerParameters) :
    CoroutineWorker(appContext, workerParams) {

    companion object {
        const val EVENT_ID = "event_id"
        const val REFRESH = "refresh"
        const val QUESTION_FOLLOW_UP = "question_follow_up"
        private val lookupMutex = Mutex()
        private const val TAG = "NotificationCompletion"
    }

    private val dao = AppDatabase.getDatabase(appContext).notificationDao()
    private val prefs = AppPreferences(appContext)
    // The worker schedules question follow-ups itself once the check is stored (see the end of completeEvent).
    private val results = NotificationResults(appContext, scheduleFollowUp = { _, _ -> })

    override suspend fun doWork(): Result = lookupMutex.withLock { completeEvent() }

    private suspend fun completeEvent(): Result {
        val eventId = inputData.getString(EVENT_ID)
        if (!validUuid(eventId) || !prefs.isConfigured) return Result.success()
        val pairingContext = prefs.pairingContext
        val row = dao.getByBackendEventId(eventId!!, pairingContext) ?: return Result.success()
        val refresh = inputData.getBoolean(REFRESH, false)
        val followUp = inputData.getBoolean(QUESTION_FOLLOW_UP, false)
        if (followUp) {
            if (!QuestionRefresh.open(row)) return Result.success()
        } else if (!refresh && row.processingStatus !in setOf("queued", "processing")) {
            results.deliverPendingAlerts()
            return Result.success()
        }
        val due = if (refresh) 0L else row.nextCheckAt ?: return Result.success()
        if (due > System.currentTimeMillis()) {
            if (followUp) {
                SyncScheduler.enqueueQuestionFollowUp(applicationContext, eventId, due - System.currentTimeMillis(), true)
            } else {
                SyncScheduler.enqueueCompletion(applicationContext, eventId, due - System.currentTimeMillis(), true)
            }
            return Result.success()
        }
        // A row with an open question keeps its status when a check fails.
        val defer: suspend (String, Boolean) -> Unit = when {
            QuestionRefresh.open(row) -> { code, terminal -> results.deferFollowUp(row, code, terminal) }
            // A review or failed row is already settled on the phone; a failed re-read leaves it as it is.
            row.processingStatus in ReviewResultRefresh.STATUSES -> { code, _ -> Log.w(TAG, "Review refresh skipped: reason=$code") }
            else -> { code, terminal -> results.defer(row, code, terminal) }
        }
        try {
            val response = ApiClient(applicationContext).createService().getNotificationResult(eventId)
            when {
                response.isSuccessful && response.body()?.ok == true && response.body()?.result != null -> {
                    val item = response.body()!!.result!!
                    if (item.eventId != eventId || !results.consume(row, item, pairingContext)) {
                        defer("invalid_result", true)
                    }
                }
                response.code() == 401 || response.code() == 403 -> defer("auth_rejected", true)
                response.code() == 404 -> defer("event_not_found", true)
                response.code() == 429 || response.code() >= 500 -> defer("server_unavailable", false)
                else -> defer("invalid_result", true)
            }
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Exception) {
            defer("network_unavailable", false)
            Log.w(TAG, "Completion deferred: network_unavailable")
        }
        results.deliverPendingAlerts()
        val updated = dao.getById(row.id) ?: return Result.success()
        val next = updated.nextCheckAt ?: return Result.success()
        if (updated.processingStatus in setOf("queued", "processing")) {
            SyncScheduler.enqueueCompletion(applicationContext, eventId, next - System.currentTimeMillis(), true)
        } else if (QuestionRefresh.open(updated)) {
            SyncScheduler.enqueueQuestionFollowUp(applicationContext, eventId, next - System.currentTimeMillis(), followUp)
        }
        return Result.success()
    }
}
