package com.alfonsusenrico.financialtracker.listener.data.sync

import android.content.Context
import androidx.work.Constraints
import androidx.work.ExistingPeriodicWorkPolicy
import androidx.work.ExistingWorkPolicy
import androidx.work.NetworkType
import androidx.work.Data
import androidx.work.OneTimeWorkRequestBuilder
import androidx.work.PeriodicWorkRequestBuilder
import androidx.work.WorkManager
import com.alfonsusenrico.financialtracker.listener.data.textcapture.TextCaptureWorker
import java.util.concurrent.TimeUnit

object SyncScheduler {

    fun enqueueSenderReply(context: Context, replyId: String, delayMillis: Long, continuation: Boolean = false) {
        val request = OneTimeWorkRequestBuilder<SenderReplyWorker>()
            .setInputData(Data.Builder().putString(SenderReplyWorker.REPLY_ID, replyId).build())
            .setInitialDelay(delayMillis.coerceAtLeast(0), TimeUnit.MILLISECONDS)
            .setConstraints(networkConstraints)
            .build()
        WorkManager.getInstance(context).enqueueUniqueWork(
            "sender_reply_$replyId",
            if (continuation) ExistingWorkPolicy.APPEND_OR_REPLACE else ExistingWorkPolicy.KEEP,
            request
        )
    }

    private val networkConstraints = Constraints.Builder()
        .setRequiredNetworkType(NetworkType.CONNECTED)
        .build()

    const val IMMEDIATE_SYNC_WORK = "immediate_notification_sync"

    fun enqueueImmediateSync(context: Context) {
        val request = OneTimeWorkRequestBuilder<NotificationSyncWorker>()
            .setConstraints(networkConstraints)
            .build()

        WorkManager.getInstance(context).enqueueUniqueWork(
            IMMEDIATE_SYNC_WORK,
            ExistingWorkPolicy.KEEP,
            request
        )
    }

    fun enqueueSubmissionFollowUp(context: Context) {
        val request = OneTimeWorkRequestBuilder<NotificationSyncWorker>()
            .setConstraints(networkConstraints)
            .build()
        WorkManager.getInstance(context).enqueueUniqueWork(
            IMMEDIATE_SYNC_WORK, ExistingWorkPolicy.APPEND_OR_REPLACE, request
        )
    }

    fun enqueueCompletion(context: Context, eventId: String, delayMillis: Long, continuation: Boolean = false, refresh: Boolean = false) {
        val request = OneTimeWorkRequestBuilder<NotificationCompletionWorker>()
            .setInputData(Data.Builder().putString(NotificationCompletionWorker.EVENT_ID, eventId)
                .putBoolean(NotificationCompletionWorker.REFRESH, refresh).build())
            .setInitialDelay(delayMillis.coerceAtLeast(0), TimeUnit.MILLISECONDS)
            .setConstraints(networkConstraints)
            .build()
        WorkManager.getInstance(context).enqueueUniqueWork(
            "notification_completion_$eventId",
            if (continuation) ExistingWorkPolicy.APPEND_OR_REPLACE else ExistingWorkPolicy.KEEP,
            request
        )
    }

    fun questionFollowUpWork(eventId: String) = "question_follow_up_$eventId"

    /**
     * The next check of an open question ([QuestionRefresh]). Its own work name keeps a delayed follow-up from
     * swallowing an immediate refresh of the same event. A new schedule replaces the pending one; the
     * follow-up worker appends its successor ([continuation]) because replacing would cancel itself.
     */
    fun enqueueQuestionFollowUp(context: Context, eventId: String, delayMillis: Long, continuation: Boolean = false) {
        val request = OneTimeWorkRequestBuilder<NotificationCompletionWorker>()
            .setInputData(Data.Builder().putString(NotificationCompletionWorker.EVENT_ID, eventId)
                .putBoolean(NotificationCompletionWorker.QUESTION_FOLLOW_UP, true).build())
            .setInitialDelay(delayMillis.coerceAtLeast(0), TimeUnit.MILLISECONDS)
            .setConstraints(networkConstraints)
            .build()
        WorkManager.getInstance(context).enqueueUniqueWork(
            questionFollowUpWork(eventId),
            if (continuation) ExistingWorkPolicy.APPEND_OR_REPLACE else ExistingWorkPolicy.REPLACE,
            request
        )
    }

    fun schedulePeriodicSync(context: Context, intervalMinutes: Long = 15) {
        val request = PeriodicWorkRequestBuilder<NotificationSyncWorker>(
            intervalMinutes, TimeUnit.MINUTES
        )
            .setConstraints(networkConstraints)
            .build()

        WorkManager.getInstance(context).enqueueUniquePeriodicWork(
            NotificationSyncWorker.WORK_NAME,
            ExistingPeriodicWorkPolicy.KEEP,
            request
        )
    }

    fun cancelPeriodicSync(context: Context) {
        WorkManager.getInstance(context).cancelUniqueWork(NotificationSyncWorker.WORK_NAME)
    }

    const val TEXT_CAPTURE_WORK = "text_capture_send"

    /** Sends queued typed captures when the network is available; repeated requests share one work. */
    fun enqueueTextCaptures(context: Context) {
        val request = OneTimeWorkRequestBuilder<TextCaptureWorker>()
            .setConstraints(networkConstraints)
            .build()
        WorkManager.getInstance(context).enqueueUniqueWork(TEXT_CAPTURE_WORK, ExistingWorkPolicy.KEEP, request)
    }

    fun cancelTextCaptures(context: Context) {
        WorkManager.getInstance(context).cancelUniqueWork(TEXT_CAPTURE_WORK)
    }

    fun cancelImmediateSync(context: Context) {
        WorkManager.getInstance(context).cancelUniqueWork(IMMEDIATE_SYNC_WORK)
    }
}
