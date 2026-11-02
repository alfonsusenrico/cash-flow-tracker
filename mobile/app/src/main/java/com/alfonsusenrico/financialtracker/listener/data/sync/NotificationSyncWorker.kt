package com.alfonsusenrico.financialtracker.listener.data.sync

import android.content.Context
import androidx.work.CoroutineWorker
import androidx.work.WorkerParameters

/** Scheduled fallback for submission; the same work also runs directly after a capture (DirectSync). */
class NotificationSyncWorker(appContext: Context, workerParams: WorkerParameters) :
    CoroutineWorker(appContext, workerParams) {

    companion object {
        const val TAG = "NotificationSyncWorker"
        const val WORK_NAME = "periodic_notification_sync"
    }

    override suspend fun doWork(): Result = when (NotificationSubmission(applicationContext).submitPending()) {
        NotificationSubmission.Outcome.RETRY -> Result.retry()
        else -> Result.success()
    }
}
