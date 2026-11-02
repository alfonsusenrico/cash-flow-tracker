package com.alfonsusenrico.financialtracker.listener.data.textcapture

import android.content.Context
import androidx.work.CoroutineWorker
import androidx.work.WorkerParameters

/** Resends queued captures once the network is back; a refusal or a missing key is not retried. */
class TextCaptureWorker(appContext: Context, params: WorkerParameters) : CoroutineWorker(appContext, params) {
    override suspend fun doWork(): Result =
        if (TextCaptures(applicationContext).sendQueued()) Result.retry() else Result.success()
}
