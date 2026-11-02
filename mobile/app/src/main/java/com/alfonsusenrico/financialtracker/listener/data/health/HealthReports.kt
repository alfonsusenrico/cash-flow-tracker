package com.alfonsusenrico.financialtracker.listener.data.health

import android.content.Context
import androidx.work.BackoffPolicy
import androidx.work.Constraints
import androidx.work.Data
import androidx.work.CoroutineWorker
import androidx.work.ExistingPeriodicWorkPolicy
import androidx.work.ExistingWorkPolicy
import androidx.work.NetworkType
import androidx.work.OneTimeWorkRequestBuilder
import androidx.work.PeriodicWorkRequestBuilder
import androidx.work.WorkManager
import androidx.work.WorkerParameters
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.data.report.ReportReadiness
import java.util.concurrent.TimeUnit

/**
 * When the phone reports its health: after setup and the self-test, when the app opens, when the listener
 * connects or disconnects ([request]), and every 15 minutes while it runs ([schedulePeriodic]). The tracker
 * limits a key to about 30 reports a minute; these triggers stay far below that.
 */
object HealthReports {
    const val NOW_WORK = "device_health_report"
    const val PERIODIC_WORK = "device_health_periodic"

    /** A report from a trigger replaces one still waiting, so a burst of triggers sends once. */
    fun request(context: Context) {
        if (!AppPreferences(context).isConfigured) return
        val request = OneTimeWorkRequestBuilder<HealthReportWorker>()
            .setConstraints(network)
            .setBackoffCriteria(BackoffPolicy.EXPONENTIAL, 30, TimeUnit.SECONDS)
            .build()
        WorkManager.getInstance(context).enqueueUniqueWork(NOW_WORK, ExistingWorkPolicy.REPLACE, request)
    }

    fun schedulePeriodic(context: Context) {
        val request = PeriodicWorkRequestBuilder<HealthReportWorker>(15, TimeUnit.MINUTES)
            .setInputData(Data.Builder().putBoolean(HealthReportWorker.PERIODIC, true).build())
            .setConstraints(network)
            .build()
        WorkManager.getInstance(context).enqueueUniquePeriodicWork(PERIODIC_WORK, ExistingPeriodicWorkPolicy.KEEP, request)
    }

    private val network = Constraints.Builder().setRequiredNetworkType(NetworkType.CONNECTED).build()

    /** After an older tracker's 404, periodic reports wait this long before asking again. */
    const val UNSUPPORTED_PAUSE_MILLIS = 6 * 60 * 60 * 1000L

    /** At most this many backoff retries per trigger; the periodic report picks up from there. */
    const val MAX_ATTEMPTS = 5

    /** Whether a run should send now: an older tracker is asked again only after the pause. */
    fun shouldSend(periodic: Boolean, unsupportedAt: Long, now: Long): Boolean =
        !periodic || unsupportedAt <= 0 || now - unsupportedAt >= UNSUPPORTED_PAUSE_MILLIS

    /** What a run returns: retry with backoff for a transient failure or an older server, within [MAX_ATTEMPTS]. */
    fun retries(outcome: HealthReporter.Outcome, attempt: Int, periodic: Boolean): Boolean =
        !periodic && attempt + 1 < MAX_ATTEMPTS &&
            outcome in setOf(HealthReporter.Outcome.RETRY, HealthReporter.Outcome.UNSUPPORTED)
}

class HealthReportWorker(context: Context, params: WorkerParameters) : CoroutineWorker(context, params) {
    override suspend fun doWork(): Result {
        val periodic = inputData.getBoolean(PERIODIC, false)
        val prefs = AppPreferences(applicationContext)
        if (!HealthReports.shouldSend(periodic, prefs.healthUnsupportedAt, System.currentTimeMillis())) return Result.success()
        val outcome = HealthReporter(applicationContext).send()
        // The monthly report notice rides on the same quarter-hour wake-up, at most every few hours.
        if (periodic) ReportReadiness.refreshIfDue(applicationContext)
        return if (HealthReports.retries(outcome, runAttemptCount, periodic)) Result.retry() else Result.success()
    }

    companion object {
        const val PERIODIC = "periodic"
    }
}
