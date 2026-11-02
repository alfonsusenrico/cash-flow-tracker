package com.alfonsusenrico.financialtracker.listener.data.sync

import android.content.Context
import android.util.Log
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.network.ApiClient
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Submits and checks results right after a capture, inside the listener's process.
 *
 * HiOS defers WorkManager jobs for this app (TranUsfAppInternal p_job), so relying on them alone
 * delays records and alerts until the app is opened. WorkManager remains the retry fallback.
 */
object DirectSync {
    private const val TAG = "DirectSync"

    // Result checks after a submission, in seconds; about 2 minutes 15 seconds in total.
    val POLL_DELAYS_SECONDS = listOf(5L, 10L, 20L, 40L, 60L)

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val running = AtomicBoolean(false)
    private val requestedAgain = AtomicBoolean(false)

    fun trigger(context: Context) {
        val appContext = context.applicationContext
        requestedAgain.set(true)
        if (!running.compareAndSet(false, true)) return
        scope.launch {
            try {
                while (requestedAgain.getAndSet(false)) {
                    runOnce(appContext)
                }
            } finally {
                running.set(false)
            }
        }
    }

    private suspend fun runOnce(context: Context) {
        val prefs = AppPreferences(context)
        if (!prefs.isConfigured) return
        val outcome = try {
            NotificationSubmission(context).submitPending()
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Exception) {
            NotificationSubmission.Outcome.RETRY
        }
        prefs.recordDirectSync(outcome.name.lowercase())
        if (outcome == NotificationSubmission.Outcome.RETRY) {
            SyncScheduler.enqueueImmediateSync(context)
            return
        }
        pollResults(context, prefs.pairingContext)
    }

    private suspend fun pollResults(context: Context, pairingContext: String) {
        val dao = AppDatabase.getDatabase(context).notificationDao()
        val results = NotificationResults(context)
        for (seconds in POLL_DELAYS_SECONDS) {
            val waiting = dao.getAwaitingResult(pairingContext)
            if (waiting.isEmpty()) break
            delay(seconds * 1000)
            val service = ApiClient(context).createService()
            for (row in waiting) {
                val eventId = row.backendEventId ?: continue
                val current = dao.getById(row.id) ?: continue
                if (current.processingStatus !in setOf("queued", "processing")) continue
                try {
                    val response = service.getNotificationResult(eventId)
                    val item = response.body()?.result
                    if (response.isSuccessful && response.body()?.ok == true && item != null && item.eventId == eventId) {
                        results.consume(current, item, pairingContext)
                    }
                } catch (cancelled: CancellationException) {
                    throw cancelled
                } catch (_: Exception) {
                    Log.w(TAG, "Result check deferred: network_unavailable")
                    return
                }
            }
            results.deliverPendingAlerts()
        }
    }
}
