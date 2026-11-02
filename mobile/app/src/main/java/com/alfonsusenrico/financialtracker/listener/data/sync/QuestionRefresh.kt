package com.alfonsusenrico.financialtracker.listener.data.sync

import android.content.Context
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.db.NotificationDao
import com.alfonsusenrico.financialtracker.listener.data.model.RawNotificationEntity
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import java.util.concurrent.TimeUnit

/**
 * Result checks for rows whose question to the owner is still open: a recorded row's "Apa ini?"
 * question, or a `needs_confirmation` row's account, sender or source-pocket question. The backend also
 * closes these without the phone (web review, ledger edit, bulk review, an accepted history suggestion,
 * movement pairing, a server-side confirmation), and only a fresh result tells the phone. So an open
 * question keeps one check scheduled until a result closes it or the capture is [WINDOW_MILLIS] old.
 */
object QuestionRefresh {
    /** Delay before the next check, by the number of earlier checks that found the question open. */
    val DELAYS_MILLIS = listOf(
        TimeUnit.MINUTES.toMillis(2),
        TimeUnit.MINUTES.toMillis(10),
        TimeUnit.MINUTES.toMillis(30),
        TimeUnit.HOURS.toMillis(2),
        TimeUnit.HOURS.toMillis(6)
    )
    val STEADY_DELAY_MILLIS = TimeUnit.HOURS.toMillis(12)

    /** No check is scheduled later than this after capture; opening the app still checks. */
    val WINDOW_MILLIS = TimeUnit.DAYS.toMillis(7)

    fun delay(step: Int): Long = DELAYS_MILLIS.getOrElse(step) { STEADY_DELAY_MILLIS }

    fun nextCheckAt(capturedAt: Long, step: Int, now: Long): Long? =
        (now + delay(step)).takeIf { it <= capturedAt + WINDOW_MILLIS }

    fun open(row: RawNotificationEntity): Boolean =
        row.processingStatus == "recorded" && row.purposeQuestionId != null || row.processingStatus == "needs_confirmation"

    /**
     * Checks every open question of the current pairing now. It runs whatever the auto-sync switch
     * says: it sends no capture and only reads results of events the server already accepted.
     */
    suspend fun refreshOpen(
        context: Context,
        dao: NotificationDao = AppDatabase.getDatabase(context).notificationDao(),
        refresh: (eventId: String) -> Unit = { event -> SyncScheduler.enqueueCompletion(context, event, 0, refresh = true) }
    ): Int {
        val prefs = AppPreferences(context)
        if (!prefs.isConfigured) return 0
        val rows = dao.getOpenQuestions(prefs.pairingContext)
        rows.forEach { refresh(it.backendEventId!!) }
        return rows.size
    }
}
