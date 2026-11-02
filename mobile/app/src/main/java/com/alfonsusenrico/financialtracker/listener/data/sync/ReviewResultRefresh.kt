package com.alfonsusenrico.financialtracker.listener.data.sync

import android.content.Context
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.db.NotificationDao
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import java.util.concurrent.TimeUnit

/**
 * Re-reads `needs_review` and `failed` results when the owner opens the app. The backend can settle
 * such an event later (noise reclassified at startup, later processing), and the phone, which stores
 * these statuses as final, would otherwise keep showing it for review.
 */
object ReviewResultRefresh {
    /** Only rows captured this recently are re-read; older ones still refresh with "Sinkronkan sekarang". */
    val WINDOW_MILLIS = TimeUnit.DAYS.toMillis(30)
    const val LIMIT = 50

    val STATUSES = setOf("needs_review", "failed")

    /**
     * Checks the newest review rows of the current pairing that the owner has not labelled and that
     * have no unsent reply. Like the open-question check it runs whatever the auto-sync switch says.
     */
    suspend fun refresh(
        context: Context,
        dao: NotificationDao = AppDatabase.getDatabase(context).notificationDao(),
        now: Long = System.currentTimeMillis(),
        refresh: (eventId: String) -> Unit = { event -> SyncScheduler.enqueueCompletion(context, event, 0, refresh = true) }
    ): Int {
        val prefs = AppPreferences(context)
        if (!prefs.isConfigured) return 0
        val rows = dao.getRefreshableReviews(prefs.pairingContext, now - WINDOW_MILLIS, LIMIT)
        rows.forEach { refresh(it.backendEventId!!) }
        return rows.size
    }
}
