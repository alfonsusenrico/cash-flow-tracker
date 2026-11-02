package com.alfonsusenrico.financialtracker.listener.ui.model

import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences

/** Listener and sync health as the phone recorded it; times are epoch millis, 0 when it never happened. */
data class SyncHealth(
    val listenerConnected: Boolean,
    val lastSyncAt: Long,
    val failedCaptures: Int,
    val dropped: Int,
    val connectedAt: Long = 0,
    val disconnectedAt: Long = 0,
    val disconnectCount: Int = 0,
    val lastCaptureAt: Long = 0,
    val lastCaptureFailureAt: Long = 0,
    val lastDirectSyncAt: Long = 0,
    val lastDirectSyncOutcome: String? = null
) {
    companion object {
        fun from(prefs: AppPreferences, listenerConnected: Boolean) = SyncHealth(
            listenerConnected = listenerConnected,
            lastSyncAt = prefs.lastSyncTime,
            failedCaptures = prefs.captureFailureCount,
            dropped = prefs.droppedCount,
            connectedAt = prefs.listenerConnectedAt,
            disconnectedAt = prefs.listenerDisconnectedAt,
            disconnectCount = prefs.listenerDisconnectCount,
            lastCaptureAt = prefs.lastCaptureAt,
            lastCaptureFailureAt = prefs.lastCaptureFailureAt,
            lastDirectSyncAt = prefs.lastDirectSyncAt,
            lastDirectSyncOutcome = prefs.lastDirectSyncOutcome
        )
    }
}
