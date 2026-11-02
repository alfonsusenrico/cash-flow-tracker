package com.alfonsusenrico.financialtracker.listener.data.sync

import android.content.Context
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences

/**
 * The "Auto-Sync Notifikasi" setting: whether captures are sent without the owner asking.
 *
 * It covers the periodic sync, the sends that a new capture or a listener reconnect starts, and the
 * automatic resend of queued typed captures. Owner actions (sync now, send a kept row, "Kirim" on a
 * typed capture, answer a question) and result checks for events the server already accepted run
 * either way.
 */
object AutoSync {

    fun setEnabled(context: Context, enabled: Boolean) {
        AppPreferences(context).isAutoSyncEnabled = enabled
        if (enabled) {
            SyncScheduler.schedulePeriodicSync(context)
            SyncScheduler.enqueueImmediateSync(context)
            SyncScheduler.enqueueTextCaptures(context)
        } else {
            SyncScheduler.cancelPeriodicSync(context)
            // A queued or retrying automatic send would otherwise still run once the network returns.
            SyncScheduler.cancelImmediateSync(context)
            SyncScheduler.cancelTextCaptures(context)
        }
    }

    /** Brings scheduled work in line with the setting at app start; older builds never cancelled it. */
    fun restore(context: Context) {
        if (AppPreferences(context).isAutoSyncEnabled) {
            SyncScheduler.schedulePeriodicSync(context)
        } else {
            SyncScheduler.cancelPeriodicSync(context)
        }
    }

    /** Sends pending captures now when automatic sending is on; returns whether it started. */
    fun sendPending(context: Context, startDirect: (Context) -> Unit = DirectSync::trigger): Boolean {
        val prefs = AppPreferences(context)
        if (!prefs.isAutoSyncEnabled || !prefs.isConfigured) return false
        startDirect(context)
        SyncScheduler.enqueueImmediateSync(context)
        return true
    }
}
