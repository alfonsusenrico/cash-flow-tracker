package com.alfonsusenrico.financialtracker.listener

import android.Manifest
import android.app.Application
import android.app.Notification
import android.app.NotificationManager
import android.content.Context
import android.content.Intent
import android.os.Bundle
import android.os.Looper
import android.util.Log
import androidx.core.app.RemoteInput
import androidx.work.Configuration
import androidx.work.testing.SynchronousExecutor
import androidx.work.testing.WorkManagerTestInitHelper
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.model.RawNotificationEntity
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.robolectric.Shadows.shadowOf

/** The app's own database, notifications and receivers, set up the way a notification answer meets them. */
class AnswerPathHarness(val context: Context, endpoint: String = "http://127.0.0.1:1") {
    val manager: NotificationManager = context.getSystemService(NotificationManager::class.java)
    val db: AppDatabase
    val pairing: String

    init {
        WorkManagerTestInitHelper.initializeTestWorkManager(
            context, Configuration.Builder().setMinimumLoggingLevel(Log.DEBUG).setExecutor(SynchronousExecutor()).build()
        )
        shadowOf(context as Application).grantPermissions(Manifest.permission.POST_NOTIFICATIONS)
        TestEndpoint.set(context, endpoint)
        AppPreferences(context).apiKey = "fictional-answer-path-key"
        pairing = AppPreferences(context).pairingContext
        // Receivers open the app's database, so the test uses it too and starts from empty tables.
        db = AppDatabase.getDatabase(context)
        runBlocking { withContext(Dispatchers.IO) { db.clearAllTables() } }
    }

    fun close() {
        AppPreferences(context).apiKey = ""
    }

    suspend fun insertRow(hash: String, postTime: Long = 1_000): Long = db.notificationDao().insertOrIgnore(
        RawNotificationEntity(
            deviceId = "fictional", packageName = "com.bca.mybca.omni.android", appLabel = "myBCA",
            postTime = postTime, capturedAt = System.currentTimeMillis(), title = "Fictional transaction", payloadHash = hash
        )
    )

    fun shown(tag: String): Notification? = manager.activeNotifications.singleOrNull { it.tag == tag }?.notification

    fun text(notification: Notification?): String? = notification?.extras?.getCharSequence(Notification.EXTRA_TEXT)?.toString()

    /** Runs the main looper (broadcasts, view-model work) until [done] holds or two seconds pass. */
    fun settle(done: () -> Boolean) {
        val until = System.currentTimeMillis() + 2_000
        while (!done() && System.currentTimeMillis() < until) {
            shadowOf(Looper.getMainLooper()).idle()
            Thread.sleep(20)
        }
        shadowOf(Looper.getMainLooper()).idle()
    }

    /** Sends a notification action as Android does, with [input] as its inline reply when given. */
    fun send(action: Notification.Action, inputKey: String, input: String?) {
        val fill = Intent()
        if (input != null) {
            RemoteInput.addResultsToIntent(
                arrayOf(RemoteInput.Builder(inputKey).build()), fill, Bundle().apply { putCharSequence(inputKey, input) }
            )
        }
        action.actionIntent.send(context, 0, fill)
    }
}
