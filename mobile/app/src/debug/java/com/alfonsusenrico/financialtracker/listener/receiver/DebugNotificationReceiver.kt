package com.alfonsusenrico.financialtracker.listener.receiver

import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import com.alfonsusenrico.financialtracker.listener.service.CapturedNotification
import com.alfonsusenrico.financialtracker.listener.service.NotificationProcessor

class DebugNotificationReceiver : BroadcastReceiver() {
    companion object {
        const val ACTION_SIMULATE = "com.alfonsusenrico.financialtracker.SIMULATE_NOTIFICATION"
    }

    override fun onReceive(context: Context, intent: Intent) {
        val appContext = context.applicationContext
        if (intent.action == ACTION_SIMULATE) {
            val packageName = intent.getStringExtra("packageName") ?: "com.jago.digitalBanking"
            val title = intent.getStringExtra("title") ?: "Jago"
            val text = intent.getStringExtra("text") ?: ""
            val bigText = intent.getStringExtra("bigText") ?: text

            NotificationProcessor(appContext).enqueue(
                CapturedNotification(
                    packageName = packageName,
                    notificationKey = "debug_${System.currentTimeMillis()}",
                    notificationId = (10000..99999).random(),
                    notificationWhen = System.currentTimeMillis(),
                    channelId = "debug_channel",
                    category = "msg",
                    title = title,
                    bodyText = text,
                    bigText = bigText,
                    subText = null,
                    summaryText = null,
                    postTime = System.currentTimeMillis(),
                    rawExtrasJson = null
                )
            )
        }
    }
}
