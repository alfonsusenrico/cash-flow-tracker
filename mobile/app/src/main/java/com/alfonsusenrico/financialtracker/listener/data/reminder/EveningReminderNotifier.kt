package com.alfonsusenrico.financialtracker.listener.data.reminder

import android.Manifest
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.content.Context
import android.content.Intent
import android.content.pm.PackageManager
import android.os.Build
import androidx.core.app.NotificationCompat
import com.alfonsusenrico.financialtracker.listener.MainActivity
import com.alfonsusenrico.financialtracker.listener.R

/** The evening summary of light questions; tapping it opens the question sheet. */
object EveningReminderNotifier {
    const val CHANNEL_ID = "evening_questions"
    const val EXTRA_OPEN_QUESTIONS = "open_questions"
    private const val NOTIFICATION_ID = 7_001

    fun title(count: Int) = "Ada $count pertanyaan ringan hari ini"
    const val TEXT = "Jawab bentar, biar aku makin hafal kebiasaanmu."

    fun post(context: Context, count: Int): Boolean {
        if (Build.VERSION.SDK_INT >= 33 &&
            context.checkSelfPermission(Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED
        ) return false
        val manager = context.getSystemService(NotificationManager::class.java)
        if (manager.getNotificationChannel(CHANNEL_ID) == null) {
            manager.createNotificationChannel(NotificationChannel(CHANNEL_ID, "Pengingat malam", NotificationManager.IMPORTANCE_DEFAULT).apply {
                description = "Satu ringkasan pertanyaan ringan (Apa ini?) di jam yang kamu pilih"
                lockscreenVisibility = NotificationCompat.VISIBILITY_PRIVATE
            })
        }
        if (!manager.areNotificationsEnabled() ||
            manager.getNotificationChannel(CHANNEL_ID)?.importance == NotificationManager.IMPORTANCE_NONE
        ) return false
        val open = PendingIntent.getActivity(
            context, NOTIFICATION_ID,
            Intent(context, MainActivity::class.java).putExtra(EXTRA_OPEN_QUESTIONS, true)
                .addFlags(Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_ACTIVITY_CLEAR_TOP),
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
        )
        val notification = NotificationCompat.Builder(context, CHANNEL_ID)
            .setSmallIcon(R.drawable.ic_notification)
            .setContentTitle(title(count))
            .setContentText(TEXT)
            .setVisibility(NotificationCompat.VISIBILITY_PRIVATE)
            .setPublicVersion(NotificationCompat.Builder(context, CHANNEL_ID).setSmallIcon(R.drawable.ic_notification)
                .setContentTitle("Ada pertanyaan ringan").build())
            .setContentIntent(open)
            .setAutoCancel(true)
            .setCategory(NotificationCompat.CATEGORY_REMINDER)
            .build()
        return try {
            manager.notify(NOTIFICATION_ID, notification)
            true
        } catch (_: RuntimeException) {
            false
        }
    }
}
