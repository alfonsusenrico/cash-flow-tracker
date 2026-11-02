package com.alfonsusenrico.financialtracker.listener.data.report

import android.Manifest
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.content.Context
import android.content.Intent
import android.content.pm.PackageManager
import android.net.Uri
import android.os.Build
import androidx.core.app.NotificationCompat
import com.alfonsusenrico.financialtracker.listener.R
import com.alfonsusenrico.financialtracker.listener.data.prefs.StoredReport

/** "Laporan Oktober 2026 udah siap": tapping it opens the report on the web. */
object ReportNotifier {
    const val CHANNEL_ID = "monthly_report"
    private const val NOTIFICATION_ID = 7_101

    fun title(report: StoredReport) = "Laporan ${report.label} udah siap"
    const val TEXT = "Buka di web buat lihat ringkasan bulanmu."

    fun post(context: Context, report: StoredReport): Boolean {
        val url = WebLinks.url(report.url) ?: return false
        if (Build.VERSION.SDK_INT >= 33 &&
            context.checkSelfPermission(Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED
        ) return false
        val manager = context.getSystemService(NotificationManager::class.java)
        if (manager.getNotificationChannel(CHANNEL_ID) == null) {
            manager.createNotificationChannel(NotificationChannel(CHANNEL_ID, "Laporan bulanan", NotificationManager.IMPORTANCE_DEFAULT).apply {
                description = "Kabar sekali sebulan saat laporan bulananmu siap di web"
            })
        }
        if (!manager.areNotificationsEnabled() ||
            manager.getNotificationChannel(CHANNEL_ID)?.importance == NotificationManager.IMPORTANCE_NONE
        ) return false
        val open = PendingIntent.getActivity(
            context, NOTIFICATION_ID,
            Intent(Intent.ACTION_VIEW, Uri.parse(url)).addFlags(Intent.FLAG_ACTIVITY_NEW_TASK),
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
        )
        val notification = NotificationCompat.Builder(context, CHANNEL_ID)
            .setSmallIcon(R.drawable.ic_notification)
            .setContentTitle(title(report))
            .setContentText(TEXT)
            .setContentIntent(open)
            .setAutoCancel(true)
            .build()
        return try {
            manager.notify(NOTIFICATION_ID, notification)
            true
        } catch (_: RuntimeException) {
            false
        }
    }
}
