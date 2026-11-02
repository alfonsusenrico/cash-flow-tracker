package com.alfonsusenrico.financialtracker.listener.service

import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.Manifest
import android.content.Context
import android.content.Intent
import android.content.pm.PackageManager
import android.graphics.BitmapFactory
import android.os.Build
import androidx.core.app.NotificationCompat
import com.alfonsusenrico.financialtracker.listener.MainActivity
import com.alfonsusenrico.financialtracker.listener.R
import com.alfonsusenrico.financialtracker.listener.data.network.model.IngestResultItem
import com.alfonsusenrico.financialtracker.listener.data.sync.completeRecordedResult
import java.text.NumberFormat
import java.util.Locale

object TransactionNotifier {
    private const val CHANNEL_ID = "transaction_alerts"

    fun notifyRecorded(context: Context, item: IngestResultItem, activeOnly: Boolean = false): Boolean {
        if (!completeRecordedResult(item)) return false
        if (Build.VERSION.SDK_INT >= 33 &&
            context.checkSelfPermission(Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED
        ) return false
        return try {
            val manager = context.getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager
            if (manager.getNotificationChannel(CHANNEL_ID) == null) {
                val channel = NotificationChannel(
                    CHANNEL_ID,
                    "Transaction Alerts",
                    NotificationManager.IMPORTANCE_DEFAULT
                ).apply {
                    description = "Shows confirmation alerts when a new transaction is recorded"
                }
                manager.createNotificationChannel(channel)
            }
            if (!manager.areNotificationsEnabled() ||
                manager.getNotificationChannel(CHANNEL_ID)?.importance == NotificationManager.IMPORTANCE_NONE
            ) return false

            val amountVal = item.amount!!
            val formattedAmount = NumberFormat.getIntegerInstance(Locale("id", "ID")).format(amountVal)

            val title = when (item.type?.lowercase()) {
                "income" -> "Income recorded • Rp $formattedAmount"
                "internal_movement", "transfer" -> "Transfer recorded • Rp $formattedAmount"
                "investment" -> "Investment recorded • Rp $formattedAmount"
                else -> "Expense recorded • Rp $formattedAmount"
            }

            val body = when (item.type?.lowercase()) {
                "internal_movement", "transfer" -> {
                    if (!item.source.isNullOrBlank() && !item.target.isNullOrBlank()) {
                        "${item.source} ➔ ${item.target}"
                    } else {
                        item.description ?: "${item.source ?: ""} ➔ ${item.target ?: ""}".trim()
                    }
                }
                "income" -> {
                    "${item.target} • ${item.description}"
                }
                "investment" -> {
                    val targetPart = if (!item.target.isNullOrBlank()) " ➔ ${item.target}" else ""
                    val descPart = if (!item.description.isNullOrBlank()) " • ${item.description}" else ""
                    "${item.source ?: "Portfolio"}$targetPart$descPart"
                }
                else -> {
                    // Regular expense
                    val sourcePrefix = if (!item.source.isNullOrBlank()) "${item.source} • " else ""
                    val catSuffix = if (!item.category.isNullOrBlank()) " [${item.category}]" else ""
                    val desc = item.description ?: "Expense"
                    "$sourcePrefix$desc$catSuffix"
                }
            }

            val intent = Intent(context, MainActivity::class.java).apply {
                flags = Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_ACTIVITY_CLEAR_TOP
            }
            val notifId = item.recordKey!!.hashCode() and 0x7FFFFFFF
            if (activeOnly && manager.activeNotifications.none { it.tag == item.recordKey && it.id == notifId }) {
                return false
            }
            val pendingIntent = PendingIntent.getActivity(
                context,
                notifId,
                intent,
                PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
            )

            val appIconBitmap = BitmapFactory.decodeResource(context.resources, R.mipmap.ic_launcher)
            val notification = NotificationCompat.Builder(context, CHANNEL_ID)
                .setContentTitle(title)
                .setContentText(body)
                .setSmallIcon(R.drawable.ic_notification)
                .setLargeIcon(appIconBitmap)
                .setContentIntent(pendingIntent)
                .setAutoCancel(true)
                .setOnlyAlertOnce(true)
                .setSilent(activeOnly)
                .build()

            manager.notify(item.recordKey, notifId, notification)
            true
        } catch (_: RuntimeException) {
            false
        }
    }
}
