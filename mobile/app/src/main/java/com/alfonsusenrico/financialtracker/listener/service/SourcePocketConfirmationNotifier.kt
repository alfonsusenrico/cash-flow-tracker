package com.alfonsusenrico.financialtracker.listener.service

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
import com.alfonsusenrico.financialtracker.listener.MainActivity
import com.alfonsusenrico.financialtracker.listener.R
import com.alfonsusenrico.financialtracker.listener.data.model.RawNotificationEntity
import com.alfonsusenrico.financialtracker.listener.data.network.model.IngestResultItem
import com.alfonsusenrico.financialtracker.listener.data.sync.SourcePocketQuestions
import java.text.NumberFormat
import java.util.Locale

object SourcePocketConfirmationNotifier {
    const val CHANNEL_ID = "source_pocket_confirmation"
    private fun tag(eventId: String) = "source-pocket:$eventId"
    private fun id(eventId: String) = tag(eventId).hashCode() and Int.MAX_VALUE

    private fun manager(context: Context): NotificationManager? {
        if (Build.VERSION.SDK_INT >= 33 &&
            context.checkSelfPermission(Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED
        ) return null
        val manager = context.getSystemService(NotificationManager::class.java)
        if (manager.getNotificationChannel(CHANNEL_ID) == null) {
            manager.createNotificationChannel(NotificationChannel(CHANNEL_ID, "Konfirmasi Kantong Jago", NotificationManager.IMPORTANCE_DEFAULT).apply {
                lockscreenVisibility = NotificationCompat.VISIBILITY_PRIVATE
            })
        }
        return manager.takeIf { it.areNotificationsEnabled() && it.getNotificationChannel(CHANNEL_ID).importance != NotificationManager.IMPORTANCE_NONE }
    }

    private fun open(context: Context, eventId: String, questionId: String?, rowId: Long): PendingIntent {
        val intent = Intent(context, MainActivity::class.java).apply {
            data = Uri.parse("cft://source-pocket/$eventId/${questionId ?: "pending"}/$rowId")
            flags = Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_ACTIVITY_CLEAR_TOP
            putExtra(MappingConfirmationNotifier.EXTRA_ROW_ID, rowId)
        }
        return PendingIntent.getActivity(context, 0, intent, PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE)
    }

    private fun builder(context: Context, event: String, row: Long, question: String? = null) =
        NotificationCompat.Builder(context, CHANNEL_ID)
            .setSmallIcon(R.drawable.ic_notification)
            .setVisibility(NotificationCompat.VISIBILITY_PRIVATE)
            .setPublicVersion(NotificationCompat.Builder(context, CHANNEL_ID)
                .setSmallIcon(R.drawable.ic_notification).setContentTitle("Konfirmasi sumber dana diperlukan").build())
            .setContentIntent(open(context, event, question, row))
            .setOnlyAlertOnce(true)
            .setAutoCancel(false)

    fun post(context: Context, row: RawNotificationEntity, item: IngestResultItem): Boolean {
        if (!SourcePocketQuestions.valid(item)) return false
        val manager = manager(context) ?: return false
        val question = item.confirmation!!
        val event = item.eventId!!
        val amount = NumberFormat.getIntegerInstance(Locale("id", "ID")).format(question.amount)
        val text = "Rp $amount" + question.destinationLabel?.let { " ke $it" }.orEmpty()
        val action = NotificationCompat.Action.Builder(0, "Pilih sumber dana", open(context, event, question.questionId, row.id))
        if (Build.VERSION.SDK_INT >= 31) action.setAuthenticationRequired(true)
        return try {
            manager.notify(tag(event), id(event), builder(context, event, row.id, question.questionId)
                .setContentTitle("Pilih sumber dana").setContentText(text)
                .addAction(action.build()).build())
            true
        } catch (_: RuntimeException) {
            false
        }
    }

    fun showState(context: Context, eventId: String, rowId: Long, message: String) {
        val manager = manager(context) ?: return
        try {
            manager.notify(tag(eventId), id(eventId), builder(context, eventId, rowId)
                .setContentTitle("Konfirmasi sumber dana").setContentText(message).build())
        } catch (_: RuntimeException) {
            // The durable inbox state remains available when Android cannot post an alert.
        }
    }

    fun cancel(context: Context, eventId: String) {
        context.getSystemService(NotificationManager::class.java).cancel(tag(eventId), id(eventId))
    }
}
