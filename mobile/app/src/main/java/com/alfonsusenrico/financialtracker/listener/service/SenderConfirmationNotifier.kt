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
import androidx.core.app.RemoteInput
import com.alfonsusenrico.financialtracker.listener.MainActivity
import com.alfonsusenrico.financialtracker.listener.R
import com.alfonsusenrico.financialtracker.listener.data.model.RawNotificationEntity
import com.alfonsusenrico.financialtracker.listener.data.network.model.IngestResultItem
import com.alfonsusenrico.financialtracker.listener.data.sync.SenderQuestions
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.receiver.SenderReplyReceiver
import java.text.NumberFormat
import java.util.Locale

object SenderConfirmationNotifier {
    const val CHANNEL_ID = "sender_confirmation"
    private fun tag(eventId: String) = "sender:$eventId"
    private fun id(eventId: String) = tag(eventId).hashCode() and Int.MAX_VALUE

    private fun manager(context: Context): NotificationManager? {
        if (Build.VERSION.SDK_INT >= 33 &&
            context.checkSelfPermission(Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED
        ) return null
        val manager = context.getSystemService(NotificationManager::class.java)
        if (manager.getNotificationChannel(CHANNEL_ID) == null) {
            manager.createNotificationChannel(NotificationChannel(CHANNEL_ID, "Konfirmasi Pengirim", NotificationManager.IMPORTANCE_DEFAULT).apply {
                lockscreenVisibility = NotificationCompat.VISIBILITY_PRIVATE
            })
        }
        return manager.takeIf {
            it.areNotificationsEnabled() && it.getNotificationChannel(CHANNEL_ID).importance != NotificationManager.IMPORTANCE_NONE
        }
    }

    private fun open(context: Context, eventId: String, rowId: Long): PendingIntent {
        val intent = Intent(context, MainActivity::class.java).apply {
            data = Uri.parse("cft://sender/$eventId/$rowId")
            flags = Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_ACTIVITY_CLEAR_TOP
            putExtra(MappingConfirmationNotifier.EXTRA_ROW_ID, rowId)
        }
        return PendingIntent.getActivity(context, id(eventId), intent, PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE)
    }

    private fun builder(context: Context, eventId: String, rowId: Long) = NotificationCompat.Builder(context, CHANNEL_ID)
        .setSmallIcon(R.drawable.ic_notification)
        .setVisibility(NotificationCompat.VISIBILITY_PRIVATE)
        .setPublicVersion(NotificationCompat.Builder(context, CHANNEL_ID)
            .setSmallIcon(R.drawable.ic_notification).setContentTitle("Konfirmasi pengirim diperlukan").build())
        .setContentIntent(open(context, eventId, rowId))
        .setOnlyAlertOnce(true)
        .setAutoCancel(false)

    /** Shows the question with its actions; [problem] says why an earlier answer from here was not stored. */
    fun post(context: Context, row: RawNotificationEntity, item: IngestResultItem, problem: String? = null): Boolean {
        if (!SenderQuestions.valid(item)) return false
        val manager = manager(context) ?: return false
        val event = item.eventId!!
        val question = item.confirmation!!
        val pairing = row.pairingContext ?: AppPreferences(context).pairingContext
        val reply = PendingIntent.getBroadcast(
            context, 0,
            SenderReplyReceiver.intent(context, row.id, event, question.questionId!!, pairing, "name"),
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_MUTABLE
        )
        val unknown = PendingIntent.getBroadcast(
            context, 0,
            SenderReplyReceiver.intent(context, row.id, event, question.questionId, pairing, "unknown"),
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
        )
        val nameAction = NotificationCompat.Action.Builder(0, "Balas nama", reply)
            .addRemoteInput(RemoteInput.Builder(SenderReplyReceiver.INPUT_NAME).setLabel("Nama pengirim").build())
            .setAllowGeneratedReplies(false)
        val unknownAction = NotificationCompat.Action.Builder(0, "Catat tanpa nama", unknown)
        if (Build.VERSION.SDK_INT >= 31) {
            nameAction.setAuthenticationRequired(true)
            unknownAction.setAuthenticationRequired(true)
        }
        val amount = NumberFormat.getIntegerInstance(Locale("id", "ID")).format(question.amount)
        val ask = "Rp $amount ke ${question.receivingAccount?.label}. Kenal ${question.maskedSender}? Isi namanya atau catat tanpa nama."
        val text = problem?.let { "$it $ask" } ?: ask
        val notification = builder(context, event, row.id)
            .setContentTitle("Siapa pengirim transfer ini?")
            .setContentText(problem ?: ask).setStyle(NotificationCompat.BigTextStyle().bigText(text))
            .addAction(nameAction.build()).addAction(unknownAction.build()).build()
        return try {
            manager.notify(tag(event), id(event), notification)
            true
        } catch (_: RuntimeException) {
            false
        }
    }

    fun showState(context: Context, eventId: String, rowId: Long, message: String) {
        val manager = manager(context) ?: return
        val notification = builder(context, eventId, rowId).setContentTitle("Konfirmasi pengirim")
            .setContentText(message).setStyle(NotificationCompat.BigTextStyle().bigText(message)).build()
        try {
            manager.notify(tag(eventId), id(eventId), notification)
        } catch (_: RuntimeException) {
            // A disabled or unavailable system channel leaves the durable question accessible in the app.
        }
    }

    fun cancel(context: Context, eventId: String) {
        context.getSystemService(NotificationManager::class.java).cancel(tag(eventId), id(eventId))
    }
}
