package com.alfonsusenrico.financialtracker.listener.service

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
import com.alfonsusenrico.financialtracker.listener.data.model.RawNotificationEntity
import com.alfonsusenrico.financialtracker.listener.data.network.model.IngestResultItem
import com.alfonsusenrico.financialtracker.listener.data.sync.validUuid
import com.alfonsusenrico.financialtracker.listener.receiver.MappingConfirmReceiver
import java.text.NumberFormat
import java.util.Locale
import kotlin.math.roundToInt

/** Actionable notification asking the owner to confirm the account the AI proposed. */
object MappingConfirmationNotifier {
    private const val CHANNEL_ID = "mapping_confirmation"
    const val EXTRA_ROW_ID = "open_notification_row_id"

    data class Content(val title: String, val text: String, val confirmLabel: String)

    fun validProposal(item: IngestResultItem): Boolean {
        if (item.confirmation != null && (item.confirmation.type != "account" || !validUuid(item.confirmation.questionId))) return false
        val proposal = item.mappingProposal ?: return false
        val confidence = proposal.confidence ?: return false
        return item.status == "needs_confirmation" && validUuid(item.eventId) &&
            validUuid(proposal.proposed?.id) && !proposal.proposed?.label.isNullOrBlank() &&
            !proposal.name.isNullOrBlank() && confidence in 0.0..1.0
    }

    fun content(item: IngestResultItem, expectedAmount: Double?): Content {
        val proposal = item.mappingProposal!!
        val label = proposal.proposed!!.label!!
        val percent = ((proposal.confidence ?: 0.0) * 100).roundToInt()
        val amount = expectedAmount?.takeIf { it > 0 }?.let {
            " • Rp " + NumberFormat.getIntegerInstance(Locale("id", "ID")).format(it.toLong())
        }.orEmpty()
        return Content(
            title = "Konfirmasi rekening$amount",
            text = "“${proposal.name}” → $label? (yakin $percent%)",
            confirmLabel = "Ya, $label"
        )
    }

    private fun tag(eventId: String) = "confirm:$eventId"
    private fun notificationId(eventId: String) = tag(eventId).hashCode() and 0x7FFFFFFF

    private fun manager(context: Context): NotificationManager? {
        if (Build.VERSION.SDK_INT >= 33 &&
            context.checkSelfPermission(Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED
        ) return null
        val manager = context.getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager
        if (manager.getNotificationChannel(CHANNEL_ID) == null) {
            manager.createNotificationChannel(
                NotificationChannel(CHANNEL_ID, "Konfirmasi Rekening", NotificationManager.IMPORTANCE_DEFAULT).apply {
                    description = "Meminta konfirmasi rekening yang diusulkan untuk notifikasi bank"
                }
            )
        }
        return manager.takeIf { it.areNotificationsEnabled() }
    }

    private fun openIntent(context: Context, row: RawNotificationEntity, eventId: String): PendingIntent {
        val intent = Intent(context, MainActivity::class.java).apply {
            flags = Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_ACTIVITY_CLEAR_TOP
            putExtra(EXTRA_ROW_ID, row.id)
        }
        return PendingIntent.getActivity(
            context, notificationId(eventId), intent, PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
        )
    }

    /** [problem] says why an earlier confirmation from here did not go through. */
    fun post(context: Context, row: RawNotificationEntity, item: IngestResultItem, problem: String? = null): Boolean {
        if (!validProposal(item)) return false
        val manager = manager(context) ?: return false
        val eventId = item.eventId!!
        val content = content(item, row.expectedAmount)
        val confirm = PendingIntent.getBroadcast(
            context, notificationId(eventId),
            MappingConfirmReceiver.intent(context, eventId, item.mappingProposal!!.proposed!!.id!!, row.id, item.confirmation?.questionId),
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
        )
        val open = openIntent(context, row, eventId)
        val notification = NotificationCompat.Builder(context, CHANNEL_ID)
            .setSmallIcon(R.drawable.ic_notification)
            .setContentTitle(content.title)
            .setContentText(problem ?: content.text)
            .setStyle(NotificationCompat.BigTextStyle().bigText(problem?.let { "$it\n${content.text}" } ?: content.text))
            .setContentIntent(open)
            .addAction(0, content.confirmLabel, confirm)
            .addAction(0, "Pilih lain", open)
            .setAutoCancel(true)
            .setOnlyAlertOnce(true)
            .build()
        return try {
            manager.notify(tag(eventId), notificationId(eventId), notification)
            true
        } catch (_: RuntimeException) {
            false
        }
    }

    fun showFailure(context: Context, eventId: String, rowId: Long, message: String) {
        val manager = manager(context) ?: return
        val intent = Intent(context, MainActivity::class.java).apply {
            flags = Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_ACTIVITY_CLEAR_TOP
            putExtra(EXTRA_ROW_ID, rowId)
        }
        val open = PendingIntent.getActivity(
            context, notificationId(eventId), intent, PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
        )
        val notification = NotificationCompat.Builder(context, CHANNEL_ID)
            .setSmallIcon(R.drawable.ic_notification)
            .setContentTitle("Konfirmasi gagal")
            .setContentText("$message Buka aplikasi untuk mencoba lagi.")
            .setContentIntent(open)
            .setAutoCancel(true)
            .build()
        try {
            manager.notify(tag(eventId), notificationId(eventId), notification)
        } catch (_: RuntimeException) {
        }
    }

    fun cancel(context: Context, eventId: String) {
        val manager = context.getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager
        manager.cancel(tag(eventId), notificationId(eventId))
    }
}
