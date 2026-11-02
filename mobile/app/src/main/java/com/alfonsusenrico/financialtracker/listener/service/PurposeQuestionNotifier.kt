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
import com.alfonsusenrico.financialtracker.listener.data.network.model.PurposeQuestion
import com.alfonsusenrico.financialtracker.listener.data.sync.PurposeQuestions
import com.alfonsusenrico.financialtracker.listener.receiver.PurposeAnswerReceiver

/**
 * The "Apa ini?" prompt. It uses the record's alert slot (tag = record key, as in [TransactionNotifier]),
 * so one payment shows one notification, and the recorded alert replaces it once the question closes.
 */
object PurposeQuestionNotifier {
    const val CHANNEL_ID = "purpose_questions"

    /** Android's notification templates show at most three actions and drop the rest. */
    const val MAX_ACTIONS = 3
    const val ACTION_LABEL_MAX = 24
    const val TEXT_ACTION_LABEL = "Tulis"

    data class PromptAction(val suggestionId: String?, val label: String)
    data class PromptContent(val title: String, val text: String, val actions: List<PromptAction>)

    private fun id(recordKey: String) = recordKey.hashCode() and Int.MAX_VALUE

    /** The prompt's title, text and buttons; "Tulis" keeps its slot whenever text answers are allowed. */
    fun content(item: IngestResultItem, question: PurposeQuestion): PromptContent {
        val account = PurposeQuestions.accountLabel(item)?.takeIf { it.isNotBlank() }
        val title = "Apa ini? ${PurposeQuestions.rupiah(question.amount!!)}" + account?.let { " · $it" }.orEmpty()
        val suggestions = question.suggestions.orEmpty()
        val allowsText = question.allowsText == true
        val shown = suggestions.take(if (allowsText) MAX_ACTIONS - 1 else MAX_ACTIONS)
        val actions = shown.map { PromptAction(it.id, actionLabel(it.label!!)) } +
            if (allowsText) listOf(PromptAction(null, TEXT_ACTION_LABEL)) else emptyList()
        val hidden = suggestions.size - shown.size
        val text = question.prompt!! + if (hidden > 0) "\n$hidden pilihan lain ada di aplikasi." else ""
        return PromptContent(title, text, actions)
    }

    /** Collapses whitespace and shortens long labels at a word boundary where one is near the end. */
    fun actionLabel(label: String, max: Int = ACTION_LABEL_MAX): String {
        val collapsed = label.trim().replace(Regex("\\s+"), " ")
        if (collapsed.codePointCount(0, collapsed.length) <= max) return collapsed
        val cut = collapsed.substring(0, collapsed.offsetByCodePoints(0, max - 1))
        val space = cut.lastIndexOf(' ')
        return (if (space >= max / 2) cut.substring(0, space) else cut).trimEnd() + "…"
    }

    private fun manager(context: Context): NotificationManager? {
        if (Build.VERSION.SDK_INT >= 33 &&
            context.checkSelfPermission(Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED
        ) return null
        val manager = context.getSystemService(NotificationManager::class.java)
        if (manager.getNotificationChannel(CHANNEL_ID) == null) {
            // High importance: the owner should see the question within seconds of paying.
            manager.createNotificationChannel(NotificationChannel(CHANNEL_ID, "Apa ini?", NotificationManager.IMPORTANCE_HIGH).apply {
                description = "Pertanyaan tujuan pembayaran atau pemasukan tanpa nama merchant"
                lockscreenVisibility = NotificationCompat.VISIBILITY_PRIVATE
            })
        }
        return manager.takeIf {
            it.areNotificationsEnabled() && it.getNotificationChannel(CHANNEL_ID).importance != NotificationManager.IMPORTANCE_NONE
        }
    }

    private fun open(context: Context, row: RawNotificationEntity, recordKey: String): PendingIntent {
        val intent = Intent(context, MainActivity::class.java).apply {
            data = Uri.parse("cft://purpose/${row.backendEventId}/${row.id}")
            flags = Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_ACTIVITY_CLEAR_TOP
            putExtra(MappingConfirmationNotifier.EXTRA_ROW_ID, row.id)
        }
        return PendingIntent.getActivity(context, id(recordKey), intent, PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE)
    }

    private fun builder(context: Context, row: RawNotificationEntity, recordKey: String) =
        NotificationCompat.Builder(context, CHANNEL_ID)
            .setSmallIcon(R.drawable.ic_notification)
            .setVisibility(NotificationCompat.VISIBILITY_PRIVATE)
            .setPublicVersion(NotificationCompat.Builder(context, CHANNEL_ID)
                .setSmallIcon(R.drawable.ic_notification).setContentTitle("Ada transaksi yang perlu dijawab").build())
            .setContentIntent(open(context, row, recordKey))
            .setOnlyAlertOnce(true)
            .setAutoCancel(false)

    /** [problem] says why an earlier answer from here was not stored; the prompt then replaces it silently. */
    fun post(context: Context, row: RawNotificationEntity, item: IngestResultItem, activeOnly: Boolean, problem: String? = null): Boolean {
        val question = PurposeQuestions.valid(item) ?: return false
        val recordKey = item.recordKey!!
        val pairing = row.pairingContext ?: return false
        val manager = manager(context) ?: return false
        if (activeOnly && manager.activeNotifications.none { it.tag == recordKey && it.id == id(recordKey) }) return false
        val content = content(item, question)
        val notification = builder(context, row, recordKey)
            .setContentTitle(content.title)
            .setContentText(problem ?: content.text)
            .setStyle(NotificationCompat.BigTextStyle().bigText(problem?.let { "$it\n${content.text}" } ?: content.text))
            .setCategory(NotificationCompat.CATEGORY_REMINDER)
            .setSilent(activeOnly || problem != null)
        for (action in content.actions) {
            val answer = PurposeAnswerReceiver.intent(context, row.id, item.eventId!!, question.questionId!!, pairing, action.suggestionId)
            // The text action must stay mutable so Android can attach the RemoteInput result.
            val mutability = if (action.suggestionId == null) PendingIntent.FLAG_MUTABLE else PendingIntent.FLAG_IMMUTABLE
            val pending = PendingIntent.getBroadcast(context, 0, answer, PendingIntent.FLAG_UPDATE_CURRENT or mutability)
            val built = NotificationCompat.Action.Builder(0, action.label, pending)
            if (action.suggestionId == null) {
                built.addRemoteInput(RemoteInput.Builder(PurposeAnswerReceiver.INPUT_TEXT).setLabel("Untuk apa?").build())
                    .setAllowGeneratedReplies(false)
            }
            if (Build.VERSION.SDK_INT >= 31) built.setAuthenticationRequired(true)
            notification.addAction(built.build())
        }
        return try {
            manager.notify(recordKey, id(recordKey), notification.build())
            true
        } catch (_: RuntimeException) {
            false
        }
    }

    /** Replaces the prompt with an answer's state; it never alerts again. */
    fun showState(context: Context, row: RawNotificationEntity, message: String) {
        val recordKey = row.recordKey ?: return
        val manager = manager(context) ?: return
        val notification = builder(context, row, recordKey).setContentTitle("Apa ini?")
            .setContentText(message).setStyle(NotificationCompat.BigTextStyle().bigText(message))
            .setSilent(true).build()
        try {
            manager.notify(recordKey, id(recordKey), notification)
        } catch (_: RuntimeException) {
            // The answer's state stays visible in the app when Android cannot post it.
        }
    }

    fun cancel(context: Context, recordKey: String) {
        context.getSystemService(NotificationManager::class.java).cancel(recordKey, id(recordKey))
    }
}
