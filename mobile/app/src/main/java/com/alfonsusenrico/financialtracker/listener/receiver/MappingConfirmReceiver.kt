package com.alfonsusenrico.financialtracker.listener.receiver

import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.net.Uri
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.sync.AnswerAttempts
import com.alfonsusenrico.financialtracker.listener.data.sync.MappingConfirmation
import com.alfonsusenrico.financialtracker.listener.data.sync.NotificationAnswer
import com.alfonsusenrico.financialtracker.listener.data.sync.SenderReplies
import com.alfonsusenrico.financialtracker.listener.data.sync.validUuid
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch

/** Confirms a proposed account from the notification shade without opening the app. */
class MappingConfirmReceiver : BroadcastReceiver() {

    companion object {
        const val ACTION_CONFIRM = "com.alfonsusenrico.financialtracker.listener.CONFIRM_MAPPING"
        private const val EXTRA_EVENT_ID = "event_id"
        private const val EXTRA_ACCOUNT_ID = "account_id"
        private const val EXTRA_ROW_ID = "row_id"
        private val MESSAGES = NotificationAnswer.Messages(notSaved = "Konfirmasi belum terkirim. Coba lagi atau buka aplikasi.")

        fun intent(context: Context, eventId: String, accountId: String, rowId: Long, questionId: String? = null): Intent =
            Intent(context, MappingConfirmReceiver::class.java).apply {
                action = ACTION_CONFIRM
                data = Uri.parse("cft://account/$eventId/${questionId ?: "legacy"}/$accountId")
                putExtra("question_id", questionId)
                putExtra(EXTRA_EVENT_ID, eventId)
                putExtra(EXTRA_ACCOUNT_ID, accountId)
                putExtra(EXTRA_ROW_ID, rowId)
            }
    }

    override fun onReceive(context: Context, intent: Intent) {
        if (intent.action != ACTION_CONFIRM) return
        val eventId = intent.getStringExtra(EXTRA_EVENT_ID)
        val accountId = intent.getStringExtra(EXTRA_ACCOUNT_ID)
        val rowId = intent.getLongExtra(EXTRA_ROW_ID, -1L)
        val questionId = intent.getStringExtra("question_id")
        val appContext = context.applicationContext
        val pending = goAsync()
        CoroutineScope(SupervisorJob() + Dispatchers.IO).launch {
            try {
                val db = AppDatabase.getDatabase(appContext)
                if (eventId == null || accountId == null || !validUuid(eventId) || !validUuid(accountId)) {
                    AnswerAttempts.record(db, AnswerAttempts.NOTIFICATION, AnswerAttempts.ACCOUNT, null, null, null, AnswerAttempts.MALFORMED)
                    return@launch
                }
                val confirmation = MappingConfirmation(appContext, db)
                NotificationAnswer(db, AnswerAttempts.ACCOUNT, rowId.takeIf { it > 0 }, eventId, questionId, MESSAGES).run(
                    // Account proposals carry no authentication requirement and no typed input.
                    locked = false,
                    missingInput = false,
                    store = {
                        confirmation.confirm(eventId, accountId, questionId).let { SenderReplies.Outcome(it.success, it.message, it.code) }
                    },
                    offerAgain = { problem -> confirmation.offerAgain(eventId, rowId, problem) }
                )
            } finally {
                pending.finish()
            }
        }
    }
}
