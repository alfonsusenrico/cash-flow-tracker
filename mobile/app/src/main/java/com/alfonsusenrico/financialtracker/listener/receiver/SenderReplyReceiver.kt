package com.alfonsusenrico.financialtracker.listener.receiver

import android.app.KeyguardManager
import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.net.Uri
import androidx.core.app.RemoteInput
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.sync.AnswerAttempts
import com.alfonsusenrico.financialtracker.listener.data.sync.NotificationAnswer
import com.alfonsusenrico.financialtracker.listener.data.sync.SenderReplies
import com.alfonsusenrico.financialtracker.listener.data.sync.validUuid
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch

class SenderReplyReceiver : BroadcastReceiver() {
    companion object {
        const val ACTION_REPLY = "com.alfonsusenrico.financialtracker.listener.CONFIRM_SENDER"
        const val INPUT_NAME = "sender_name"
        private val MESSAGES = NotificationAnswer.Messages(
            locked = "Buka kunci lalu balas lagi.",
            missingInput = "Isi nama pengirim, maksimal 80 karakter.",
            notSaved = "Balasan belum tersimpan. Coba lagi atau jawab di aplikasi."
        )

        fun intent(context: Context, row: Long, event: String, question: String, pairing: String, replyAction: String) =
            Intent(context, SenderReplyReceiver::class.java).apply {
                action = ACTION_REPLY
                data = Uri.Builder().scheme("cft").authority("sender").appendPath(event).appendPath(question)
                    .appendPath(pairing).appendPath(replyAction).build()
                putExtra("row_id", row)
            }
    }

    private class Reply(val rowId: Long, val event: String, val question: String, val pairing: String, val action: String)

    private fun parse(intent: Intent): Reply? {
        if (intent.data?.scheme != "cft" || intent.data?.authority != "sender") return null
        val parts = intent.data?.pathSegments ?: return null
        if (parts.size != 4 || parts.take(3).any { !validUuid(it) } || parts[3] !in setOf("name", "unknown")) return null
        val rowId = intent.getLongExtra("row_id", -1)
        if (rowId < 1) return null
        return Reply(rowId, parts[0], parts[1], parts[2], parts[3])
    }

    override fun onReceive(context: Context, intent: Intent) {
        if (intent.action != ACTION_REPLY) return
        val app = context.applicationContext
        val reply = parse(intent)
        // API 26–30 cannot authenticate individual actions; the owner answers again once unlocked.
        val locked = context.getSystemService(KeyguardManager::class.java).isDeviceLocked
        val input = if (reply?.action == "name") {
            RemoteInput.getResultsFromIntent(intent)?.getCharSequence(INPUT_NAME)?.toString()
        } else {
            null
        }
        val pending = goAsync()
        CoroutineScope(SupervisorJob() + Dispatchers.IO).launch {
            try {
                handle(app, reply, locked, input)
            } finally {
                pending.finish()
            }
        }
    }

    private suspend fun handle(app: Context, reply: Reply?, locked: Boolean, input: String?) {
        val db = AppDatabase.getDatabase(app)
        if (reply == null) {
            AnswerAttempts.record(db, AnswerAttempts.NOTIFICATION, AnswerAttempts.SENDER, null, null, null, AnswerAttempts.MALFORMED)
            return
        }
        val replies = SenderReplies(app, db)
        NotificationAnswer(db, AnswerAttempts.SENDER, reply.rowId, reply.event, reply.question, MESSAGES).run(
            locked = locked,
            missingInput = reply.action == "name" && input == null,
            store = { replies.enqueue(reply.rowId, reply.event, reply.question, reply.pairing, reply.action, input) },
            offerAgain = { problem -> replies.offerAgain(reply.rowId, reply.event, problem) }
        )
    }
}
