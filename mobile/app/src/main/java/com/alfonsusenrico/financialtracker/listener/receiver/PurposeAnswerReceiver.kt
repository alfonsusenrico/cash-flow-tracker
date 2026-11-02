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
import com.alfonsusenrico.financialtracker.listener.data.sync.PurposeAnswers
import com.alfonsusenrico.financialtracker.listener.data.sync.validUuid
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch

class PurposeAnswerReceiver : BroadcastReceiver() {
    companion object {
        const val ACTION_ANSWER = "com.alfonsusenrico.financialtracker.listener.ANSWER_PURPOSE"
        const val INPUT_TEXT = "purpose_text"
        private const val TEXT = "text"
        private const val SUGGESTION = "suggestion"
        private val SUGGESTION_ID = Regex("[A-Za-z0-9_-]{1,16}")
        private val MESSAGES = NotificationAnswer.Messages(
            locked = "Buka kunci lalu jawab lagi.",
            missingInput = "Tulis jawabannya lalu kirim lagi.",
            notSaved = "Jawaban belum tersimpan. Coba lagi atau jawab di aplikasi."
        )

        /** One distinct intent per action: the data URI names the question and the chosen answer. */
        fun intent(context: Context, row: Long, event: String, question: String, pairing: String, suggestionId: String?) =
            Intent(context, PurposeAnswerReceiver::class.java).apply {
                action = ACTION_ANSWER
                data = Uri.Builder().scheme("cft").authority("purpose").appendPath(event).appendPath(question)
                    .appendPath(pairing).appendPath(if (suggestionId == null) TEXT else SUGGESTION)
                    .appendPath(suggestionId ?: TEXT).build()
                putExtra("row_id", row)
            }
    }

    private class Answer(val rowId: Long, val event: String, val question: String, val pairing: String, val suggestionId: String?)

    private fun parse(intent: Intent): Answer? {
        if (intent.data?.scheme != "cft" || intent.data?.authority != "purpose") return null
        val parts = intent.data?.pathSegments ?: return null
        if (parts.size != 5 || parts.take(3).any { !validUuid(it) } || parts[3] !in setOf(TEXT, SUGGESTION) ||
            parts[3] == SUGGESTION && !parts[4].matches(SUGGESTION_ID)
        ) return null
        val rowId = intent.getLongExtra("row_id", -1)
        if (rowId < 1) return null
        return Answer(rowId, parts[0], parts[1], parts[2], parts[4].takeIf { parts[3] == SUGGESTION })
    }

    override fun onReceive(context: Context, intent: Intent) {
        if (intent.action != ACTION_ANSWER) return
        val app = context.applicationContext
        val answer = parse(intent)
        // API 26–30 cannot authenticate individual actions; the owner answers again once unlocked.
        val locked = context.getSystemService(KeyguardManager::class.java).isDeviceLocked
        val text = if (answer != null && answer.suggestionId == null) {
            RemoteInput.getResultsFromIntent(intent)?.getCharSequence(INPUT_TEXT)?.toString()
        } else {
            null
        }
        val pending = goAsync()
        CoroutineScope(SupervisorJob() + Dispatchers.IO).launch {
            try {
                handle(app, answer, locked, text)
            } finally {
                pending.finish()
            }
        }
    }

    private suspend fun handle(app: Context, answer: Answer?, locked: Boolean, text: String?) {
        val db = AppDatabase.getDatabase(app)
        if (answer == null) {
            AnswerAttempts.record(db, AnswerAttempts.NOTIFICATION, AnswerAttempts.PURPOSE, null, null, null, AnswerAttempts.MALFORMED)
            return
        }
        val answers = PurposeAnswers(app, db)
        NotificationAnswer(db, AnswerAttempts.PURPOSE, answer.rowId, answer.event, answer.question, MESSAGES).run(
            locked = locked,
            missingInput = answer.suggestionId == null && text == null,
            store = {
                answers.enqueue(answer.rowId, answer.question, answer.suggestionId, text, expectedEvent = answer.event, expectedPairing = answer.pairing)
            },
            offerAgain = { problem -> answers.offerAgain(answer.rowId, problem) }
        )
    }
}
