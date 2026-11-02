package com.alfonsusenrico.financialtracker.listener.data.sync

import android.content.Context
import androidx.room.withTransaction
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.model.RawNotificationEntity
import com.alfonsusenrico.financialtracker.listener.data.model.SenderReplyEntity
import com.alfonsusenrico.financialtracker.listener.data.network.model.IngestResultItem
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.service.SenderConfirmationNotifier
import java.util.UUID

class SenderReplies(
    private val context: Context,
    private val db: AppDatabase = AppDatabase.getDatabase(context),
    private val schedule: (String, Long) -> Unit = { id, delay -> SyncScheduler.enqueueSenderReply(context, id, delay) },
    private val showQueued: (String, Long) -> Unit = { event, row ->
        SenderConfirmationNotifier.showState(context, event, row, "Balasan tersimpan; menunggu server.")
    }
) {
    /** [code] is the [AnswerAttempts] reason for this outcome; the source-pocket outbox records none and leaves it null. */
    data class Outcome(val queued: Boolean, val message: String? = null, val code: String? = null)

    suspend fun enqueue(rowId: Long, eventId: String, questionId: String, pairing: String, action: String, input: String?): Outcome {
        val prefs = AppPreferences(context)
        if (!prefs.isConfigured || pairing != prefs.pairingContext) {
            return Outcome(false, "Hubungkan kembali akun yang menerima notifikasi ini.", AnswerAttempts.NOT_PAIRED)
        }
        val name = if (action == "name") SenderQuestions.normalizeName(input.orEmpty()) else null
        if (action !in setOf("name", "unknown") || action == "name" && name == null || action == "unknown" && input != null) {
            return Outcome(false, "Isi nama pengirim, maksimal 80 karakter.", AnswerAttempts.INVALID_NAME)
        }
        val reply = db.withTransaction {
            val row = db.notificationDao().getById(rowId) ?: return@withTransaction null
            val item = SenderQuestions.parse(row.resultJson) ?: return@withTransaction null
            if (row.processingStatus != "needs_confirmation" || row.pairingContext != pairing ||
                row.backendEventId != eventId || item.eventId != eventId || item.confirmation?.questionId != questionId
            ) return@withTransaction null
            val dao = db.senderReplyDao()
            val previous = dao.forQuestion(pairing, eventId, questionId)
            if (previous != null && previous.state != "invalid") return@withTransaction previous
            if (previous != null) dao.removeInvalid(previous.replyId)
            val created = SenderReplyEntity(UUID.randomUUID().toString(), rowId, pairing, eventId, questionId, action, name)
            dao.insert(created)
            dao.forQuestion(pairing, eventId, questionId)
        } ?: return Outcome(false, "Pertanyaan sudah berubah. Buka kembali notifikasi ini.", AnswerAttempts.QUESTION_CHANGED)
        if (reply.action != action || reply.name != name) {
            return Outcome(false, "Balasan sebelumnya sudah tersimpan. Tunggu hasil server.", AnswerAttempts.ALREADY_ANSWERED)
        }
        if (reply.state !in setOf("queued", "waiting_auth")) return Outcome(false, senderReplyMessage(reply.state), AnswerAttempts.ALREADY_ANSWERED)
        showQueued(eventId, rowId)
        schedule(reply.replyId, 0)
        return Outcome(true, null, AnswerAttempts.QUEUED)
    }

    /**
     * After an answer from the notification was not stored, shows the row's open sender question again with its
     * actions and [problem], so it stays answerable there. When that question already has a stored answer, or the
     * row asks none, only [problem] is shown.
     */
    suspend fun offerAgain(rowId: Long, eventId: String, problem: String) {
        val reposted = openQuestion(rowId, eventId)?.let { (row, item) ->
            SenderConfirmationNotifier.post(context, row, item, problem)
        } == true
        if (!reposted) SenderConfirmationNotifier.showState(context, eventId, rowId, problem)
    }

    /** The row and its sender question while that question still waits for an answer on this phone. */
    private suspend fun openQuestion(rowId: Long, eventId: String): Pair<RawNotificationEntity, IngestResultItem>? {
        val row = db.notificationDao().getById(rowId) ?: return null
        val item = SenderQuestions.parse(row.resultJson) ?: return null
        val pairing = row.pairingContext ?: return null
        val questionId = item.confirmation?.questionId ?: return null
        if (row.processingStatus != "needs_confirmation" || row.backendEventId != eventId || item.eventId != eventId) return null
        val stored = db.senderReplyDao().forQuestion(pairing, eventId, questionId)
        return if (stored == null || stored.state == "invalid") row to item else null
    }

    suspend fun recover() {
        val prefs = AppPreferences(context)
        if (!prefs.isConfigured) return
        for (reply in db.senderReplyDao().recoverable(prefs.pairingContext)) {
            db.senderReplyDao().defer(reply.replyId, "queued", reply.attempt, reply.nextAttemptAt, reply.errorCode)
            schedule(reply.replyId, reply.nextAttemptAt - System.currentTimeMillis())
        }
    }
}

fun senderReplyMessage(state: String): String = when (state) {
    "queued" -> "Balasan tersimpan; menunggu server."
    "waiting_auth", "waiting_pairing" -> "Balasan belum dikirim. Periksa koneksi akun di Pengaturan."
    "invalid" -> "Nama belum diterima. Periksa nama lalu kirim kembali."
    "stale" -> "Pertanyaan sudah berubah. Muat ulang hasil notifikasi."
    else -> "Balasan sudah diterima server; menunggu hasil pencatatan."
}
