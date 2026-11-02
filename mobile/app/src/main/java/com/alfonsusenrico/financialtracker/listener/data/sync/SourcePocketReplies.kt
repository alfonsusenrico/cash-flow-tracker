package com.alfonsusenrico.financialtracker.listener.data.sync

import android.app.KeyguardManager
import android.content.Context
import androidx.room.withTransaction
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.model.SenderReplyEntity
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.service.SourcePocketConfirmationNotifier
import java.util.UUID

class SourcePocketReplies(
    private val context: Context,
    private val db: AppDatabase = AppDatabase.getDatabase(context),
    private val schedule: (String, Long) -> Unit = { id, delay -> SyncScheduler.enqueueSenderReply(context, id, delay) }
) {
    suspend fun enqueue(rowId: Long, questionId: String, accountId: String): SenderReplies.Outcome {
        val prefs = AppPreferences(context)
        if (!prefs.isConfigured || !validUuid(accountId) || context.getSystemService(KeyguardManager::class.java).isDeviceLocked) {
            return SenderReplies.Outcome(false, "Buka kunci ponsel dan hubungkan kembali akun.")
        }
        val reply = db.withTransaction {
            val row = db.notificationDao().getById(rowId) ?: return@withTransaction null
            val item = SourcePocketQuestions.parse(row.resultJson) ?: return@withTransaction null
            val options = SourcePocketQuestions.cached(row.sourcePocketOptionsJson, item) ?: return@withTransaction null
            if (row.processingStatus != "needs_confirmation" || row.pairingContext != prefs.pairingContext ||
                row.backendEventId != item.eventId || item.confirmation?.questionId != questionId ||
                options.options?.none { it.id == accountId } != false
            ) return@withTransaction null
            val dao = db.senderReplyDao()
            val previous = dao.forQuestion(prefs.pairingContext, item.eventId!!, questionId)
            if (previous != null && previous.state !in setOf("invalid", "stale")) return@withTransaction previous
            if (previous != null) dao.removeRejected(previous.replyId)
            val created = SenderReplyEntity(
                UUID.randomUUID().toString(), rowId, prefs.pairingContext, item.eventId, questionId,
                action = "select", name = null, answerKind = "source_pocket", accountId = accountId
            )
            dao.insert(created)
            dao.forQuestion(prefs.pairingContext, item.eventId, questionId)
        } ?: return SenderReplies.Outcome(false, "Pertanyaan berubah. Segarkan pilihan kantong.")
        if (reply.answerKind != "source_pocket" || reply.accountId != accountId) {
            return SenderReplies.Outcome(false, "Pilihan sebelumnya tersimpan; tunggu hasil server.")
        }
        if (reply.state !in setOf("queued", "waiting_auth")) return SenderReplies.Outcome(false, sourcePocketReplyMessage(reply.state))
        SourcePocketConfirmationNotifier.showState(context, reply.eventId, rowId, sourcePocketReplyMessage("queued"))
        schedule(reply.replyId, 0)
        return SenderReplies.Outcome(true)
    }
}

fun sourcePocketReplyMessage(state: String): String = when (state) {
    "queued" -> "Pilihan menunggu koneksi; belum dicatat."
    "waiting_auth", "waiting_pairing" -> "Pilihan belum dikirim. Periksa koneksi akun di Pengaturan."
    "invalid" -> "Pilihan belum diterima. Segarkan dan pilih kantong kembali."
    "stale" -> "Pertanyaan atau kantong berubah. Segarkan hasil notifikasi."
    else -> "Pilihan diterima server; menunggu pencatatan."
}
