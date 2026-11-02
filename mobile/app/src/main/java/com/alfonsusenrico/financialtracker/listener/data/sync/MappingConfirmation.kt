package com.alfonsusenrico.financialtracker.listener.data.sync

import android.content.Context
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.network.ApiClient
import com.alfonsusenrico.financialtracker.listener.data.network.model.MappingConfirmRequest
import com.alfonsusenrico.financialtracker.listener.data.network.serverMessage
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import kotlinx.coroutines.CancellationException
import com.google.gson.Gson
import com.alfonsusenrico.financialtracker.listener.data.network.model.IngestResultItem
import com.alfonsusenrico.financialtracker.listener.service.MappingConfirmationNotifier

/** Sends the owner's account choice for an event awaiting confirmation and applies the result. */
class MappingConfirmation(
    private val context: Context,
    private val db: AppDatabase = AppDatabase.getDatabase(context)
) {
    /** [code] is the [AnswerAttempts] reason for this outcome. */
    data class Outcome(val success: Boolean, val message: String?, val code: String)

    suspend fun confirm(eventId: String, accountId: String, questionId: String? = null): Outcome {
        val prefs = AppPreferences(context)
        if (!prefs.isConfigured) return Outcome(false, "Aplikasi belum terhubung ke server.", AnswerAttempts.NOT_PAIRED)
        val pairingContext = prefs.pairingContext
        val dao = db.notificationDao()
        val row = dao.getByBackendEventId(eventId, pairingContext)
            ?: return Outcome(false, "Notifikasi tidak ditemukan di aplikasi.", AnswerAttempts.QUESTION_CHANGED)
        if (row.processingStatus != "needs_confirmation") {
            return Outcome(false, "Notifikasi ini tidak menunggu konfirmasi rekening.", AnswerAttempts.QUESTION_CHANGED)
        }
        return try {
            val stored = Gson().fromJson(row.resultJson, IngestResultItem::class.java)
            if (stored == null || !MappingConfirmationNotifier.validProposal(stored) ||
                questionId != null && stored.confirmation?.questionId != questionId
            ) return Outcome(false, "Pertanyaan sudah berubah. Buka kembali notifikasi ini.", AnswerAttempts.QUESTION_CHANGED)
            val response = ApiClient(context).createService().confirmMapping(
                eventId, MappingConfirmRequest(accountId, stored.confirmation?.questionId)
            )
            val item = response.body()?.result
            if (!response.isSuccessful || response.body()?.ok != true || item == null || item.eventId != eventId) {
                return Outcome(false, serverMessage(response), AnswerAttempts.SERVER_REFUSED)
            }
            val results = NotificationResults(context, db)
            if (!results.consume(row, item, pairingContext)) return Outcome(false, "Balasan server tidak valid.", AnswerAttempts.INVALID_RESULT)
            val saved = dao.getById(row.id)
            if (saved?.backendEventId != null && saved.processingStatus in setOf("queued", "processing")) {
                SyncScheduler.enqueueCompletion(
                    context, saved.backendEventId, (saved.nextCheckAt ?: 0L) - System.currentTimeMillis(), true
                )
            }
            results.deliverPendingAlerts()
            Outcome(true, null, AnswerAttempts.CONFIRMED)
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Exception) {
            Outcome(false, "Koneksi tidak tersedia.", AnswerAttempts.NETWORK_UNAVAILABLE)
        }
    }

    /**
     * After a confirmation from the notification did not go through, shows the event's open account proposal
     * again with its actions and [problem], so it stays answerable there; otherwise only [problem] is shown.
     */
    suspend fun offerAgain(eventId: String, rowId: Long, problem: String) {
        val row = db.notificationDao().getByBackendEventId(eventId, AppPreferences(context).pairingContext)
        val item = row?.takeIf { it.processingStatus == "needs_confirmation" }?.let {
            Gson().fromJson(it.resultJson, IngestResultItem::class.java)
        }?.takeIf(MappingConfirmationNotifier::validProposal)
        val reposted = row != null && item != null && MappingConfirmationNotifier.post(context, row, item, problem)
        if (!reposted) MappingConfirmationNotifier.showFailure(context, eventId, row?.id ?: rowId, problem)
    }
}
