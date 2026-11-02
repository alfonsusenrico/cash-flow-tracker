package com.alfonsusenrico.financialtracker.listener.data.sync

import android.content.Context
import androidx.room.withTransaction
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.model.RawNotificationEntity
import com.alfonsusenrico.financialtracker.listener.data.model.SenderReplyEntity
import com.alfonsusenrico.financialtracker.listener.data.network.ApiClient
import com.alfonsusenrico.financialtracker.listener.data.network.IngestionApiService
import com.alfonsusenrico.financialtracker.listener.data.network.model.IngestResultItem
import com.alfonsusenrico.financialtracker.listener.data.network.model.PurposeAnswerRequest
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.service.PurposeQuestionNotifier
import com.google.gson.JsonParser
import kotlinx.coroutines.CancellationException
import retrofit2.Response
import java.util.UUID

/**
 * The durable outbox for "Apa ini?" answers. Each answer is stored with its reply ID before any network
 * call and shares `sender_replies` (answer kind `purpose`) with the other owner answers.
 *
 * States: queued, waiting_auth and waiting_pairing are delivered later; accepted is final; closed means
 * the server no longer asks this question (a 409, or a refreshed result without it, `question_closed`);
 * failed means the server rejected the answer (404/422), and the owner may answer again with a new reply ID.
 */
class PurposeAnswers(
    private val context: Context,
    private val db: AppDatabase = AppDatabase.getDatabase(context),
    private val schedule: (replyId: String, delayMillis: Long, continuation: Boolean) -> Unit = { id, delay, continuation ->
        SyncScheduler.enqueueSenderReply(context, id, delay, continuation)
    },
    private val refresh: (eventId: String) -> Unit = { event -> SyncScheduler.enqueueCompletion(context, event, 0, refresh = true) },
    private val showState: (RawNotificationEntity, String) -> Unit = { row, message ->
        PurposeQuestionNotifier.showState(context, row, message)
    },
    private val repost: (RawNotificationEntity, IngestResultItem, String) -> Boolean = { row, item, problem ->
        PurposeQuestionNotifier.post(context, row, item, activeOnly = false, problem = problem)
    },
    private val results: NotificationResults = NotificationResults(context, db),
    private val service: (pairing: String) -> IngestionApiService = { pairing -> ApiClient(context).createService(pairing) },
    private val now: () -> Long = System::currentTimeMillis
) {
    private val replies = db.senderReplyDao()
    private val rows = db.notificationDao()

    private sealed interface Prepared {
        data class Saved(val row: RawNotificationEntity, val reply: SenderReplyEntity, val answerText: String?) : Prepared
        data class Refused(val message: String, val code: String = AnswerAttempts.QUESTION_CHANGED) : Prepared
    }

    suspend fun enqueue(
        rowId: Long, questionId: String, suggestionId: String?, text: String?,
        expectedEvent: String? = null, expectedPairing: String? = null
    ): SenderReplies.Outcome {
        val prefs = AppPreferences(context)
        val pairing = prefs.pairingContext
        if (!prefs.isConfigured || expectedPairing != null && expectedPairing != pairing) {
            return SenderReplies.Outcome(false, "Hubungkan kembali akun yang menerima notifikasi ini.", AnswerAttempts.NOT_PAIRED)
        }
        if ((suggestionId == null) == (text == null)) {
            return SenderReplies.Outcome(false, "Pilih saran atau tulis jawaban.", AnswerAttempts.INVALID_ANSWER)
        }
        val prepared = db.withTransaction {
            val row = rows.getById(rowId)
            val (item, question) = PurposeQuestions.parse(row?.resultJson) ?: return@withTransaction Prepared.Refused(STALE)
            if (row == null || row.processingStatus != "recorded" || row.pairingContext != pairing ||
                row.backendEventId != item.eventId || expectedEvent != null && expectedEvent != item.eventId ||
                row.purposeQuestionId != questionId || question.questionId != questionId
            ) return@withTransaction Prepared.Refused(STALE)
            val answerText = if (text != null) {
                if (question.allowsText != true) {
                    return@withTransaction Prepared.Refused("Pertanyaan ini hanya menerima saran.", AnswerAttempts.INVALID_ANSWER)
                }
                val limit = question.textMaxLength ?: PurposeQuestions.TEXT_MAX_LENGTH
                PurposeQuestions.normalizeText(text, limit)
                    ?: return@withTransaction Prepared.Refused(textRule(limit), AnswerAttempts.INVALID_ANSWER)
            } else {
                null
            }
            if (suggestionId != null && question.suggestions.orEmpty().none { it.id == suggestionId }) {
                return@withTransaction Prepared.Refused(STALE)
            }
            val previous = replies.forQuestion(pairing, item.eventId!!, questionId)
            if (previous != null && previous.state != "failed") return@withTransaction Prepared.Saved(row, previous, answerText)
            if (previous != null) replies.removeFailed(previous.replyId)
            replies.insert(SenderReplyEntity(
                replyId = UUID.randomUUID().toString(), rowId = rowId, pairingContext = pairing, eventId = item.eventId,
                questionId = questionId, action = if (suggestionId != null) "suggestion" else "text", name = null,
                answerKind = KIND, suggestionId = suggestionId, answerText = answerText
            ))
            Prepared.Saved(row, replies.forQuestion(pairing, item.eventId, questionId)!!, answerText)
        }
        val (row, reply, answerText) = when (prepared) {
            is Prepared.Refused -> return SenderReplies.Outcome(false, prepared.message, prepared.code)
            is Prepared.Saved -> prepared
        }
        if (reply.state !in setOf("queued", "waiting_auth")) {
            return SenderReplies.Outcome(false, purposeReplyMessage(reply.state, reply.errorCode), AnswerAttempts.ALREADY_ANSWERED)
        }
        if (reply.answerKind != KIND || reply.suggestionId != suggestionId || reply.answerText != answerText) {
            return SenderReplies.Outcome(false, "Jawaban sebelumnya tersimpan; tunggu hasil server.", AnswerAttempts.ALREADY_ANSWERED)
        }
        showState(row, purposeReplyMessage("queued", null))
        schedule(reply.replyId, 0, false)
        return SenderReplies.Outcome(true, null, AnswerAttempts.QUEUED)
    }

    /**
     * After an answer from the notification was not stored, shows the row's open question again with its actions
     * and [problem], so it stays answerable there. When the question already has a stored answer other than a failed
     * one, or the row asks none, only [problem] is shown in the row's slot.
     */
    suspend fun offerAgain(rowId: Long, problem: String) {
        val row = rows.getById(rowId) ?: return
        val open = PurposeQuestions.parse(row.resultJson)?.takeIf { (item, question) ->
            val pairing = row.pairingContext
            row.processingStatus == "recorded" && pairing != null && row.backendEventId == item.eventId &&
                question.questionId == row.purposeQuestionId &&
                replies.forQuestion(pairing, item.eventId!!, question.questionId!!).let { it == null || it.state == "failed" }
        }
        if (open == null || !repost(row, open.first, problem)) showState(row, problem)
    }

    suspend fun deliver(replyId: String) {
        val reply = replies.get(replyId) ?: return
        if (reply.answerKind != KIND || reply.state !in setOf("queued", "waiting_auth")) return
        val row = rows.getById(reply.rowId) ?: return
        val prefs = AppPreferences(context)
        suspend fun settle(state: String, error: String?) {
            replies.defer(reply.replyId, state, reply.attempt, 0, error)
            showState(row, purposeReplyMessage(state, error))
        }
        suspend fun retry(error: String) {
            val delay = senderRetryDelay(reply.attempt)
            replies.defer(reply.replyId, "queued", reply.attempt + 1, now() + delay, error)
            schedule(reply.replyId, delay, true)
        }
        if (prefs.pairingContext != reply.pairingContext) return settle("waiting_pairing", "pairing_changed")
        if (!prefs.isConfigured) return settle("waiting_auth", "auth_rejected")
        if (row.backendEventId != reply.eventId || row.pairingContext != reply.pairingContext) return settle("failed", "result_mismatch")
        if ((reply.suggestionId == null) == (reply.answerText == null)) return settle("failed", "invalid_reply")
        try {
            // A replay reuses the stored reply ID, so the server applies the answer at most once.
            val response = service(reply.pairingContext).answerPurpose(
                reply.eventId, PurposeAnswerRequest(reply.questionId, reply.replyId, reply.suggestionId, reply.answerText)
            )
            if (prefs.pairingContext != reply.pairingContext) return settle("waiting_pairing", "pairing_changed")
            val code = response.code()
            when {
                code == 401 || code == 403 -> settle("waiting_auth", "auth_rejected")
                code == 409 -> {
                    settle("closed", errorCode(response) ?: "stale_question")
                    refresh(reply.eventId)
                }
                code == 404 -> settle("failed", errorCode(response) ?: "event_not_found")
                code == 400 || code == 422 -> settle("failed", errorCode(response) ?: "invalid_purpose_answer")
                code == 429 || code >= 500 -> retry("server_unavailable")
                !response.isSuccessful -> settle("failed", "unexpected_response")
                else -> {
                    val body = response.body()
                    val item = body?.result
                    // Persist the result first; a crash before accepting only causes an idempotent replay.
                    if (body?.ok != true || item?.eventId != reply.eventId || item.payloadHash != row.payloadHash ||
                        !results.consume(row, item, reply.pairingContext, answering = reply.replyId)
                    ) {
                        retry("invalid_result")
                    } else {
                        replies.acceptPurpose(reply.replyId)
                        results.deliverPendingAlerts(reply.pairingContext)
                    }
                }
            }
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Exception) {
            if (prefs.pairingContext != reply.pairingContext) settle("waiting_pairing", "pairing_changed") else retry("network_unavailable")
        }
    }

    companion object {
        const val KIND = "purpose"
        private const val STALE = "Pertanyaan sudah berubah. Muat ulang hasil notifikasi."
        private val ERROR_CODE = Regex("[a-z0-9_]{1,64}")

        fun textRule(limit: Int) = "Tulis 1–$limit karakter tanpa karakter khusus."

        /** The backend's `detail.code`, for example `stale_question` or `unknown_suggestion`. */
        internal fun errorCode(response: Response<*>): String? = try {
            JsonParser.parseString(response.errorBody()?.string().orEmpty()).asJsonObject
                .getAsJsonObject("detail")?.get("code")?.takeIf { it.isJsonPrimitive }?.asString
                ?.takeIf { it.matches(ERROR_CODE) }
        } catch (_: RuntimeException) {
            null
        }
    }
}

fun purposeReplyMessage(state: String, errorCode: String?): String = when (state) {
    "queued" -> "Jawaban tersimpan; mengirim ke server."
    "waiting_auth", "waiting_pairing" -> "Jawaban belum terkirim. Periksa koneksi akun di Pengaturan."
    "closed" -> when (errorCode) {
        // Closed by a refreshed result, not a 409: an earlier attempt may have reached the server.
        "question_closed" -> "Pertanyaan sudah selesai di server; jawaban ini tidak dikirim lagi."
        else -> "Pertanyaan sudah ditutup di server; jawaban ini tidak dipakai."
    }
    "failed" -> when (errorCode) {
        "unknown_suggestion", "invalid_suggestion" -> "Saran ini tidak berlaku lagi. Pilih saran lain atau tulis jawaban."
        "invalid_purpose_answer" -> "Jawaban tidak diterima. Tulis 1–200 karakter tanpa karakter khusus."
        "event_not_found" -> "Transaksi tidak ditemukan di server."
        else -> "Jawaban ditolak server. Pilih saran lain atau tulis jawaban."
    }
    else -> "Jawaban diterima."
}
