package com.alfonsusenrico.financialtracker.listener.data.textcapture

import android.content.Context
import androidx.room.withTransaction
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.model.TextCaptureEntity
import com.alfonsusenrico.financialtracker.listener.data.model.TextCaptureState
import com.alfonsusenrico.financialtracker.listener.data.network.ApiClient
import com.alfonsusenrico.financialtracker.listener.data.network.IngestionApiService
import com.alfonsusenrico.financialtracker.listener.data.network.model.CaptureAnswerRequest
import com.alfonsusenrico.financialtracker.listener.data.network.model.CaptureEnvelope
import com.alfonsusenrico.financialtracker.listener.data.network.model.CaptureQuestion
import com.alfonsusenrico.financialtracker.listener.data.network.model.CaptureQuestionTypes
import com.alfonsusenrico.financialtracker.listener.data.network.model.CaptureSuggestion
import com.alfonsusenrico.financialtracker.listener.data.network.model.CaptureTextRequest
import com.alfonsusenrico.financialtracker.listener.data.network.model.CaptureView
import com.alfonsusenrico.financialtracker.listener.data.network.model.TransactionUpdateRequest
import com.alfonsusenrico.financialtracker.listener.data.network.serverErrorCode
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.data.sync.SyncScheduler
import com.alfonsusenrico.financialtracker.listener.data.sync.validUuid
import com.google.gson.Gson
import com.google.gson.JsonParseException
import kotlinx.coroutines.CancellationException
import retrofit2.Response
import java.io.IOException
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.UUID

/** The owner's answer to the open question: one offered option, a typed amount, or an owner-picked id. */
data class CaptureAnswer(
    val questionId: String,
    val optionId: String? = null,
    val text: String? = null,
    val accountId: String? = null,
    val categoryId: String? = null
)

/** An answer exactly as it is sent, with the reply id that makes a resend idempotent. */
data class PendingAnswer(val answer: CaptureAnswer, val replyId: String)

/** Outcomes of the owner's actions on a capture; the UI turns each into a message. */
enum class CaptureActionResult {
    DONE, OFFLINE, SERVER_ERROR, AUTH, OTHER_PAIRING, INVALID, STALE, TOO_LATE, CHANGED, NOT_FOUND, NOT_ALLOWED, CONFLICT
}

/**
 * Typed captures: written to Room before any request, then sent, answered, undone, edited or deleted
 * against the tracker. Every send carries the capture's own client id and every answer its stored reply
 * id, so repeating a request after a timeout or a killed process never records twice.
 */
class TextCaptures(
    private val context: Context,
    private val db: AppDatabase = AppDatabase.getDatabase(context),
    private val clock: () -> Long = System::currentTimeMillis,
    private val zone: ZoneId = ZoneId.systemDefault(),
    private val scheduleResend: (Context) -> Unit = SyncScheduler::enqueueTextCaptures,
    private val newId: () -> String = { UUID.randomUUID().toString() }
) {
    private val dao = db.textCaptureDao()
    private val prefs = AppPreferences(context)
    private val gson = Gson()

    companion object {
        /** Longer than the client's connect plus read timeouts, so a live send is never taken over. */
        const val STALE_CLAIM_MILLIS = 90_000L
        const val NETWORK = "network"
        const val SERVER = "server"
        const val AUTH = "auth"
        const val NOT_PAIRED = "not_paired"
        private val TRANSIENT = setOf(NETWORK, SERVER)

        fun question(capture: TextCaptureEntity): CaptureQuestion? = parse(capture.questionJson, CaptureQuestion::class.java)
        fun view(capture: TextCaptureEntity): CaptureView? = parse(capture.summaryJson, CaptureView::class.java)
        fun pendingAnswer(capture: TextCaptureEntity): PendingAnswer? = parse(capture.pendingAnswerJson, PendingAnswer::class.java)

        private fun <T> parse(json: String?, type: Class<T>): T? = try {
            json?.let { Gson().fromJson(it, type) }
        } catch (_: JsonParseException) {
            null
        }
    }

    /** Stores a typed capture; returns its row id, or null when the text breaks the backend's rules. */
    suspend fun create(rawText: String): Long? {
        val text = CaptureText.normalize(rawText) ?: return null
        val now = clock()
        return dao.insert(TextCaptureEntity(clientCaptureId = newId(), text = text, deviceTime = deviceTime(now), createdAt = now))
    }

    /**
     * One send attempt. A transient failure leaves the capture queued and schedules a resend when the
     * owner asked for this send or auto-sync is on; otherwise it waits for "Kirim".
     */
    suspend fun send(id: Long, ownerAsked: Boolean): TextCaptureEntity? =
        attempt(id) { if (ownerAsked || prefs.isAutoSyncEnabled) scheduleResend(context) }

    private suspend fun attempt(id: Long, onTransient: () -> Unit): TextCaptureEntity? {
        if (!prefs.isConfigured) {
            dao.noteQueued(id, NOT_PAIRED)
            return dao.getById(id)
        }
        val now = clock()
        if (dao.claim(id, now, now - STALE_CLAIM_MILLIS) != 1) return dao.getById(id)
        val capture = dao.getById(id) ?: return null
        val pairing = prefs.pairingContext
        val response = try {
            ApiClient(context).createService(pairing).captureText(CaptureTextRequest(capture.clientCaptureId, capture.text, capture.deviceTime, CaptureQuestionTypes.ANSWERED))
        } catch (cancelled: CancellationException) {
            dao.release(id, NETWORK)
            throw cancelled
        } catch (_: IOException) {
            return queuedAgain(id, NETWORK, onTransient)
        } catch (_: RuntimeException) {
            return queuedAgain(id, SERVER, onTransient)
        }
        val view = response.body()?.capture
        when {
            response.isSuccessful && response.body()?.ok == true && view != null && view.isValidFor(capture.clientCaptureId, null) ->
                apply(id, view, pairing, response.headers().getDate("Date")?.time)
            response.isSuccessful -> return queuedAgain(id, SERVER, onTransient)
            response.code() == 401 || response.code() == 403 -> dao.release(id, AUTH)
            response.code() == 429 || response.code() >= 500 -> return queuedAgain(id, SERVER, onTransient)
            else -> dao.fail(id, serverErrorCode(response) ?: "http_${response.code()}")
        }
        return dao.getById(id)
    }

    private suspend fun queuedAgain(id: Long, reason: String, onTransient: () -> Unit): TextCaptureEntity? {
        dao.release(id, reason)
        onTransient()
        return dao.getById(id)
    }

    /** Resends every capture the tracker has not accepted; true while some wait on a transient failure. */
    suspend fun sendQueued(): Boolean {
        if (!prefs.isConfigured) return false
        for (capture in dao.sendable(clock() - STALE_CLAIM_MILLIS)) {
            // The worker retries on its own; scheduling from inside it would replace itself.
            val sent = attempt(capture.id) {}
            if (sent?.state == TextCaptureState.QUEUED && sent.errorCode == AUTH) return false
        }
        return dao.sendable(clock() - STALE_CLAIM_MILLIS).any { it.errorCode in TRANSIENT }
    }

    /** The owner's "Kirim" or "Coba lagi": a refused capture is queued again and sent now. */
    suspend fun retry(id: Long): TextCaptureEntity? {
        dao.requeue(id)
        return send(id, ownerAsked = true)
    }

    /** At process start no send is running: sends a killed process left behind return to the queue. */
    suspend fun recover() {
        dao.releaseInterrupted()
        resumeQueued()
    }

    /** With auto-sync on, queued captures are resent once the network allows, after a restart or re-pairing. */
    suspend fun resumeQueued() {
        if (prefs.isAutoSyncEnabled && prefs.isConfigured && dao.sendable(clock() - STALE_CLAIM_MILLIS).isNotEmpty()) {
            scheduleResend(context)
        }
    }

    suspend fun discard(id: Long): Boolean = dao.discard(id) == 1

    /** Reads the capture again from the tracker, for a question that changed or an edit. */
    suspend fun refresh(id: Long): CaptureActionResult {
        val capture = dao.getById(id) ?: return CaptureActionResult.NOT_FOUND
        val serverId = capture.serverId ?: return CaptureActionResult.NOT_ALLOWED
        if (capture.pairingContext != prefs.pairingContext) return CaptureActionResult.OTHER_PAIRING
        val response = request { it.getCapture(serverId) } ?: return CaptureActionResult.OFFLINE
        return applyResponse(capture, response) ?: statusResult(response)
    }

    /**
     * Answers the open question. The answer and its reply id are stored first; the same answer is resent
     * with the same reply id until the tracker settles it.
     */
    suspend fun answer(id: Long, answer: CaptureAnswer): CaptureActionResult {
        val capture = dao.getById(id) ?: return CaptureActionResult.NOT_FOUND
        val question = question(capture)
        if (capture.state != TextCaptureState.NEEDS_DETAIL || question?.questionId != answer.questionId) return CaptureActionResult.STALE
        if (capture.pairingContext != prefs.pairingContext) return CaptureActionResult.OTHER_PAIRING
        if (!fits(question, answer)) return CaptureActionResult.INVALID
        val pending = db.withTransaction {
            val current = dao.getById(id) ?: return@withTransaction null
            val stored = pendingAnswer(current)?.takeIf { it.answer == answer }
            val chosen = stored ?: PendingAnswer(answer, newId())
            if (stored == null) dao.update(current.copy(pendingAnswerJson = gson.toJson(chosen)))
            chosen
        } ?: return CaptureActionResult.NOT_FOUND
        return sendAnswer(id, pending)
    }

    /** Sends the stored answer again, unchanged. */
    suspend fun resendAnswer(id: Long): CaptureActionResult {
        val capture = dao.getById(id) ?: return CaptureActionResult.NOT_FOUND
        val pending = pendingAnswer(capture) ?: return CaptureActionResult.STALE
        if (capture.pairingContext != prefs.pairingContext) return CaptureActionResult.OTHER_PAIRING
        return sendAnswer(id, pending)
    }

    private suspend fun sendAnswer(id: Long, pending: PendingAnswer): CaptureActionResult {
        val capture = dao.getById(id) ?: return CaptureActionResult.NOT_FOUND
        val serverId = capture.serverId ?: return CaptureActionResult.NOT_ALLOWED
        val answer = pending.answer
        val request = CaptureAnswerRequest(answer.questionId, pending.replyId, answer.optionId, answer.text, answer.accountId, answer.categoryId)
        val response = request { it.answerCapture(serverId, request) } ?: return CaptureActionResult.OFFLINE
        val code = response.code()
        if (response.isSuccessful) {
            return applyResponse(capture, response, clearAnswer = true) ?: CaptureActionResult.SERVER_ERROR
        }
        return when {
            code == 422 -> {
                clearPendingAnswer(id)
                CaptureActionResult.INVALID
            }
            code == 409 -> {
                clearPendingAnswer(id)
                refresh(id)
                CaptureActionResult.STALE
            }
            else -> statusResult(response)
        }
    }

    private suspend fun clearPendingAnswer(id: Long) {
        db.withTransaction {
            dao.getById(id)?.let { dao.update(it.copy(pendingAnswerJson = null)) }
        }
    }

    /** "Batalkan": sent only while the local window is open; the server decides with its own grace. */
    suspend fun undo(id: Long): CaptureActionResult {
        val capture = dao.getById(id) ?: return CaptureActionResult.NOT_FOUND
        if (!UndoWindow.isOpen(capture, clock())) return CaptureActionResult.TOO_LATE
        if (capture.pairingContext != prefs.pairingContext) return CaptureActionResult.OTHER_PAIRING
        val serverId = capture.serverId ?: return CaptureActionResult.NOT_ALLOWED
        val response = request { it.undoCapture(serverId) } ?: return CaptureActionResult.OFFLINE
        if (response.isSuccessful) return applyResponse(capture, response) ?: CaptureActionResult.SERVER_ERROR
        if (response.code() != 409) return statusResult(response)
        val reason = serverErrorCode(response)
        closeUndo(id)
        return if (reason == "capture_transaction_changed") CaptureActionResult.CHANGED else CaptureActionResult.TOO_LATE
    }

    private suspend fun closeUndo(id: Long) {
        db.withTransaction {
            dao.getById(id)?.let { dao.update(it.copy(undoDeadline = 0)) }
        }
    }

    /** "Ubah" through the existing transaction edit; an edit ends the undo window on the server. */
    suspend fun edit(id: Long, update: TransactionUpdateRequest): CaptureActionResult {
        val capture = dao.getById(id) ?: return CaptureActionResult.NOT_FOUND
        if (!CaptureAbilities.canEdit(capture, view(capture), prefs.pairingContext)) return CaptureActionResult.NOT_ALLOWED
        if (update.type !in setOf("expense", "income")) return CaptureActionResult.INVALID
        val response = request { it.updateTransaction(capture.transactionId!!, update) } ?: return CaptureActionResult.OFFLINE
        if (!response.isSuccessful || response.body()?.ok != true) return statusResult(response)
        db.withTransaction {
            dao.getById(id)?.let { dao.update(it.copy(edited = true)) }
        }
        refresh(id)
        return CaptureActionResult.DONE
    }

    /** "Hapus dari tracker": deletes the transaction this capture created, and only that one. */
    suspend fun deleteFromTracker(id: Long): CaptureActionResult {
        val capture = dao.getById(id) ?: return CaptureActionResult.NOT_FOUND
        if (!CaptureAbilities.canDeleteFromTracker(capture, view(capture), prefs.pairingContext)) return CaptureActionResult.NOT_ALLOWED
        val response = request { it.deleteTransaction(capture.transactionId!!) } ?: return CaptureActionResult.OFFLINE
        return when {
            response.isSuccessful -> {
                markDeleted(id, null)
                CaptureActionResult.DONE
            }
            response.code() == 404 -> {
                markDeleted(id, "not_found")
                CaptureActionResult.NOT_FOUND
            }
            else -> statusResult(response)
        }
    }

    private suspend fun markDeleted(id: Long, reason: String?) {
        db.withTransaction {
            dao.getById(id)?.let { dao.update(it.copy(state = TextCaptureState.DELETED, errorCode = reason, undoDeadline = 0)) }
        }
    }

    /** Up to six suggestions from the owner's history; null when the tracker could not be asked. */
    suspend fun suggestions(): List<CaptureSuggestion>? {
        if (!prefs.isConfigured) return null
        val response = request { it.captureSuggestions(deviceTime(clock())) } ?: return null
        val body = response.body()
        if (!response.isSuccessful || body?.ok != true) return null
        return body.suggestions.orEmpty().filter { suggestion ->
            val text = suggestion.text
            text != null && CaptureText.normalize(text) == text && !suggestion.label.isNullOrBlank()
        }.take(6)
    }

    /** The device time with its offset and whole seconds always written, as the backend parses it. */
    private fun deviceTime(millis: Long): String =
        DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(OffsetDateTime.ofInstant(Instant.ofEpochMilli(millis), zone).truncatedTo(ChronoUnit.SECONDS))

    private suspend fun <T> request(call: suspend (IngestionApiService) -> Response<T>): Response<T>? =
        try {
            call(ApiClient(context).createService(prefs.pairingContext))
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: IOException) {
            null
        } catch (_: RuntimeException) {
            null
        }

    private suspend fun applyResponse(capture: TextCaptureEntity, response: Response<CaptureEnvelope>, clearAnswer: Boolean = false): CaptureActionResult? {
        val view = response.body()?.capture
        if (!response.isSuccessful || response.body()?.ok != true || view == null ||
            !view.isValidFor(capture.clientCaptureId, capture.serverId)
        ) return null
        apply(capture.id, view, capture.pairingContext ?: prefs.pairingContext, response.headers().getDate("Date")?.time, clearAnswer)
        return CaptureActionResult.DONE
    }

    private fun statusResult(response: Response<*>): CaptureActionResult = when (response.code()) {
        401, 403 -> CaptureActionResult.AUTH
        404 -> CaptureActionResult.NOT_FOUND
        409 -> CaptureActionResult.CONFLICT
        422 -> CaptureActionResult.INVALID
        else -> CaptureActionResult.SERVER_ERROR
    }

    /** Stores the tracker's view. The first recorded view fixes the local undo deadline. */
    private suspend fun apply(id: Long, view: CaptureView, pairing: String, serverTime: Long?, clearAnswer: Boolean = false) {
        val now = clock()
        db.withTransaction {
            val current = dao.getById(id) ?: return@withTransaction
            if (current.state == TextCaptureState.DELETED) return@withTransaction
            val state = TextCaptureState.fromServer(view.status) ?: return@withTransaction
            val deadline = when {
                state != TextCaptureState.RECORDED -> current.undoDeadline
                current.undoDeadline != null -> current.undoDeadline
                else -> UndoWindow.localDeadline(view.undoUntil, serverTime, now)
            }
            val question = view.question.takeIf { state == TextCaptureState.NEEDS_DETAIL }
            dao.update(
                current.copy(
                    state = state,
                    serverId = view.id,
                    pairingContext = current.pairingContext ?: pairing,
                    questionJson = question?.let(gson::toJson),
                    summaryJson = gson.toJson(view.copy(question = null)),
                    transactionId = view.transaction?.id?.takeIf(::validUuid) ?: current.transactionId.takeIf { state == TextCaptureState.RECORDED },
                    undoUntil = view.undoUntil,
                    undoDeadline = deadline,
                    claimedAt = null,
                    errorCode = null,
                    pendingAnswerJson = if (clearAnswer || state != TextCaptureState.NEEDS_DETAIL) null else current.pendingAnswerJson
                )
            )
        }
    }

    /** The owner's answer must fit the question: an amount is text, a time a chip or text, the others an offered or owner-picked id. */
    private fun fits(question: CaptureQuestion, answer: CaptureAnswer): Boolean {
        val optionIds = question.options.orEmpty().mapNotNull { it.id }.toSet()
        val byOption = answer.optionId != null && answer.optionId in optionIds &&
            answer.text == null && answer.accountId == null && answer.categoryId == null
        return when (question.type) {
            "amount" -> answer.optionId == null && answer.accountId == null && answer.categoryId == null &&
                answer.text != null && CaptureText.normalize(answer.text, CaptureText.ANSWER_MAX_LENGTH) == answer.text
            "kind" -> byOption
            // A chip's moment, or the owner's own words ("tadi jam 2") read by the tracker.
            "time" -> byOption || answer.optionId == null && answer.accountId == null && answer.categoryId == null &&
                answer.text != null && question.allowsText == true &&
                CaptureText.normalize(answer.text, CaptureText.ANSWER_MAX_LENGTH) == answer.text
            "account" -> byOption || question.allowsOther == true && answer.optionId == null && answer.text == null &&
                answer.categoryId == null && validUuid(answer.accountId)
            "category" -> byOption || question.allowsOther == true && answer.optionId == null && answer.text == null &&
                answer.accountId == null && validUuid(answer.categoryId)
            else -> false
        }
    }
}
