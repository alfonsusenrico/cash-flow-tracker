package com.alfonsusenrico.financialtracker.listener.data.sync

import android.content.Context
import android.util.Log
import androidx.room.withTransaction
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.model.AlertReceiptEntity
import com.alfonsusenrico.financialtracker.listener.data.model.RawNotificationEntity
import com.alfonsusenrico.financialtracker.listener.data.network.model.IngestResultItem
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.service.MappingConfirmationNotifier
import com.alfonsusenrico.financialtracker.listener.service.PurposeQuestionNotifier
import com.alfonsusenrico.financialtracker.listener.service.TransactionNotifier
import com.alfonsusenrico.financialtracker.listener.service.SenderConfirmationNotifier
import com.alfonsusenrico.financialtracker.listener.service.SourcePocketConfirmationNotifier
import com.google.gson.Gson
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.UUID
import kotlin.math.min

internal fun validUuid(value: String?): Boolean = try {
    value != null && UUID.fromString(value).toString() == value.lowercase()
} catch (_: IllegalArgumentException) {
    false
}

internal fun completeRecordedResult(item: IngestResultItem): Boolean {
    if (item.status != "recorded" || item.recordKey.isNullOrBlank() ||
        item.description.isNullOrBlank() || item.amount == null || item.amount <= 0 ||
        item.currency != "IDR"
    ) return false
    return when (item.type) {
        "expense" -> !item.source.isNullOrBlank()
        "income" -> !item.target.isNullOrBlank()
        "internal_movement", "investment" -> !item.source.isNullOrBlank() && !item.target.isNullOrBlank()
        else -> false
    }
}

internal fun nextCheckDelay(attempt: Int): Long = min(900_000L, 10_000L * (1L shl min(attempt, 6)))

class NotificationResults(
    private val context: Context,
    private val db: AppDatabase = AppDatabase.getDatabase(context),
    private val postConfirmation: (RawNotificationEntity, IngestResultItem) -> Boolean = { row, item ->
        if (SourcePocketQuestions.valid(item)) {
            MappingConfirmationNotifier.cancel(context, item.eventId!!)
            SenderConfirmationNotifier.cancel(context, item.eventId)
            SourcePocketConfirmationNotifier.post(context, row, item)
        } else if (SenderQuestions.valid(item)) {
            SourcePocketConfirmationNotifier.cancel(context, item.eventId!!)
            MappingConfirmationNotifier.cancel(context, item.eventId!!)
            SenderConfirmationNotifier.post(context, row, item)
        } else {
            SourcePocketConfirmationNotifier.cancel(context, item.eventId!!)
            SenderConfirmationNotifier.cancel(context, item.eventId!!)
            MappingConfirmationNotifier.post(context, row, item)
        }
    },
    private val clearConfirmation: (String) -> Unit = { eventId ->
        MappingConfirmationNotifier.cancel(context, eventId)
        SenderConfirmationNotifier.cancel(context, eventId)
        SourcePocketConfirmationNotifier.cancel(context, eventId)
    },
    private val postPurpose: (RawNotificationEntity, IngestResultItem, Boolean) -> Boolean = { row, item, activeOnly ->
        PurposeQuestionNotifier.post(context, row, item, activeOnly)
    },
    /** Schedules the next check of an open question; the completion worker schedules its own. */
    private val scheduleFollowUp: (eventId: String, delayMillis: Long) -> Unit = { eventId, delay ->
        SyncScheduler.enqueueQuestionFollowUp(context, eventId, delay)
    },
    private val now: () -> Long = System::currentTimeMillis,
    private val postAlert: (IngestResultItem, Boolean) -> Boolean = { item, activeOnly ->
        TransactionNotifier.notifyRecorded(context, item, activeOnly)
    }
) {
    private val dao = db.notificationDao()
    private val gson = Gson()
    private val prefs = AppPreferences(context)

    companion object {
        private val alertMutex = Mutex()
        private const val TAG = "NotificationResults"

        fun confirmationKey(eventId: String, questionId: String? = null) = "confirm:$eventId" + (questionId?.let { ":$it" } ?: "")
    }

    /** [answering] names the purpose reply whose acceptance returned [item]; that reply is settled by its sender. */
    suspend fun consume(
        row: RawNotificationEntity, item: IngestResultItem, pairingContext: String, answering: String? = null
    ): Boolean {
        if (item.payloadHash != row.payloadHash ||
            row.backendEventId != null && row.backendEventId != item.eventId ||
            row.isSynced && row.pairingContext != null && row.pairingContext != pairingContext
        ) {
            dao.saveSubmissionError(row.id, "result_mismatch", pairingContext)
            return false
        }
        val status = item.status ?: run {
            dao.saveSubmissionError(row.id, "invalid_result", pairingContext)
            return false
        }
        if (status !in setOf("queued", "processing", "recorded", "needs_review", "needs_confirmation", "failed", "ignored")) {
            dao.saveSubmissionError(row.id, "invalid_result", pairingContext)
            return false
        }
        val accepted = status == "ignored" || validUuid(item.eventId)
        if (!accepted) {
            dao.saveSubmissionError(row.id, "missing_event_id", pairingContext)
            return false
        }
        val awaitingConfirmation = status == "needs_confirmation"
        if (awaitingConfirmation && !MappingConfirmationNotifier.validProposal(item) && !SenderQuestions.valid(item) && !SourcePocketQuestions.valid(item)) {
            if (row.isSynced) {
                dao.updateCheckFailure(row.id, pairingContext, "needs_review", "invalid_result", null, 0)
            } else {
                dao.saveSubmissionError(row.id, "invalid_result", pairingContext)
            }
            return false
        }
        val now = now()
        val validRecord = status == "recorded" && completeRecordedResult(item)
        val errorCode = when {
            status == "recorded" && !validRecord -> "invalid_recorded_result"
            status in setOf("failed", "needs_review", "needs_confirmation") ->
                item.errorCode?.takeIf { it.matches(Regex("[a-z0-9_]{1,64}")) }
            else -> null
        }
        val ordinaryId = item.transactionId?.takeIf {
            validRecord && item.type in setOf("expense", "income") && validUuid(it)
        }
        val purpose = if (validRecord) PurposeQuestions.inspect(item) else PurposeQuestions.Inspection.Absent
        val purposeError = (purpose as? PurposeQuestions.Inspection.Malformed)?.reason
        // Reason code only: question text and amounts are personal data.
        if (purposeError != null) Log.w(TAG, "Purpose question ignored: reason=$purposeError")
        val openQuestion = (purpose as? PurposeQuestions.Inspection.Valid)?.question?.questionId
        val stored = if (openQuestion == null && item.question != null) item.copy(question = null) else item
        val questionKey = when {
            openQuestion != null -> purposeKey(openQuestion)
            awaitingConfirmation -> confirmationKey(item.eventId!!, item.confirmation?.questionId)
            else -> null
        }
        val nextAttempt = if (status in setOf("queued", "processing")) row.checkAttempt + 1 else 0
        var nextCheck = if (nextAttempt > 0) now + nextCheckDelay(nextAttempt - 1) else null
        var checkAttempt = nextAttempt
        val persisted = db.withTransaction {
            val current = dao.getById(row.id) ?: return@withTransaction false
            if (current.isSynced && current.pairingContext != null && current.pairingContext != pairingContext) return@withTransaction false
            val currentKey = questionKey(current)
            if (questionKey != null) {
                // The same question still open moves one step along the schedule; a new one starts over.
                checkAttempt = if (currentKey == questionKey) current.checkAttempt + 1 else 0
                nextCheck = QuestionRefresh.nextCheckAt(current.capturedAt, checkAttempt, now)
            }
            if (current.processingStatus == "needs_confirmation" && currentKey != questionKey && current.backendEventId != null) {
                // Pockets cached for a source-pocket question that is no longer asked.
                dao.saveSourcePocketOptions(row.id, pairingContext, current.backendEventId, null)
            }
            dao.saveCompletion(
                id = row.id,
                accepted = true,
                syncedAt = current.syncedAt ?: now,
                eventId = item.eventId,
                status = status,
                errorCode = errorCode,
                recordKey = item.recordKey,
                transactionId = ordinaryId,
                pairingContext = pairingContext,
                nextCheckAt = nextCheck,
                checkAttempt = checkAttempt,
                resultJson = if (validRecord || awaitingConfirmation) gson.toJson(stored) else null,
                purposeQuestionId = openQuestion,
                purposeQuestionError = purposeError
            )
            if (validRecord) {
                dao.insertAlertReceipt(AlertReceiptEntity(pairingContext, item.recordKey!!))
                val closed = db.senderReplyDao().closeUnsentPurpose(row.id, pairingContext, item.eventId!!, openQuestion, answering)
                if (closed > 0) Log.i(TAG, "Unsent purpose answers closed: reason=question_closed count=$closed")
            }
            if (awaitingConfirmation) {
                dao.insertAlertReceipt(AlertReceiptEntity(pairingContext, confirmationKey(item.eventId!!, item.confirmation?.questionId)))
            }
            true
        }
        if (!persisted) return false
        if (questionKey != null) nextCheck?.let { scheduleFollowUp(item.eventId!!, it - now) }
        if (awaitingConfirmation) {
            val key = confirmationKey(item.eventId!!, item.confirmation?.questionId)
            alertMutex.withLock {
                if (dao.getAlertReceipt(pairingContext, key)?.deliveredAt == null && postConfirmation(row, item)) {
                    dao.markAlertDelivered(pairingContext, key)
                }
            }
        } else if (item.eventId != null) {
            clearConfirmation(item.eventId)
        }
        if (validRecord && dao.getAlertReceipt(pairingContext, item.recordKey!!)?.deliveredAt != null) {
            alertMutex.withLock { alert(row, stored, true, pairingContext) }
        }
        return true
    }

    /**
     * A record with an open purpose question alerts as the question. Once an answer exists, the
     * notification shows that answer's state, so a refresh of the same question leaves it alone.
     */
    private suspend fun alert(row: RawNotificationEntity, item: IngestResultItem, activeOnly: Boolean, pairingContext: String): Boolean {
        val question = PurposeQuestions.valid(item) ?: return postAlert(item, activeOnly)
        val reply = db.senderReplyDao().forQuestion(pairingContext, item.eventId!!, question.questionId!!)
        return when {
            reply == null -> postPurpose(row, item, activeOnly)
            activeOnly -> true
            reply.state == "failed" -> postPurpose(row, item, false)
            else -> postAlert(item, false)
        }
    }

    suspend fun defer(row: RawNotificationEntity, errorCode: String, terminal: Boolean) {
        val pairingContext = row.pairingContext ?: return
        val attempt = row.checkAttempt + 1
        dao.updateCheckFailure(
            id = row.id,
            pairingContext = pairingContext,
            status = if (terminal) "failed" else row.processingStatus ?: "queued",
            errorCode = errorCode,
            nextCheckAt = if (terminal) null else System.currentTimeMillis() + nextCheckDelay(attempt),
            attempt = attempt
        )
    }

    /** The open question a stored row holds, as the key [consume] compares; null when none is open. */
    private fun questionKey(row: RawNotificationEntity): String? = when {
        !QuestionRefresh.open(row) -> null
        row.purposeQuestionId != null -> purposeKey(row.purposeQuestionId)
        else -> try {
            gson.fromJson(row.resultJson, IngestResultItem::class.java)
        } catch (_: RuntimeException) {
            null
        }?.let { stored -> stored.eventId?.let { confirmationKey(it, stored.confirmation?.questionId) } }
    }

    private fun purposeKey(questionId: String) = "purpose:$questionId"

    /**
     * A failed check of an open question leaves the row's status, error and result as they are: a
     * transient failure moves the check one step along the schedule, any other failure stops the checks.
     */
    suspend fun deferFollowUp(row: RawNotificationEntity, errorCode: String, terminal: Boolean) {
        val pairingContext = row.pairingContext ?: return
        val status = row.processingStatus ?: return
        val step = row.checkAttempt + 1
        val next = if (terminal) null else QuestionRefresh.nextCheckAt(row.capturedAt, step, now())
        dao.updateQuestionCheck(row.id, pairingContext, status, row.resultJson, next, step)
        if (terminal) Log.w(TAG, "Question checks stopped: reason=$errorCode")
    }

    suspend fun deliverPendingAlerts(pairingContext: String = prefs.pairingContext) = alertMutex.withLock {
        for (row in dao.getPendingAlerts(pairingContext)) {
            val recordKey = row.recordKey ?: continue
            if (dao.getAlertReceipt(pairingContext, recordKey)?.deliveredAt != null) continue
            val item = try {
                gson.fromJson(row.resultJson, IngestResultItem::class.java)
            } catch (_: RuntimeException) {
                null
            }
            if (item != null && completeRecordedResult(item) &&
                alert(row, item, false, pairingContext)
            ) {
                dao.markAlertDelivered(pairingContext, recordKey)
            }
        }
    }
}
