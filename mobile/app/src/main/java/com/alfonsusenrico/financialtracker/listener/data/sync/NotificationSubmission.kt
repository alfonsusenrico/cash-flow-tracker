package com.alfonsusenrico.financialtracker.listener.data.sync

import android.content.Context
import android.util.Log
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.model.RawNotificationEntity
import com.alfonsusenrico.financialtracker.listener.data.network.ApiClient
import com.alfonsusenrico.financialtracker.listener.data.network.model.BatchIngestRequest
import com.alfonsusenrico.financialtracker.listener.data.network.model.BatchIngestResponse
import com.alfonsusenrico.financialtracker.listener.data.network.model.NotificationEventRequest
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.time.Instant
import java.time.format.DateTimeFormatter

/** Submits pending captured send rows; shared by the scheduled worker and direct sync. */
class NotificationSubmission(private val context: Context) {

    enum class Outcome { NOTHING, SUBMITTED, RETRY, STOPPED }

    companion object {
        private const val TAG = "NotificationSubmission"
        // Ignored rows (promotions, amount-less confirmations) stay briefly for diagnosis and resending.
        private const val IGNORED_RETENTION_MILLIS = 3L * 24 * 60 * 60 * 1000
        // Rows kept on the phone (and stored duplicates) stay a week so the owner can still send them.
        private const val LOCAL_ONLY_RETENTION_MILLIS = 7L * 24 * 60 * 60 * 1000
        // One submission at a time per process, whether started by WorkManager or after a capture.
        private val submissionMutex = Mutex()
    }

    private val dao = AppDatabase.getDatabase(context).notificationDao()
    private val prefs = AppPreferences(context)
    private val results = NotificationResults(context)
    private val gson = Gson()

    suspend fun submitPending(): Outcome = submissionMutex.withLock {
        dao.pruneLocalOnlyBefore(System.currentTimeMillis() - LOCAL_ONLY_RETENTION_MILLIS)
        if (!prefs.isConfigured) return@withLock Outcome.NOTHING
        val pairingContext = prefs.pairingContext
        val pending = dao.getUnsyncedNotifications(pairingContext)
        if (pending.isEmpty()) {
            results.deliverPendingAlerts()
            scheduleDueCompletion(pairingContext)
            return@withLock Outcome.NOTHING
        }

        // Built on first use inside the request's error handling, so a bad server URL defers rows.
        val service by lazy { ApiClient(context).createService() }
        val chunks = submitIsolating(pending) { rows ->
            try {
                val response = service.ingestNotifications(BatchIngestRequest(rows.map(::toRequest)))
                val body = response.body()
                if (response.isSuccessful && body != null) BatchReply.Accepted(body) else BatchReply.Rejected(response.code())
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (_: Exception) {
                BatchReply.Unreachable
            }
        }

        val outcomes = chunks.map { applyReply(it, pairingContext) }
        val accepted = chunks.any { it.reply is BatchReply.Accepted }
        if (accepted) {
            prefs.lastSyncTime = System.currentTimeMillis()
            results.deliverPendingAlerts()
            scheduleDueCompletion(pairingContext)
            dao.pruneResolvedKeepLatest()
            dao.pruneIgnoredBefore(System.currentTimeMillis() - IGNORED_RETENTION_MILLIS)
        }
        val outcome = when {
            Outcome.RETRY in outcomes -> Outcome.RETRY
            Outcome.SUBMITTED in outcomes -> Outcome.SUBMITTED
            else -> Outcome.STOPPED
        }
        if (accepted && outcome != Outcome.RETRY && dao.countUnsubmitted(pairingContext) > 0) {
            SyncScheduler.enqueueSubmissionFollowUp(context)
        }
        Log.i(TAG, "Submission finished: requests=${chunks.size} rows=${pending.size} outcome=${outcome.name.lowercase()}")
        outcome
    }

    private suspend fun applyReply(chunk: BatchChunk<RawNotificationEntity>, pairingContext: String): Outcome =
        when (val reply = chunk.reply) {
            is BatchReply.Accepted -> applyAccepted(chunk.rows, reply.body, pairingContext)
            is BatchReply.Rejected -> {
                val code = when (reply.httpStatus) {
                    401, 403 -> "auth_rejected"
                    400, 404, 422 -> "invalid_request"
                    else -> "server_unavailable"
                }
                chunk.rows.forEach { dao.saveSubmissionError(it.id, code, pairingContext) }
                Log.w(TAG, "Rows deferred: status=${reply.httpStatus} rows=${chunk.rows.size}")
                if (code == "server_unavailable") Outcome.RETRY else Outcome.STOPPED
            }
            BatchReply.Unreachable -> {
                chunk.rows.forEach { dao.saveSubmissionError(it.id, "network_unavailable", pairingContext) }
                Log.w(TAG, "Rows deferred: network_unavailable rows=${chunk.rows.size}")
                Outcome.RETRY
            }
        }

    private suspend fun applyAccepted(
        rows: List<RawNotificationEntity>, body: BatchIngestResponse, pairingContext: String
    ): Outcome {
        val matched = rows.associateBy { it.payloadHash }
        val consumed = mutableSetOf<Long>()
        var invalidResults = false
        for (item in body.results.orEmpty()) {
            val row = matched[item.payloadHash] ?: continue
            if (!consumed.add(row.id)) continue
            if (results.consume(row, item, pairingContext)) {
                val saved = dao.getById(row.id)
                if (saved != null && saved.processingStatus in setOf("queued", "processing") && saved.backendEventId != null) {
                    SyncScheduler.enqueueCompletion(
                        context,
                        saved.backendEventId,
                        (saved.nextCheckAt ?: 0L) - System.currentTimeMillis()
                    )
                }
            } else {
                invalidResults = true
            }
        }
        for (row in rows) {
            if (row.id !in consumed) {
                dao.saveSubmissionError(row.id, "missing_result", pairingContext)
                invalidResults = true
            }
        }
        return if (invalidResults) Outcome.RETRY else Outcome.SUBMITTED
    }

    private suspend fun scheduleDueCompletion(pairingContext: String) {
        for (row in dao.getDueCompletion(pairingContext, System.currentTimeMillis())) {
            val eventId = row.backendEventId ?: continue
            SyncScheduler.enqueueCompletion(context, eventId, 0)
        }
    }

    private fun toRequest(entity: RawNotificationEntity): NotificationEventRequest {
        val effectiveClass = if (entity.eventClass == "transfer" && entity.expectedCounterparty.isNullOrBlank()) {
            "income"
        } else {
            entity.eventClass
        }
        val extrasType = object : TypeToken<Map<String, Any>>() {}.type
        val rawExtras: Map<String, Any>? = try {
            entity.rawExtrasJson?.let { gson.fromJson(it, extrasType) }
        } catch (_: RuntimeException) {
            null
        }
        return NotificationEventRequest(
            deviceId = entity.deviceId,
            packageName = entity.packageName,
            appLabel = entity.appLabel,
            notificationKey = entity.notificationKey,
            notificationId = entity.notificationId,
            channelId = entity.channelId,
            category = entity.category,
            title = entity.title,
            bodyText = entity.bodyText,
            bigText = entity.bigText,
            subText = entity.subText,
            summaryText = entity.summaryText,
            postTime = DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochMilli(entity.postTime)),
            payloadHash = entity.payloadHash,
            rawExtras = rawExtras,
            sourceVersion = entity.sourceVersion,
            isFinancial = entity.isFinancial,
            eventClass = effectiveClass,
            expectedAmount = entity.expectedAmount,
            expectedDirection = entity.expectedDirection,
            expectedCounterparty = entity.expectedCounterparty,
            labelNotes = entity.labelNotes
        )
    }
}
