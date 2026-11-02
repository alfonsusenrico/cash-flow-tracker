package com.alfonsusenrico.financialtracker.listener.data.capture

import androidx.room.withTransaction
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.model.RawNotificationEntity
import com.alfonsusenrico.financialtracker.listener.registry.FinancialSourceRegistry
import com.alfonsusenrico.financialtracker.listener.service.CaptureOutcome
import com.alfonsusenrico.financialtracker.listener.service.CaptureReason
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.delay
import java.security.MessageDigest

/** Fingerprints that identify a posting and a cross-app duplicate. */
object CaptureFingerprint {

    /** Exact content: re-posts of one notification carry byte-identical text. */
    fun content(title: String?, bodyText: String?, bigText: String?): String =
        sha256(listOf(title.orEmpty(), bodyText.orEmpty(), bigText.orEmpty()).joinToString("\u0000"))

    /**
     * Title plus the text a reader sees, case- and whitespace-insensitive, so the same payment from
     * two apps matches even when only one of them fills the expanded text.
     */
    fun normalized(title: String?, bodyText: String?, bigText: String?): String {
        val text = if (!bigText.isNullOrBlank()) bigText else bodyText
        val normalized = listOf(title.orEmpty(), text.orEmpty())
            .joinToString("\n") { it.trim().lowercase().replace(Regex("""\s+"""), " ") }
        return sha256(normalized)
    }

    private fun sha256(value: String): String =
        MessageDigest.getInstance("SHA-256").digest(value.toByteArray(Charsets.UTF_8))
            .joinToString("") { "%02x".format(it) }
}

/** Stores classified captures once per posting and links cross-app duplicates. */
class CaptureStore(private val db: AppDatabase) {

    sealed interface Result {
        data class Inserted(val id: Long, val outcome: String) : Result
        data class Reposted(val id: Long) : Result
        /** The payload hash already exists, e.g. a drawer rescan of a row stored before version 6. */
        data object AlreadyStored : Result
    }

    companion object {
        // Both apps of an institution are notified from the same payment event, so their posts land
        // seconds apart; two minutes tolerates delayed push delivery without pairing unrelated payments.
        const val DUPLICATE_WINDOW_MILLIS = 2L * 60 * 1000
    }

    private val dao = db.notificationDao()

    suspend fun store(row: RawNotificationEntity, isGroupSummary: Boolean): Result = db.withTransaction {
        val key = row.notificationKey
        val contentFingerprint = row.contentFingerprint
        val notificationWhen = row.notificationWhen
        if (key != null && contentFingerprint != null && notificationWhen != null) {
            val posting = dao.findPosting(row.packageName, key, contentFingerprint, notificationWhen)
            if (posting != null) {
                dao.recordRepost(posting.id, row.postTime)
                return@withTransaction Result.Reposted(posting.id)
            }
        }
        val stored = linkDuplicate(row, isGroupSummary)
        val id = dao.insertOrIgnore(stored)
        if (id == -1L) Result.AlreadyStored else Result.Inserted(id, stored.captureOutcome)
    }

    private suspend fun linkDuplicate(row: RawNotificationEntity, isGroupSummary: Boolean): RawNotificationEntity {
        val fingerprint = row.normalizedFingerprint
        if (fingerprint == null || row.captureOutcome == CaptureOutcome.DROP.code) return row
        if (isGroupSummary) {
            val child = dao.findRestatedChild(row.packageName.lowercase(), fingerprint, row.postTime, DUPLICATE_WINDOW_MILLIS)
            if (child != null) return row.asDrop(CaptureReason.GROUP_SUMMARY, child)
        }
        val siblings = FinancialSourceRegistry.siblingPackages(row.packageName)
        if (siblings.isEmpty()) return row
        val original = dao.findSiblingOriginal(siblings, fingerprint, row.postTime, DUPLICATE_WINDOW_MILLIS)
            ?: return row
        return row.asDrop(CaptureReason.DUPLICATE_SOURCE, original)
    }

    private fun RawNotificationEntity.asDrop(reason: String, originalId: Long) = copy(
        captureOutcome = CaptureOutcome.DROP.code,
        captureReason = reason,
        duplicateOfId = originalId
    )
}

/**
 * Runs [block], retrying after each of [retryDelaysMillis] when it throws. Returns null after the
 * last attempt fails, having reported the failure; cancellation is never retried or swallowed.
 */
suspend fun <T> retryingWrite(
    retryDelaysMillis: List<Long>,
    onFailure: (Exception) -> Unit,
    block: suspend () -> T
): T? {
    var attempt = 0
    while (true) {
        try {
            return block()
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (error: Exception) {
            if (attempt >= retryDelaysMillis.size) {
                onFailure(error)
                return null
            }
            delay(retryDelaysMillis[attempt])
            attempt++
        }
    }
}
