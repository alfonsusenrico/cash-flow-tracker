package com.alfonsusenrico.financialtracker.listener.data.db

import androidx.room.Dao
import androidx.room.Insert
import androidx.room.OnConflictStrategy
import androidx.room.Query
import com.alfonsusenrico.financialtracker.listener.data.model.AlertReceiptEntity
import com.alfonsusenrico.financialtracker.listener.data.model.RawNotificationEntity
import kotlinx.coroutines.flow.Flow

@Dao
interface NotificationDao {
    @Query("""
        UPDATE raw_notifications SET source_pocket_options_json = :json
        WHERE id = :rowId AND pairing_context = :pairing AND backend_event_id = :eventId
          AND (:expectedResultJson IS NULL OR result_json = :expectedResultJson)
    """)
    suspend fun saveSourcePocketOptions(
        rowId: Long, pairing: String, eventId: String, json: String?, expectedResultJson: String? = null
    ): Int

    @Insert(onConflict = OnConflictStrategy.IGNORE)
    suspend fun insertOrIgnore(entity: RawNotificationEntity): Long

    /** The earlier capture of the same posting: same key, identical content and the same app-set time. */
    @Query("""
        SELECT * FROM raw_notifications
        WHERE package_name = :packageName AND notification_key = :notificationKey
          AND content_fingerprint = :contentFingerprint AND notification_when = :notificationWhen
        ORDER BY id ASC LIMIT 1
    """)
    suspend fun findPosting(
        packageName: String, notificationKey: String, contentFingerprint: String, notificationWhen: Long
    ): RawNotificationEntity?

    @Query("""
        UPDATE raw_notifications SET repost_count = repost_count + 1,
          last_posted_at = MAX(COALESCE(last_posted_at, post_time), :postedAt)
        WHERE id = :id
    """)
    suspend fun recordRepost(id: Long, postedAt: Long): Int

    /**
     * The closest unclaimed row with the same normalised text from a sibling app of the same
     * institution. A row absorbs at most one duplicate, so repeated identical payments pair one-to-one.
     */
    @Query("""
        SELECT r.id FROM raw_notifications r
        WHERE LOWER(r.package_name) IN (:siblingPackages)
          AND r.normalized_fingerprint = :normalizedFingerprint
          AND r.capture_outcome != 'drop'
          AND ABS(r.post_time - :postTime) <= :windowMillis
          AND NOT EXISTS (
              SELECT 1 FROM raw_notifications d
              WHERE d.duplicate_of_id = r.id AND d.capture_reason = 'duplicate_source'
          )
        ORDER BY ABS(r.post_time - :postTime) ASC, r.id ASC LIMIT 1
    """)
    suspend fun findSiblingOriginal(
        siblingPackages: List<String>, normalizedFingerprint: String, postTime: Long, windowMillis: Long
    ): Long?

    /** A captured child of the same app that a group summary restates. */
    @Query("""
        SELECT id FROM raw_notifications
        WHERE LOWER(package_name) = :packageName AND normalized_fingerprint = :normalizedFingerprint
          AND capture_outcome != 'drop' AND ABS(post_time - :postTime) <= :windowMillis
        ORDER BY ABS(post_time - :postTime) ASC, id ASC LIMIT 1
    """)
    suspend fun findRestatedChild(
        packageName: String, normalizedFingerprint: String, postTime: Long, windowMillis: Long
    ): Long?

    @Query("""
        SELECT * FROM raw_notifications
        WHERE capture_outcome = 'keep_local'
          AND (processing_status IS NULL OR processing_status != 'dismissed')
        ORDER BY post_time DESC
    """)
    fun getLocalOnlyFlow(): Flow<List<RawNotificationEntity>>

    /** The owner's "Kirim ke Tracker": a kept row joins the send pipeline. */
    @Query("""
        UPDATE raw_notifications SET capture_outcome = 'send', capture_reason = 'owner_sent', sync_error = NULL
        WHERE id = :id AND capture_outcome = 'keep_local' AND is_synced = 0
    """)
    suspend fun sendLocal(id: Long): Int

    @Query("DELETE FROM raw_notifications WHERE capture_outcome IN ('keep_local', 'drop') AND post_time < :beforeTimestamp")
    suspend fun pruneLocalOnlyBefore(beforeTimestamp: Long): Int

    @Query("""
        SELECT * FROM raw_notifications
        WHERE ((title IS NOT NULL AND title != '')
            OR (body_text IS NOT NULL AND body_text != '')
            OR (big_text IS NOT NULL AND big_text != ''))
          AND capture_outcome = 'send'
          AND (processing_status IS NULL OR processing_status NOT IN ('ignored', 'dismissed'))
        ORDER BY post_time DESC
    """)
    fun getAllNotificationsFlow(): Flow<List<RawNotificationEntity>>

    @Query("""
        SELECT * FROM raw_notifications
        WHERE is_synced = 0 AND capture_outcome = 'send'
          AND (processing_status IS NULL OR processing_status != 'dismissed')
          AND (sync_error IS NULL OR sync_error NOT IN ('auth_rejected', 'invalid_request')
               OR pairing_context IS NULL OR pairing_context != :pairingContext)
        ORDER BY post_time ASC
        LIMIT :limit
    """)
    suspend fun getUnsyncedNotifications(pairingContext: String, limit: Int = 50): List<RawNotificationEntity>

    @Query("SELECT * FROM raw_notifications WHERE processing_status = 'ignored' ORDER BY post_time DESC")
    fun getIgnoredNotificationsFlow(): Flow<List<RawNotificationEntity>>

    /** Clears server state of an ignored row so the next sync submits it again as a new event. */
    @Query("""
        UPDATE raw_notifications SET is_synced = 0, synced_at = NULL, sync_error = NULL,
          backend_event_id = NULL, processing_status = NULL, processing_error = NULL, record_key = NULL,
          editable_transaction_id = NULL, next_check_at = NULL, check_attempt = 0, result_json = NULL,
          purpose_question_id = NULL, purpose_question_error = NULL
        WHERE id = :id AND processing_status = 'ignored'
    """)
    suspend fun resubmitIgnored(id: Long): Int

    /**
     * Hides a row on the phone only. The row stays as a tombstone (pruned with ignored rows) so the
     * listener's drawer rescan cannot capture the same notification again; server records are untouched.
     */
    @Query("""
        UPDATE raw_notifications SET processing_status = 'dismissed', next_check_at = NULL
        WHERE id = :id AND (processing_status IS NULL OR processing_status != 'dismissed')
    """)
    suspend fun dismiss(id: Long): Int

    @Query("""
        UPDATE raw_notifications SET processing_status = :status, next_check_at = :nextCheckAt
        WHERE id = :id AND processing_status = 'dismissed'
    """)
    suspend fun restoreDismissed(id: Long, status: String?, nextCheckAt: Long?): Int

    @Query("UPDATE raw_notifications SET processing_status = 'dismissed' WHERE processing_status = 'ignored'")
    suspend fun dismissAllIgnored(): Int

    @Query("SELECT * FROM raw_notifications WHERE id = :id")
    suspend fun getById(id: Long): RawNotificationEntity?

    @Query("SELECT * FROM raw_notifications WHERE id = :id")
    fun observeById(id: Long): Flow<RawNotificationEntity?>

    /** Cross-app duplicates the store kept back, until they are pruned with other local-only rows. */
    @Query("SELECT COUNT(*) FROM raw_notifications WHERE capture_outcome = 'drop' AND capture_reason = 'duplicate_source'")
    fun getHiddenDuplicateCountFlow(): Flow<Int>

    @Query("SELECT * FROM raw_notifications WHERE backend_event_id = :eventId AND pairing_context = :pairingContext LIMIT 1")
    suspend fun getByBackendEventId(eventId: String, pairingContext: String): RawNotificationEntity?

    @Query("""
        SELECT * FROM raw_notifications
        WHERE is_synced = 1 AND pairing_context = :pairingContext
          AND backend_event_id IS NOT NULL
          AND processing_status IN ('queued', 'processing')
          AND next_check_at IS NOT NULL AND next_check_at <= :now
        ORDER BY next_check_at ASC LIMIT :limit
    """)
    suspend fun getDueCompletion(pairingContext: String, now: Long, limit: Int = 50): List<RawNotificationEntity>

    @Query("""
        SELECT * FROM raw_notifications
        WHERE is_synced = 1 AND pairing_context = :pairingContext
          AND backend_event_id IS NOT NULL AND processing_status IN ('needs_review', 'needs_confirmation', 'failed', 'recorded')
        ORDER BY post_time DESC LIMIT 50
    """)
    suspend fun getRefreshableResults(pairingContext: String): List<RawNotificationEntity>

    @Query("""
        SELECT * FROM raw_notifications
        WHERE is_synced = 1 AND pairing_context = :pairingContext AND backend_event_id IS NOT NULL
          AND processing_status IN ('queued', 'processing')
        ORDER BY post_time ASC LIMIT 50
    """)
    suspend fun getAwaitingResult(pairingContext: String): List<RawNotificationEntity>

    @Query("""
        SELECT COUNT(*) FROM raw_notifications WHERE is_synced = 0 AND capture_outcome = 'send'
          AND (processing_status IS NULL OR processing_status != 'dismissed')
          AND (sync_error IS NULL OR sync_error NOT IN ('auth_rejected', 'invalid_request')
               OR pairing_context IS NULL OR pairing_context != :pairingContext)
    """)
    suspend fun countUnsubmitted(pairingContext: String): Int

    @Query("""
        UPDATE raw_notifications SET is_synced = :accepted, synced_at = :syncedAt,
          sync_error = NULL, backend_event_id = :eventId, processing_status = :status,
          processing_error = :errorCode, record_key = :recordKey,
          editable_transaction_id = :transactionId, pairing_context = :pairingContext,
          next_check_at = :nextCheckAt, check_attempt = :checkAttempt,
          result_json = :resultJson, purpose_question_id = :purposeQuestionId,
          purpose_question_error = :purposeQuestionError
        WHERE id = :id
    """)
    suspend fun saveCompletion(
        id: Long, accepted: Boolean, syncedAt: Long?, eventId: String?, status: String,
        errorCode: String?, recordKey: String?, transactionId: String?, pairingContext: String?,
        nextCheckAt: Long?, checkAttempt: Int, resultJson: String?,
        purposeQuestionId: String? = null, purposeQuestionError: String? = null
    )

    /** Recorded rows whose purpose question still needs the owner: no answer, or only a rejected one. */
    @Query("""
        SELECT * FROM raw_notifications n
        WHERE n.purpose_question_id IS NOT NULL AND n.processing_status = 'recorded'
          AND NOT EXISTS (
              SELECT 1 FROM sender_replies r
              WHERE r.row_id = n.id AND r.answer_kind = 'purpose'
                AND r.question_id = n.purpose_question_id AND r.state != 'failed'
          )
        ORDER BY n.post_time DESC
    """)
    fun getNeedsAnswerFlow(): Flow<List<RawNotificationEntity>>

    /**
     * Review and failed rows of this pairing captured since [since], newest first, that the owner has
     * not labelled on the phone and that have no unsent reply.
     */
    @Query("""
        SELECT * FROM raw_notifications n
        WHERE n.is_synced = 1 AND n.pairing_context = :pairingContext AND n.backend_event_id IS NOT NULL
          AND n.processing_status IN ('needs_review', 'failed')
          AND n.captured_at >= :since AND n.labelled_at IS NULL
          AND NOT EXISTS (
              SELECT 1 FROM sender_replies r
              WHERE r.row_id = n.id AND r.state IN ('queued', 'waiting_auth', 'waiting_pairing')
          )
        ORDER BY n.post_time DESC LIMIT :limit
    """)
    suspend fun getRefreshableReviews(pairingContext: String, since: Long, limit: Int): List<RawNotificationEntity>

    /**
     * Rows of this pairing with an open question, answered on the phone or not: recorded rows with a
     * purpose question, and rows awaiting confirmation.
     */
    @Query("""
        SELECT * FROM raw_notifications
        WHERE is_synced = 1 AND pairing_context = :pairingContext AND backend_event_id IS NOT NULL
          AND (processing_status = 'recorded' AND purpose_question_id IS NOT NULL
               OR processing_status = 'needs_confirmation')
        ORDER BY post_time DESC LIMIT :limit
    """)
    suspend fun getOpenQuestions(pairingContext: String, limit: Int = 50): List<RawNotificationEntity>

    /**
     * Reschedules the check of an open question. Status, error and stored result stay as they are; the
     * update applies only while the row still holds the status and result it was read with.
     */
    @Query("""
        UPDATE raw_notifications SET next_check_at = :nextCheckAt, check_attempt = :attempt
        WHERE id = :id AND pairing_context = :pairingContext AND processing_status = :status
          AND result_json IS :resultJson
    """)
    suspend fun updateQuestionCheck(
        id: Long, pairingContext: String, status: String, resultJson: String?, nextCheckAt: Long?, attempt: Int
    ): Int

    @Query("UPDATE raw_notifications SET sync_error = :errorCode, pairing_context = :pairingContext WHERE id = :id AND is_synced = 0")
    suspend fun saveSubmissionError(id: Long, errorCode: String, pairingContext: String)

    @Query("""
        UPDATE raw_notifications SET processing_status = :status, processing_error = :errorCode,
          next_check_at = :nextCheckAt, check_attempt = :attempt
        WHERE id = :id AND pairing_context = :pairingContext
    """)
    suspend fun updateCheckFailure(
        id: Long, pairingContext: String, status: String,
        errorCode: String, nextCheckAt: Long?, attempt: Int
    )

    @Insert(onConflict = OnConflictStrategy.IGNORE)
    suspend fun insertAlertReceipt(receipt: AlertReceiptEntity): Long

    @Query("SELECT * FROM alert_receipts WHERE pairing_context = :pairingContext AND record_key = :recordKey")
    suspend fun getAlertReceipt(pairingContext: String, recordKey: String): AlertReceiptEntity?

    @Query("UPDATE alert_receipts SET delivered_at = :deliveredAt WHERE pairing_context = :pairingContext AND record_key = :recordKey AND delivered_at IS NULL")
    suspend fun markAlertDelivered(pairingContext: String, recordKey: String, deliveredAt: Long = System.currentTimeMillis())

    @Query("""
        SELECT n.* FROM raw_notifications n
        JOIN alert_receipts a ON a.pairing_context = n.pairing_context AND a.record_key = n.record_key
        WHERE n.pairing_context = :pairingContext AND a.delivered_at IS NULL
          AND n.processing_status = 'recorded' AND n.result_json IS NOT NULL
        ORDER BY n.post_time ASC LIMIT :limit
    """)
    suspend fun getPendingAlerts(pairingContext: String, limit: Int = 50): List<RawNotificationEntity>

    @Query("""
        SELECT COUNT(*) FROM raw_notifications
        WHERE capture_outcome = 'send'
          AND (processing_status IS NULL OR processing_status NOT IN ('ignored', 'dismissed'))
          AND ((title IS NOT NULL AND title != '')
            OR (body_text IS NOT NULL AND body_text != '')
            OR (big_text IS NOT NULL AND big_text != ''))
    """)
    fun getTotalCountFlow(): Flow<Int>

    /**
     * Stores the owner's type, amount and note from the detail page. The captured counterparty is
     * left alone: the form never edits it, and submission needs it to keep a transfer a transfer.
     */
    @Query("""
        UPDATE raw_notifications SET
            is_financial = 1,
            event_class = :eventClass,
            expected_amount = :expectedAmount,
            expected_direction = :expectedDirection,
            label_notes = :labelNotes,
            labelled_at = :labelledAt,
            sync_error = CASE WHEN is_synced = 0 THEN NULL ELSE sync_error END
        WHERE id = :id
    """)
    suspend fun saveOwnerLabel(
        id: Long,
        eventClass: String,
        expectedAmount: Double,
        expectedDirection: String?,
        labelNotes: String?,
        labelledAt: Long
    )

    @Query("DELETE FROM raw_notifications WHERE id = :id")
    suspend fun deleteById(id: Long)

    @Query("""
        DELETE FROM raw_notifications
        WHERE processing_status IN ('recorded', 'ignored', 'dismissed')
          AND id NOT IN (SELECT id FROM raw_notifications ORDER BY post_time DESC LIMIT :keepCount)
          AND purpose_question_id IS NULL
          AND NOT EXISTS (
              SELECT 1 FROM sender_replies r WHERE r.row_id = raw_notifications.id AND r.state != 'accepted'
          )
          AND (result_json IS NULL OR result_json NOT LIKE '%needs_confirmation%')
          AND NOT EXISTS (
              SELECT 1 FROM alert_receipts a
              WHERE a.pairing_context = raw_notifications.pairing_context
                AND a.record_key = raw_notifications.record_key
                AND a.delivered_at IS NULL
          )
    """)
    suspend fun pruneResolvedKeepLatest(keepCount: Int = 1000)

    @Query("""DELETE FROM raw_notifications WHERE processing_status IN ('ignored', 'dismissed') AND post_time < :beforeTimestamp
        AND (result_json IS NULL OR result_json NOT LIKE '%needs_confirmation%')
        AND NOT EXISTS (SELECT 1 FROM sender_replies r WHERE r.row_id = raw_notifications.id AND r.state != 'accepted')""")
    suspend fun pruneIgnoredBefore(beforeTimestamp: Long): Int
}
