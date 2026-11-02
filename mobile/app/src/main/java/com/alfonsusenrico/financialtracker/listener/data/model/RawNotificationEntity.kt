package com.alfonsusenrico.financialtracker.listener.data.model

import androidx.room.ColumnInfo
import androidx.room.Entity
import androidx.room.Index
import androidx.room.PrimaryKey

@Entity(
    tableName = "raw_notifications",
    indices = [
        Index(value = ["payload_hash"], unique = true),
        Index(value = ["post_time"]),
        Index(value = ["package_name"]),
        Index(value = ["is_synced"]),
        Index(value = ["notification_key"]),
        Index(value = ["normalized_fingerprint"]),
        Index(value = ["capture_outcome"])
    ]
)
data class RawNotificationEntity(
    @PrimaryKey(autoGenerate = true)
    val id: Long = 0,

    @ColumnInfo(name = "device_id")
    val deviceId: String,

    @ColumnInfo(name = "package_name")
    val packageName: String,

    @ColumnInfo(name = "app_label")
    val appLabel: String,

    @ColumnInfo(name = "notification_key")
    val notificationKey: String? = null,

    @ColumnInfo(name = "notification_id")
    val notificationId: Int? = null,

    @ColumnInfo(name = "channel_id")
    val channelId: String? = null,

    @ColumnInfo(name = "category")
    val category: String? = null,

    @ColumnInfo(name = "title")
    val title: String? = null,

    @ColumnInfo(name = "body_text")
    val bodyText: String? = null,

    @ColumnInfo(name = "big_text")
    val bigText: String? = null,

    @ColumnInfo(name = "sub_text")
    val subText: String? = null,

    @ColumnInfo(name = "summary_text")
    val summaryText: String? = null,

    @ColumnInfo(name = "post_time")
    val postTime: Long,

    @ColumnInfo(name = "captured_at")
    val capturedAt: Long = System.currentTimeMillis(),

    @ColumnInfo(name = "payload_hash")
    val payloadHash: String,

    @ColumnInfo(name = "raw_extras_json")
    val rawExtrasJson: String? = null,

    @ColumnInfo(name = "source_version")
    val sourceVersion: String? = "1.0.0",

    @ColumnInfo(name = "is_synced")
    val isSynced: Boolean = false,

    @ColumnInfo(name = "synced_at")
    val syncedAt: Long? = null,

    @ColumnInfo(name = "sync_error")
    val syncError: String? = null,

    /**
     * Written only by builds before database version 3. Kept so the Room schema still matches
     * stored rows; edits use [editableTransactionId] and alerts use [recordKey].
     */
    @ColumnInfo(name = "server_transaction_id")
    val serverTransactionId: String? = null,

    @ColumnInfo(name = "backend_event_id")
    val backendEventId: String? = null,

    @ColumnInfo(name = "processing_status")
    val processingStatus: String? = null,

    @ColumnInfo(name = "processing_error")
    val processingError: String? = null,

    @ColumnInfo(name = "record_key")
    val recordKey: String? = null,

    @ColumnInfo(name = "editable_transaction_id")
    val editableTransactionId: String? = null,

    @ColumnInfo(name = "pairing_context")
    val pairingContext: String? = null,

    @ColumnInfo(name = "next_check_at")
    val nextCheckAt: Long? = null,

    @ColumnInfo(name = "check_attempt", defaultValue = "0")
    val checkAttempt: Int = 0,

    @ColumnInfo(name = "result_json")
    val resultJson: String? = null,

    @ColumnInfo(name = "source_pocket_options_json")
    val sourcePocketOptionsJson: String? = null,

    /** "send", "keep_local" or "drop"; only send rows are ever submitted. */
    @ColumnInfo(name = "capture_outcome", defaultValue = "send")
    val captureOutcome: String = "send",

    @ColumnInfo(name = "capture_reason")
    val captureReason: String? = null,

    /** The app-set Notification.when; with the key and content it identifies one posting. */
    @ColumnInfo(name = "notification_when")
    val notificationWhen: Long? = null,

    @ColumnInfo(name = "content_fingerprint")
    val contentFingerprint: String? = null,

    @ColumnInfo(name = "normalized_fingerprint")
    val normalizedFingerprint: String? = null,

    @ColumnInfo(name = "last_posted_at")
    val lastPostedAt: Long? = null,

    @ColumnInfo(name = "repost_count", defaultValue = "0")
    val repostCount: Int = 0,

    @ColumnInfo(name = "duplicate_of_id")
    val duplicateOfId: Long? = null,

    /** The open "Apa ini?" question of a recorded result; null once it is answered or closed. */
    @ColumnInfo(name = "purpose_question_id")
    val purposeQuestionId: String? = null,

    /** Reason code when the result carried a question this build could not use. */
    @ColumnInfo(name = "purpose_question_error")
    val purposeQuestionError: String? = null,

    // Ground-truth classification & dataset labeling fields
    @ColumnInfo(name = "is_financial")
    val isFinancial: Boolean? = null,

    @ColumnInfo(name = "event_class")
    val eventClass: String? = null, // "expense", "income", "transfer", "promo", "otp", "other"

    @ColumnInfo(name = "expected_amount")
    val expectedAmount: Double? = null,

    @ColumnInfo(name = "expected_direction")
    val expectedDirection: String? = null, // "in", "out"

    @ColumnInfo(name = "expected_counterparty")
    val expectedCounterparty: String? = null,

    @ColumnInfo(name = "label_notes")
    val labelNotes: String? = null,

    @ColumnInfo(name = "labelled_at")
    val labelledAt: Long? = null
)
