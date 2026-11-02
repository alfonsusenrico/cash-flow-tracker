package com.alfonsusenrico.financialtracker.listener.data.network.model

import com.google.gson.annotations.SerializedName
import java.time.DateTimeException
import java.time.OffsetDateTime
import java.time.ZoneId

data class NotificationEventRequest(
    @SerializedName("device_id")
    val deviceId: String,

    @SerializedName("package_name")
    val packageName: String,

    @SerializedName("app_label")
    val appLabel: String?,

    @SerializedName("notification_key")
    val notificationKey: String?,

    @SerializedName("notification_id")
    val notificationId: Int?,

    @SerializedName("channel_id")
    val channelId: String?,

    @SerializedName("category")
    val category: String?,

    @SerializedName("title")
    val title: String?,

    @SerializedName("body_text")
    val bodyText: String?,

    @SerializedName("big_text")
    val bigText: String?,

    @SerializedName("sub_text")
    val subText: String?,

    @SerializedName("summary_text")
    val summaryText: String?,

    @SerializedName("post_time")
    val postTime: String, // ISO 8601 UTC

    @SerializedName("payload_hash")
    val payloadHash: String,

    @SerializedName("raw_extras")
    val rawExtras: Map<String, Any>?,

    @SerializedName("source_version")
    val sourceVersion: String?,

    @SerializedName("is_financial")
    val isFinancial: Boolean?,

    @SerializedName("event_class")
    val eventClass: String?,

    @SerializedName("expected_amount")
    val expectedAmount: Double?,

    @SerializedName("expected_direction")
    val expectedDirection: String?,

    @SerializedName("expected_counterparty")
    val expectedCounterparty: String?,

    @SerializedName("label_notes")
    val labelNotes: String?
)

data class BatchIngestRequest(
    @SerializedName("events")
    val events: List<NotificationEventRequest>
)

data class IngestResultItem(
    @SerializedName("payload_hash")
    val payloadHash: String? = null,

    @SerializedName("event_id")
    val eventId: String? = null,

    @SerializedName("status")
    val status: String? = null,

    @SerializedName("error_code")
    val errorCode: String? = null,

    @SerializedName("record_key")
    val recordKey: String? = null,

    @SerializedName("transaction_id")
    val transactionId: String? = null,

    @SerializedName("type")
    val type: String? = null, // "expense", "income", "internal_movement", "investment"

    @SerializedName("description")
    val description: String? = null,

    @SerializedName("source")
    val source: String? = null,

    @SerializedName("target")
    val target: String? = null,

    @SerializedName("category")
    val category: String? = null,

    @SerializedName("amount")
    val amount: Long? = null,

    @SerializedName("currency")
    val currency: String? = null,

    @SerializedName("mapping_proposal")
    val mappingProposal: MappingProposal? = null,

    @SerializedName("mapped")
    val mapped: List<MappedName>? = null,

    @SerializedName("confirmation")
    val confirmation: ConfirmationQuestion? = null,

    @SerializedName("review_status")
    val reviewStatus: String? = null,

    @SerializedName("question")
    val question: PurposeQuestion? = null,

    @SerializedName("habit")
    val habit: RecordedHabit? = null
)

/**
 * Sent by trackers from 1.8.0 when an expense or income was recorded from the owner's habits
 * (`classified_by = habit`). `label` is "Dicatat dari kebiasaan: <category>", `basis` is
 * "<n> dari <m> jawaban sebelumnya". Older trackers omit it.
 */
data class RecordedHabit(
    @SerializedName("label") val label: String? = null,
    @SerializedName("basis") val basis: String? = null
) {
    /** The habit as shown, or null when either text is missing or implausibly long. */
    fun shown(): RecordedHabit? {
        val label = label?.trim()?.takeIf { it.isNotEmpty() && it.length <= MAX_TEXT } ?: return null
        val basis = basis?.trim()?.takeIf { it.isNotEmpty() && it.length <= MAX_TEXT } ?: return null
        return RecordedHabit(label, basis)
    }

    private companion object {
        const val MAX_TEXT = 200
    }
}

/** The backend's "Apa ini?" question on a recorded expense or income (backend design D13). */
data class PurposeQuestion(
    @SerializedName("type") val type: String? = null,
    @SerializedName("question_id") val questionId: String? = null,
    @SerializedName("transaction_id") val transactionId: String? = null,
    @SerializedName("direction") val direction: String? = null,
    @SerializedName("amount") val amount: Long? = null,
    @SerializedName("currency") val currency: String? = null,
    @SerializedName("prompt") val prompt: String? = null,
    @SerializedName("allows_text") val allowsText: Boolean? = null,
    @SerializedName("text_max_length") val textMaxLength: Int? = null,
    @SerializedName("suggestions") val suggestions: List<PurposeSuggestion>? = null
)

data class PurposeSuggestion(
    @SerializedName("id") val id: String? = null,
    @SerializedName("kind") val kind: String? = null,
    @SerializedName("label") val label: String? = null,
    @SerializedName("category_name") val categoryName: String? = null
)

data class PurposeAnswerRequest(
    @SerializedName("question_id") val questionId: String,
    @SerializedName("reply_id") val replyId: String,
    @SerializedName("suggestion_id") val suggestionId: String? = null,
    @SerializedName("text") val text: String? = null
)

data class ConfirmationQuestion(
    @SerializedName("type") val type: String? = null,
    @SerializedName("question_id") val questionId: String? = null,
    @SerializedName("masked_sender") val maskedSender: String? = null,
    @SerializedName("institution") val institution: String? = null,
    @SerializedName("receiving_account") val receivingAccount: AccountChoice? = null,
    @SerializedName("amount") val amount: Long? = null,
    @SerializedName("currency") val currency: String? = null,
    @SerializedName("jago_account") val jagoAccount: AccountChoice? = null,
    @SerializedName("destination_label") val destinationLabel: String? = null
)

data class SourcePocketConfirmRequest(
    @SerializedName("question_id") val questionId: String,
    @SerializedName("reply_id") val replyId: String,
    @SerializedName("account_id") val accountId: String
)

data class SourcePocketOptions(
    @SerializedName("event_id") val eventId: String? = null,
    @SerializedName("question_id") val questionId: String? = null,
    @SerializedName("options") val options: List<AccountChoice>? = null
)

data class SourcePocketOptionsResponse(
    @SerializedName("ok") val ok: Boolean,
    @SerializedName("result") val result: SourcePocketOptions?
)

data class SenderConfirmRequest(
    @SerializedName("question_id") val questionId: String,
    @SerializedName("reply_id") val replyId: String,
    @SerializedName("action") val action: String,
    @SerializedName("name") val name: String? = null
)

data class AccountChoice(
    @SerializedName("id") val id: String? = null,
    @SerializedName("label") val label: String? = null
)

data class MappingProposal(
    @SerializedName("name") val name: String? = null,
    @SerializedName("role") val role: String? = null,
    @SerializedName("confidence") val confidence: Double? = null,
    @SerializedName("proposed") val proposed: AccountChoice? = null,
    @SerializedName("alternatives") val alternatives: List<AccountChoice>? = null
)

data class MappedName(
    @SerializedName("name") val name: String? = null,
    @SerializedName("account") val account: String? = null,
    @SerializedName("mode") val mode: String? = null,
    @SerializedName("confidence") val confidence: Double? = null
)

data class MappingConfirmRequest(
    @SerializedName("account_id") val accountId: String,
    @SerializedName("question_id") val questionId: String? = null
)

data class NotificationResolveRequest(
    @SerializedName("type") val type: String,
    @SerializedName("account_id") val accountId: String,
    @SerializedName("category_id") val categoryId: String,
    @SerializedName("amount") val amount: Long? = null,
    @SerializedName("notes") val notes: String? = null
)

data class NotificationResultResponse(
    @SerializedName("ok") val ok: Boolean,
    @SerializedName("result") val result: IngestResultItem?
)

data class BatchIngestResponse(
    @SerializedName("ok")
    val ok: Boolean,

    @SerializedName("received")
    val received: Int? = null,

    @SerializedName("inserted")
    val inserted: Int? = null,

    @SerializedName("updated")
    val updated: Int? = null,

    @SerializedName("created_transactions")
    val createdTransactions: Int? = null,

    @SerializedName("results")
    val results: List<IngestResultItem>? = null
)

data class HealthResponse(
    @SerializedName("ok")
    val ok: Boolean,

    @SerializedName("status")
    val status: String?
)

data class AccountItemResponse(
    @SerializedName("id") val id: String,
    @SerializedName("name") val name: String,
    @SerializedName("type") val type: String? = null,
    @SerializedName("parent_id") val parentId: String? = null,
    @SerializedName("default_pocket_id") val defaultPocketId: String? = null,
    @SerializedName("balance") val balance: Long = 0,
    @SerializedName("is_archived") val isArchived: Boolean = false
)

data class AccountsEnvelope(
    @SerializedName("ok") val ok: Boolean,
    @SerializedName("accounts") val accounts: List<AccountItemResponse> = emptyList(),
    @SerializedName("total_balance") val totalBalance: Long? = null
)

data class CategoryItemResponse(
    @SerializedName("id") val id: String,
    @SerializedName("name") val name: String,
    @SerializedName("kind") val kind: String, // "expense", "income"
    @SerializedName("icon") val icon: String? = null,
    @SerializedName("color") val color: String? = null
)

data class CategoriesEnvelope(
    @SerializedName("ok") val ok: Boolean,
    @SerializedName("categories") val categories: List<CategoryItemResponse> = emptyList()
)

data class TransactionUpdateRequest(
    @SerializedName("type") val type: String? = null,
    @SerializedName("amount") val amount: Long? = null,
    @SerializedName("account_id") val accountId: String? = null,
    @SerializedName("category_id") val categoryId: String? = null,
    @SerializedName("notes") val notes: String? = null,
    @SerializedName("date") val date: String? = null
)

data class TransactionUpdateResponse(
    @SerializedName("ok") val ok: Boolean,
    @SerializedName("message") val message: String? = null
)

/**
 * The tracker's own ledger cash flow for an observed period, never a sum of local notification rows.
 * `outflow` is `grossOutflow` less the reimbursements the server applied to it.
 */
data class LedgerCashFlowSummary(
    @SerializedName("ok") val ok: Boolean = false,
    @SerializedName("period") val period: String? = null,
    @SerializedName("timezone") val timezone: String? = null,
    @SerializedName("start") val start: String? = null,
    @SerializedName("end") val end: String? = null,
    @SerializedName("generated_at") val generatedAt: String? = null,
    @SerializedName("currency") val currency: String? = null,
    @SerializedName("inflow") val inflow: Long? = null,
    @SerializedName("outflow") val outflow: Long? = null,
    @SerializedName("net") val net: Long? = null,
    @SerializedName("gross_outflow") val grossOutflow: Long? = null,
    @SerializedName("reimbursement_offset") val reimbursementOffset: Long? = null,
    /** Null when the tracker does not send it (before the field existed); empty when nothing was spent. */
    @SerializedName("top_categories") val topCategories: List<TopCategory?>? = null
) {
    /**
     * The tracker's top categories for this summary's period, or null when they are absent or break the
     * contract: at most three, each with a non-negative outflow, sorted by outflow descending. Every
     * entry with an id needs a name; at most one entry, the spending without a category, has a null id,
     * and it is shown under the tracker's name or "Tanpa kategori", without a pillar.
     */
    fun shownTopCategories(): List<TopCategory>? {
        val entries = topCategories ?: return null
        val categories = entries.filterNotNull()
        if (categories.size != entries.size || categories.size > MAX_TOP_CATEGORIES) return null
        if (categories.count { it.isUncategorised } > 1) return null
        if (categories.any { (!it.isUncategorised && it.name.isNullOrBlank()) || it.outflow == null || it.outflow < 0 }) return null
        if (categories.any { it.kakeiboType != null && it.kakeiboType !in KAKEIBO_TYPES }) return null
        if (categories.zipWithNext().any { (a, b) -> a.outflow!! < b.outflow!! }) return null
        return categories.map { if (it.isUncategorised) it.asUncategorised() else it }
    }

    /** True only for a today summary whose figures and timestamps are present and consistent. */
    fun isComplete(): Boolean {
        if (!ok || period != "today" || currency != "IDR") return false
        if (inflow == null || outflow == null || net == null || grossOutflow == null || reimbursementOffset == null) return false
        if (inflow < 0 || outflow < 0 || reimbursementOffset < 0) return false
        if (net != inflow - outflow || outflow != grossOutflow - reimbursementOffset) return false
        if (timezone == null || start == null || end == null || generatedAt == null) return false
        return try {
            ZoneId.of(timezone)
            OffsetDateTime.parse(generatedAt)
            !OffsetDateTime.parse(end).isBefore(OffsetDateTime.parse(start))
        } catch (_: DateTimeException) {
            false
        }
    }

    private companion object {
        const val MAX_TOP_CATEGORIES = 3
        val KAKEIBO_TYPES = setOf("need", "want", "culture", "unexpected", "saving")
    }
}

/**
 * One of the tracker's top spending categories, with the same period and rules as the summary's `outflow`.
 * A null `categoryId` is the tracker's spending without a category.
 */
data class TopCategory(
    @SerializedName("category_id") val categoryId: String? = null,
    @SerializedName("name") val name: String? = null,
    @SerializedName("kakeibo_type") val kakeiboType: String? = null,
    @SerializedName("outflow") val outflow: Long? = null
) {
    val isUncategorised: Boolean get() = categoryId == null

    /** Spending without a category belongs to no pillar, whatever the tracker labels it internally. */
    internal fun asUncategorised(): TopCategory =
        copy(name = name?.takeIf { it.isNotBlank() } ?: UNCATEGORISED_NAME, kakeiboType = null)

    private companion object {
        const val UNCATEGORISED_NAME = "Tanpa kategori"
    }
}
