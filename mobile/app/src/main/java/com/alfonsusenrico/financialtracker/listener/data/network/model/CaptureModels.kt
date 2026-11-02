package com.alfonsusenrico.financialtracker.listener.data.network.model

import com.google.gson.annotations.SerializedName

/**
 * `POST /api/capture/text`; the backend bounds the text to 1–200 characters after normalisation.
 * [questionTypes] lists the questions this phone can answer; without `time` the tracker never asks it.
 */
data class CaptureTextRequest(
    @SerializedName("client_capture_id") val clientCaptureId: String,
    @SerializedName("text") val text: String,
    @SerializedName("device_time") val deviceTime: String,
    @SerializedName("question_types") val questionTypes: List<String>? = null
)

/** The follow-up question types this companion answers. */
object CaptureQuestionTypes {
    val ANSWERED = listOf("amount", "kind", "account", "category", "time")
}

/** `POST /api/capture/{id}/answer`: exactly the field the open question's type takes, plus its ids. */
data class CaptureAnswerRequest(
    @SerializedName("question_id") val questionId: String,
    @SerializedName("reply_id") val replyId: String,
    @SerializedName("option_id") val optionId: String? = null,
    @SerializedName("text") val text: String? = null,
    @SerializedName("account_id") val accountId: String? = null,
    @SerializedName("category_id") val categoryId: String? = null,
    @SerializedName("kakeibo") val kakeibo: String? = null
)

data class CaptureEnvelope(
    @SerializedName("ok") val ok: Boolean = false,
    @SerializedName("capture") val capture: CaptureView? = null
)

/** The backend's `capture_view`. */
data class CaptureView(
    @SerializedName("id") val id: String? = null,
    @SerializedName("client_capture_id") val clientCaptureId: String? = null,
    @SerializedName("status") val status: String? = null,
    @SerializedName("text") val text: String? = null,
    @SerializedName("question") val question: CaptureQuestion? = null,
    @SerializedName("summary") val summary: CaptureSummary? = null,
    @SerializedName("transaction") val transaction: CaptureTransaction? = null,
    @SerializedName("movement") val movement: CaptureMovement? = null,
    @SerializedName("recorded_at") val recordedAt: String? = null,
    @SerializedName("undo_until") val undoUntil: String? = null,
    @SerializedName("undone_at") val undoneAt: String? = null
)

/** One open question: `amount`, `time`, `kind`, `account` (with `role`) or `category`. */
data class CaptureQuestion(
    @SerializedName("question_id") val questionId: String? = null,
    @SerializedName("type") val type: String? = null,
    @SerializedName("role") val role: String? = null,
    @SerializedName("reason") val reason: String? = null,
    @SerializedName("prompt") val prompt: String? = null,
    @SerializedName("options") val options: List<CaptureOption>? = null,
    @SerializedName("allows_text") val allowsText: Boolean? = null,
    @SerializedName("allows_other") val allowsOther: Boolean? = null
)

/** A kind, time, account or category choice; category options carry their category id and pillar, time options their moment. */
data class CaptureOption(
    @SerializedName("id") val id: String? = null,
    @SerializedName("label") val label: String? = null,
    @SerializedName("date") val date: String? = null,
    @SerializedName("type") val type: String? = null,
    @SerializedName("category_id") val categoryId: String? = null,
    @SerializedName("kakeibo") val kakeibo: String? = null
)

/** What the tracker has worked out so far; a field it does not know yet is null. */
data class CaptureSummary(
    @SerializedName("type") val type: String? = null,
    @SerializedName("amount") val amount: Long? = null,
    @SerializedName("date") val date: String? = null,
    /** "Hari ini 14.00": the moment as the tracker read it, null while the time is still asked. */
    @SerializedName("time_label") val timeLabel: String? = null,
    @SerializedName("description") val description: String? = null
)

data class CaptureTransaction(
    @SerializedName("id") val id: String? = null,
    @SerializedName("type") val type: String? = null,
    @SerializedName("amount") val amount: Long? = null,
    @SerializedName("date") val date: String? = null,
    @SerializedName("account_id") val accountId: String? = null,
    @SerializedName("account_name") val accountName: String? = null,
    @SerializedName("merchant") val merchant: String? = null,
    @SerializedName("notes") val notes: String? = null,
    @SerializedName("category_id") val categoryId: String? = null,
    @SerializedName("category_name") val categoryName: String? = null,
    @SerializedName("kakeibo") val kakeibo: String? = null,
    @SerializedName("source") val source: String? = null,
    @SerializedName("classified_by") val classifiedBy: String? = null
)

data class CaptureMovement(
    @SerializedName("id") val id: String? = null,
    @SerializedName("source") val source: CaptureTransaction? = null,
    @SerializedName("target") val target: CaptureTransaction? = null
)

data class CaptureSuggestionsResponse(
    @SerializedName("ok") val ok: Boolean = false,
    @SerializedName("suggestions") val suggestions: List<CaptureSuggestion>? = null
)

/** A prefill from the owner's own history; `reason` is the backend's short Indonesian explanation. */
data class CaptureSuggestion(
    @SerializedName("text") val text: String? = null,
    @SerializedName("label") val label: String? = null,
    @SerializedName("amount") val amount: Long? = null,
    @SerializedName("category_name") val categoryName: String? = null,
    @SerializedName("account_name") val accountName: String? = null,
    @SerializedName("reason") val reason: String? = null
)
