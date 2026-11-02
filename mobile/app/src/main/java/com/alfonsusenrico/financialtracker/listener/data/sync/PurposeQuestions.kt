package com.alfonsusenrico.financialtracker.listener.data.sync

import com.alfonsusenrico.financialtracker.listener.data.network.model.IngestResultItem
import com.alfonsusenrico.financialtracker.listener.data.network.model.PurposeQuestion
import com.google.gson.Gson
import java.text.Normalizer
import java.util.Locale

/** The "Apa ini?" question the backend attaches to a recorded expense or income (backend design D13). */
object PurposeQuestions {
    /** The backend's own limit; a question may ask for less but never more. */
    const val TEXT_MAX_LENGTH = 200
    private const val PROMPT_MAX_LENGTH = 500
    private const val LABEL_MAX_LENGTH = 160
    private const val SUGGESTION_LIMIT = 5
    private val SUGGESTION_ID = Regex("[A-Za-z0-9_-]{1,16}")

    sealed interface Inspection {
        data object Absent : Inspection
        data class Valid(val question: PurposeQuestion) : Inspection
        /** [reason] is a code for logs and the row; it never carries question text. */
        data class Malformed(val reason: String) : Inspection
    }

    fun inspect(item: IngestResultItem): Inspection {
        val question = item.question ?: return Inspection.Absent
        // A reviewed transaction is already classified, so its question is closed whatever the result carries.
        if (item.reviewStatus == "reviewed") return Inspection.Absent
        if (question.type != "purpose") return Inspection.Malformed("purpose_question_unsupported")
        if (item.status != "recorded" || !completeRecordedResult(item) || item.type !in setOf("expense", "income") ||
            !validUuid(item.eventId) || !validUuid(item.transactionId) || question.transactionId != item.transactionId ||
            question.direction != item.type || question.amount != item.amount || question.currency != "IDR"
        ) return Inspection.Malformed("purpose_question_mismatch")
        val suggestions = question.suggestions
        val textLimit = question.textMaxLength
        val valid = validUuid(question.questionId) &&
            question.prompt?.let { it.isNotBlank() && it.length <= PROMPT_MAX_LENGTH } == true &&
            question.allowsText != null &&
            (question.allowsText == false || textLimit != null && textLimit in 1..TEXT_MAX_LENGTH) &&
            suggestions != null && suggestions.size <= SUGGESTION_LIMIT &&
            suggestions.all { suggestion ->
                suggestion.id?.matches(SUGGESTION_ID) == true &&
                    suggestion.label?.let { it.isNotBlank() && it.length <= LABEL_MAX_LENGTH } == true
            } &&
            suggestions.map { it.id }.distinct().size == suggestions.size &&
            (suggestions.isNotEmpty() || question.allowsText == true)
        return if (valid) Inspection.Valid(question) else Inspection.Malformed("purpose_question_invalid")
    }

    fun valid(item: IngestResultItem): PurposeQuestion? = (inspect(item) as? Inspection.Valid)?.question

    /** The stored result of a row and its open question, or null when the row has none. */
    fun parse(json: String?): Pair<IngestResultItem, PurposeQuestion>? = try {
        Gson().fromJson(json, IngestResultItem::class.java)?.let { item -> valid(item)?.let { item to it } }
    } catch (_: RuntimeException) {
        null
    }

    /** The backend's rule for text answers: NFKC, trimmed, 1–limit characters, no control characters. */
    fun normalizeText(value: String, limit: Int = TEXT_MAX_LENGTH): String? {
        val text = Normalizer.normalize(value, Normalizer.Form.NFKC).trim()
        if (text.codePoints().anyMatch { Character.getType(it) in OTHER_CATEGORIES }) return null
        return text.takeIf { it.codePointCount(0, it.length) in 1..limit.coerceIn(1, TEXT_MAX_LENGTH) }
    }

    /** Unicode "Other" categories (Cc, Cf, Cs, Co, Cn), as the backend rejects them. */
    private val OTHER_CATEGORIES = setOf(
        Character.CONTROL.toInt(), Character.FORMAT.toInt(), Character.SURROGATE.toInt(),
        Character.PRIVATE_USE.toInt(), Character.UNASSIGNED.toInt()
    )

    /** "Rp187.500", the backend's notation in prompts. */
    fun rupiah(amount: Long): String = "Rp" + String.format(Locale.ROOT, "%,d", amount).replace(',', '.')

    /** The record's own account: the source of an expense, the target of an income. */
    fun accountLabel(item: IngestResultItem): String? = if (item.type == "income") item.target else item.source
}
