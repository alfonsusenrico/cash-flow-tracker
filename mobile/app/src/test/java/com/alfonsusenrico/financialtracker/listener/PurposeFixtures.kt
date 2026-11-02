package com.alfonsusenrico.financialtracker.listener

import com.alfonsusenrico.financialtracker.listener.data.network.model.IngestResultItem
import com.google.gson.Gson

/** Fictional results shaped like the backend contract (cash-flow-tracker design D13). */
object PurposeFixtures {
    const val EVENT = "00000000-0000-4000-8000-000000000101"
    const val TRANSACTION = "00000000-0000-4000-8000-000000000102"
    const val QUESTION = "00000000-0000-4000-8000-000000000103"
    const val CATEGORY = "00000000-0000-4000-8000-000000000104"
    const val SOURCE_QUESTION = "00000000-0000-4000-8000-000000000106"
    const val JAGO_ACCOUNT = "00000000-0000-4000-8000-000000000107"
    const val HASH = "fictional-purpose-hash"

    fun questionJson(
        suggestions: Int = 2, allowsText: Boolean = true, questionId: String = QUESTION, amount: Long = 187500
    ): String {
        val items = (1..suggestions).joinToString(",") { index ->
            """{"id": "s$index", "kind": "history", "label": "makan kafe senja $index", "merchant": "Kafe Senja",
               "notes": "makan kafe senja $index", "category_id": "$CATEGORY", "category_name": "Makanan & Minuman",
               "kakeibo": "want", "offset_category_id": null, "offset_category_name": null}"""
        }
        return """{"type": "purpose", "question_id": "$questionId", "transaction_id": "$TRANSACTION",
            "direction": "expense", "amount": $amount, "currency": "IDR",
            "prompt": "Apa ini? Rp187.500 dari BCA Harian · kategori bank: Groceries",
            "allows_text": $allowsText, "text_max_length": 200, "suggestions": [$items]}"""
    }

    fun resultJson(question: String? = questionJson(), description: String = "Pengeluaran Groceries",
                   reviewStatus: String = "needs_review"): String =
        """{"payload_hash": "$HASH", "event_id": "$EVENT", "status": "recorded",
            "record_key": "notification:$EVENT", "type": "expense", "description": "$description",
            "source": "BCA Harian", "target": null, "amount": 187500, "currency": "IDR",
            "transaction_id": "$TRANSACTION", "review_status": "$reviewStatus"
            ${question?.let { ", \"question\": $it" }.orEmpty()}}"""

    /** A Jago source-pocket question awaiting the owner, as the backend sends it. */
    fun sourcePocketJson(questionId: String = SOURCE_QUESTION): String =
        """{"payload_hash": "$HASH", "event_id": "$EVENT", "status": "needs_confirmation",
            "error_code": "source_pocket_required", "confirmation": {"type": "source_pocket",
            "question_id": "$questionId", "institution": "jago", "amount": 125000, "currency": "IDR",
            "jago_account": {"id": "$JAGO_ACCOUNT", "label": "Bank Jago"}}}"""

    fun ignoredJson(): String = """{"payload_hash": "$HASH", "event_id": "$EVENT", "status": "ignored"}"""

    fun result(question: String? = questionJson()): IngestResultItem =
        Gson().fromJson(resultJson(question), IngestResultItem::class.java)
}
