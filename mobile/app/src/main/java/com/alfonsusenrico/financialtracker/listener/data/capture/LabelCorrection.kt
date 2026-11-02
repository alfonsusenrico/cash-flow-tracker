package com.alfonsusenrico.financialtracker.listener.data.capture

import com.alfonsusenrico.financialtracker.listener.data.sync.validUuid
import org.json.JSONObject

/** The pillar the owner chose in "Perbaiki label"; [Unclassified] is "Belum diklasifikasi", sent as null. */
sealed interface PillarChoice {
    data class Pillar(val code: String) : PillarChoice
    data object Unclassified : PillarChoice

    companion object {
        val CODES = listOf("need", "want", "culture", "unexpected", "saving")
    }
}

/**
 * "Perbaiki label": the owner's correction of a recorded notification's category, Kakeibo pillar and
 * merchant. Only what the owner changed is sent, so an untouched field keeps the tracker's value; the tracker
 * records the change as an owner correction and learns from it. Amount, account, date and type stay as
 * recorded: those are changed on the web.
 */
data class LabelChange(
    val categoryId: String? = null,
    val pillar: PillarChoice? = null,
    val merchant: String? = null
) {
    val isEmpty: Boolean get() = categoryId == null && pillar == null && merchant.isNullOrBlank()

    /** Why the change cannot be sent, or null when it can; [type] is the recorded `expense` or `income`. */
    fun problem(type: String?): String? = when {
        type !in setOf("expense", "income") -> "Label transaksi ini hanya bisa diubah di web."
        isEmpty -> "Pilih kategori, pilar, atau isi merchant dulu."
        categoryId != null && !validUuid(categoryId) -> "Kategori tidak dikenal. Muat ulang daftar kategori."
        pillar != null && type != "expense" -> "Pilar Kakeibo hanya untuk pengeluaran."
        pillar is PillarChoice.Pillar && pillar.code !in PillarChoice.CODES -> "Pilar tidak dikenal."
        merchant != null && merchant.trim().length > MERCHANT_MAX_LENGTH -> "Nama merchant maksimal $MERCHANT_MAX_LENGTH karakter."
        else -> null
    }

    /** The `PATCH /api/transactions/{id}` body: the changed fields only, with an explicit null to clear the pillar. */
    fun toJson(): String {
        val body = JSONObject()
        categoryId?.let { body.put("category_id", it) }
        when (val chosen = pillar) {
            is PillarChoice.Pillar -> body.put("kakeibo_type", chosen.code)
            PillarChoice.Unclassified -> body.put("kakeibo_type", JSONObject.NULL)
            null -> Unit
        }
        merchant?.trim()?.takeIf { it.isNotEmpty() }?.let { body.put("merchant", it) }
        return body.toString()
    }

    companion object {
        /** The web's merchant limit (`TransactionClassificationInput.merchant`) is longer; the phone keeps names short. */
        const val MERCHANT_MAX_LENGTH = 120
    }
}
