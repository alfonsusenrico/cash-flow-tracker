package com.alfonsusenrico.financialtracker.listener.data.sync

import com.alfonsusenrico.financialtracker.listener.data.network.model.IngestResultItem
import com.google.gson.Gson
import java.text.Normalizer

object SenderQuestions {
    fun valid(item: IngestResultItem): Boolean {
        val question = item.confirmation ?: return false
        return item.status == "needs_confirmation" && validUuid(item.eventId) &&
            question.type == "sender" && validUuid(question.questionId) && question.institution == "bca" &&
            question.maskedSender?.let { it.isNotBlank() && it.length <= 150 && '*' in it } == true &&
            validUuid(question.receivingAccount?.id) && !question.receivingAccount?.label.isNullOrBlank() &&
            question.amount?.let { it > 0 } == true && question.currency == "IDR"
    }

    fun parse(json: String?): IngestResultItem? = try {
        Gson().fromJson(json, IngestResultItem::class.java)?.takeIf(::valid)
    } catch (_: RuntimeException) {
        null
    }

    fun normalizeName(value: String): String? {
        if (value.codePoints().anyMatch { Character.getType(it) in setOf(
                Character.CONTROL.toInt(), Character.FORMAT.toInt(), Character.SURROGATE.toInt(),
                Character.PRIVATE_USE.toInt(), Character.UNASSIGNED.toInt()
            ) }) return null
        val name = Normalizer.normalize(value, Normalizer.Form.NFKC).trim()
        return name.takeIf { it.codePointCount(0, it.length) in 1..80 }
    }
}
