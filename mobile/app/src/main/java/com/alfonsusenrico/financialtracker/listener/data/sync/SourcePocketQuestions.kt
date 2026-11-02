package com.alfonsusenrico.financialtracker.listener.data.sync

import com.alfonsusenrico.financialtracker.listener.data.network.model.IngestResultItem
import com.alfonsusenrico.financialtracker.listener.data.network.model.SourcePocketOptions
import com.google.gson.Gson

object SourcePocketQuestions {
    fun valid(item: IngestResultItem): Boolean {
        val question = item.confirmation ?: return false
        return item.status == "needs_confirmation" && validUuid(item.eventId) &&
            question.type == "source_pocket" && validUuid(question.questionId) && question.institution == "jago" &&
            validUuid(question.jagoAccount?.id) && question.jagoAccount?.label?.let { it.isNotBlank() && it.length <= 150 } == true &&
            question.amount?.let { it > 0 } == true && question.currency == "IDR" &&
            (question.destinationLabel == null || question.destinationLabel.length <= 150)
    }

    fun parse(json: String?): IngestResultItem? = try {
        Gson().fromJson(json, IngestResultItem::class.java)?.takeIf(::valid)
    } catch (_: RuntimeException) {
        null
    }

    fun validOptions(options: SourcePocketOptions, item: IngestResultItem): Boolean {
        val choices = options.options ?: return false
        return valid(item) && options.eventId == item.eventId &&
            options.questionId == item.confirmation?.questionId && choices.isNotEmpty() &&
            choices.all { validUuid(it.id) && it.label?.let { label -> label.isNotBlank() && label.length <= 300 } == true } &&
            choices.map { it.id }.distinct().size == choices.size
    }

    fun cached(json: String?, item: IngestResultItem): SourcePocketOptions? = try {
        Gson().fromJson(json, SourcePocketOptions::class.java)?.takeIf { validOptions(it, item) }
    } catch (_: RuntimeException) {
        null
    }
}
