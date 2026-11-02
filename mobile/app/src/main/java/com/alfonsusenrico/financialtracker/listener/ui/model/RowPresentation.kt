package com.alfonsusenrico.financialtracker.listener.ui.model

import com.alfonsusenrico.financialtracker.listener.data.model.RawNotificationEntity
import com.alfonsusenrico.financialtracker.listener.data.network.model.IngestResultItem
import com.alfonsusenrico.financialtracker.listener.data.network.model.RecordedHabit
import com.alfonsusenrico.financialtracker.listener.data.sync.PurposeQuestions
import com.alfonsusenrico.financialtracker.listener.data.sync.SenderQuestions
import com.alfonsusenrico.financialtracker.listener.data.sync.SourcePocketQuestions
import com.alfonsusenrico.financialtracker.listener.data.sync.completeRecordedResult
import com.alfonsusenrico.financialtracker.listener.ui.components.ProcessingReasons
import com.google.gson.Gson
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneId

enum class MoneyDirection { IN, OUT, MOVE }

/** Colour family of a status; every tone is paired with an icon and a text label. */
enum class StatusTone { OK, WARN, ASK, MOVE, PHONE, MUTED }

enum class StatusIcon { CHECK, ALERT, HELP, SWAP, PHONE, EYE_OFF, CLOCK, REFRESH, SEND }

data class RowStatus(val label: String, val tone: StatusTone, val icon: StatusIcon)

/** The Aktivitas tag of a row the tracker recorded from a habit; TalkBack reads it with the row. */
const val HABIT_TAG = "Dari kebiasaan"

private const val HABIT_PREFIX = "Dicatat dari kebiasaan"

/**
 * Rincian's habit line, "Dicatat dari kebiasaan: Makan & Minum · 3 dari 3 jawaban sebelumnya". The
 * tracker's label already starts with the prefix; a label without it is the bare category name.
 */
fun RecordedHabit.detailLine(): String {
    val shownLabel = label.orEmpty().let { if (it.startsWith(HABIT_PREFIX, ignoreCase = true)) it else "$HABIT_PREFIX: $it" }
    return "$shownLabel · ${basis.orEmpty()}"
}

/** What the screens show for one stored notification, derived only from the row itself. */
data class RowPresentation(
    val row: RawNotificationEntity,
    val status: RowStatus,
    val question: QuestionKind?,
    val title: String,
    /** Null when neither the tracker nor the phone knows the amount; shown as "Rp —", never 0. */
    val amount: Long?,
    val direction: MoneyDirection?,
    /** The account line: the tracker's account(s) once recorded, otherwise the source app. */
    val account: String,
    val recorded: IngestResultItem?,
    /** Set only when the tracker recorded this expense or income from the owner's habits. */
    val habit: RecordedHabit?
) {
    val localDate: LocalDate get() = Instant.ofEpochMilli(row.postTime).atZone(ZoneId.systemDefault()).toLocalDate()

    companion object {
        private val gson = Gson()

        fun recordedResult(row: RawNotificationEntity): IngestResultItem? = try {
            gson.fromJson(row.resultJson, IngestResultItem::class.java)?.takeIf(::completeRecordedResult)
        } catch (_: RuntimeException) {
            null
        }

        fun of(row: RawNotificationEntity): RowPresentation {
            val question = OpenQuestions.kindOf(row)
            val recorded = recordedResult(row)?.takeIf { row.processingStatus == "recorded" && row.processingError == null }
            val questionAmount = when (question) {
                QuestionKind.SENDER -> SenderQuestions.parse(row.resultJson)?.confirmation?.amount
                QuestionKind.SOURCE_POCKET -> SourcePocketQuestions.parse(row.resultJson)?.confirmation?.amount
                QuestionKind.PURPOSE -> PurposeQuestions.parse(row.resultJson)?.second?.amount
                else -> null
            }
            val direction = when {
                recorded != null -> when (recorded.type) {
                    "income" -> MoneyDirection.IN
                    "expense" -> MoneyDirection.OUT
                    else -> MoneyDirection.MOVE
                }
                question == QuestionKind.SENDER -> MoneyDirection.IN
                question == QuestionKind.SOURCE_POCKET -> MoneyDirection.OUT
                else -> when (row.expectedDirection?.lowercase()) {
                    "out" -> MoneyDirection.OUT
                    "in" -> MoneyDirection.IN
                    "internal" -> MoneyDirection.MOVE
                    else -> null
                }
            }
            val amount = recorded?.amount ?: questionAmount
                ?: row.expectedAmount?.takeIf { it > 0 }?.toLong()
            val account = when {
                recorded == null -> row.appLabel
                direction == MoneyDirection.MOVE -> "${recorded.source} → ${recorded.target}"
                direction == MoneyDirection.IN -> recorded.target ?: row.appLabel
                else -> recorded.source ?: row.appLabel
            }
            val title = recorded?.description?.takeIf { it.isNotBlank() }
                ?: row.expectedCounterparty?.takeIf { it.isNotBlank() }
                ?: row.title?.takeIf { it.isNotBlank() }
                ?: row.appLabel
            val habit = recorded?.takeIf { it.type in setOf("expense", "income") }?.habit?.shown()
            return RowPresentation(row, status(row, question, recorded), question, title, amount, direction, account, recorded, habit)
        }

        /** The status label; the wording keeps the distinctions the companion has always shown. */
        fun status(row: RawNotificationEntity, question: QuestionKind?, recorded: IngestResultItem?): RowStatus = when {
            row.captureOutcome == "keep_local" -> RowStatus("Di ponsel", StatusTone.PHONE, StatusIcon.PHONE)
            !row.isSynced && row.isFinancial != true -> RowStatus("Tidak dikenali", StatusTone.MUTED, StatusIcon.HELP)
            !row.isSynced -> RowStatus("Belum dikirim", StatusTone.MUTED, StatusIcon.SEND)
            row.processingStatus == "queued" || row.processingStatus == "processing" ->
                RowStatus("Diproses", StatusTone.MUTED, StatusIcon.CLOCK)
            row.processingStatus == "recorded" && row.processingError != null ->
                RowStatus("Perlu diperiksa", StatusTone.WARN, StatusIcon.ALERT)
            question != null -> RowStatus(question.title, StatusTone.ASK, StatusIcon.HELP)
            row.processingStatus == "recorded" && recorded != null && recorded.type !in setOf("expense", "income") ->
                RowStatus("Pindah saldo", StatusTone.MOVE, StatusIcon.SWAP)
            row.processingStatus == "recorded" -> RowStatus("Tercatat", StatusTone.OK, StatusIcon.CHECK)
            row.processingStatus == "needs_confirmation" -> RowStatus("Perlu konfirmasi", StatusTone.WARN, StatusIcon.ALERT)
            row.processingStatus == "needs_review" -> RowStatus("Perlu cek", StatusTone.WARN, StatusIcon.ALERT)
            row.processingStatus == "failed" && ProcessingReasons.isServerRetrying(row.processingError) ->
                RowStatus("Diulang otomatis", StatusTone.MUTED, StatusIcon.REFRESH)
            row.processingStatus == "failed" -> RowStatus("Gagal", StatusTone.WARN, StatusIcon.ALERT)
            row.processingStatus == "ignored" -> RowStatus("Diabaikan", StatusTone.MUTED, StatusIcon.EYE_OFF)
            row.processingError != null -> RowStatus("Perlu diperiksa", StatusTone.WARN, StatusIcon.ALERT)
            else -> RowStatus("Terkirim", StatusTone.MUTED, StatusIcon.SEND)
        }
    }
}
