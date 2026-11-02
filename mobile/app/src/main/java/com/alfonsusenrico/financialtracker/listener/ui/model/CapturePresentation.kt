package com.alfonsusenrico.financialtracker.listener.ui.model

import com.alfonsusenrico.financialtracker.listener.data.model.TextCaptureEntity
import com.alfonsusenrico.financialtracker.listener.data.model.TextCaptureState
import com.alfonsusenrico.financialtracker.listener.data.network.model.CaptureQuestion
import com.alfonsusenrico.financialtracker.listener.data.network.model.CaptureView
import com.alfonsusenrico.financialtracker.listener.data.textcapture.TextCaptures
import java.time.DateTimeException
import java.time.Instant
import java.time.LocalDate
import java.time.OffsetDateTime
import java.time.ZoneId

/** The web's source label for typed captures (`provenance.ts`), so both say the same. */
const val CAPTURE_SOURCE_LABEL = "Diketik di ponsel"

/** A recorded amount the home figures add up, from a notification or a capture on this phone. */
data class MoneyFact(val date: LocalDate, val direction: MoneyDirection, val amount: Long, val category: String?)

/** What the screens show for one typed capture, derived from its row and the tracker's last view. */
data class CapturePresentation(
    val capture: TextCaptureEntity,
    val view: CaptureView?,
    val question: CaptureQuestion?,
    val status: RowStatus,
    val title: String,
    /** Null until the tracker knows it; shown as "Rp —", never 0. */
    val amount: Long?,
    val direction: MoneyDirection?,
    /** The tracker's account, or both accounts of a move; null before it is known. */
    val account: String?,
    val category: String?,
    val kakeibo: String?,
    /** The transaction's time once recorded, otherwise when it was typed. */
    val moment: Long
) {
    val id: Long get() = capture.id
    val localDate: LocalDate get() = Instant.ofEpochMilli(moment).atZone(ZoneId.systemDefault()).toLocalDate()
    val isMovement: Boolean get() = direction == MoneyDirection.MOVE
    val isRecorded: Boolean get() = capture.state == TextCaptureState.RECORDED

    /** Not yet settled by the tracker: waiting to be sent, being sent, refused, or asking a question. */
    val isUnsettled: Boolean get() = capture.state in UNSETTLED

    /** Still only on this phone: the tracker has not accepted it. */
    val isOnPhoneOnly: Boolean get() = capture.serverId == null && capture.state in ON_PHONE

    /** The recorded money this capture adds to the home figures; nothing until it is recorded. */
    val recordedFact: MoneyFact?
        get() {
            if (!isRecorded) return null
            val value = amount ?: return null
            val towards = direction ?: return null
            return MoneyFact(localDate, towards, value, category.takeIf { towards == MoneyDirection.OUT })
        }

    companion object {
        private val UNSETTLED = setOf(TextCaptureState.QUEUED, TextCaptureState.SENDING, TextCaptureState.FAILED, TextCaptureState.NEEDS_DETAIL)
        private val ON_PHONE = setOf(TextCaptureState.QUEUED, TextCaptureState.SENDING, TextCaptureState.FAILED)

        fun of(capture: TextCaptureEntity): CapturePresentation {
            val view = TextCaptures.view(capture)
            val question = TextCaptures.question(capture)
            val transaction = view?.transaction
            val movement = view?.movement
            val kind = transaction?.type?.takeIf { movement == null } ?: if (movement != null) "movement" else view?.summary?.type
            val direction = when (kind) {
                "expense" -> MoneyDirection.OUT
                "income" -> MoneyDirection.IN
                "movement" -> MoneyDirection.MOVE
                else -> null
            }
            val account = when {
                movement?.source?.accountName != null && movement.target?.accountName != null ->
                    "${movement.source.accountName} → ${movement.target.accountName}"
                else -> transaction?.accountName
            }
            val title = listOf(transaction?.merchant, transaction?.notes, view?.summary?.description)
                .firstOrNull { !it.isNullOrBlank() }?.trim() ?: capture.text
            val recordedAt = transaction?.date?.let(::epochMillis)?.takeIf { capture.state == TextCaptureState.RECORDED }
            return CapturePresentation(
                capture = capture,
                view = view,
                question = question.takeIf { capture.state == TextCaptureState.NEEDS_DETAIL },
                status = status(capture, direction),
                title = title,
                amount = transaction?.amount ?: view?.summary?.amount,
                direction = direction,
                account = account,
                category = transaction?.categoryName.takeIf { movement == null },
                kakeibo = transaction?.kakeibo.takeIf { movement == null && direction == MoneyDirection.OUT },
                moment = recordedAt ?: capture.createdAt
            )
        }

        fun status(capture: TextCaptureEntity, direction: MoneyDirection?): RowStatus = when (capture.state) {
            TextCaptureState.SENDING -> RowStatus("Dikirim", StatusTone.MUTED, StatusIcon.CLOCK)
            TextCaptureState.QUEUED -> RowStatus("Di antrean", StatusTone.PHONE, StatusIcon.PHONE)
            TextCaptureState.NEEDS_DETAIL -> RowStatus("Perlu jawaban", StatusTone.ASK, StatusIcon.HELP)
            TextCaptureState.FAILED -> RowStatus("Gagal dicatat", StatusTone.WARN, StatusIcon.ALERT)
            TextCaptureState.UNDONE -> RowStatus("Dibatalkan", StatusTone.MUTED, StatusIcon.REFRESH)
            TextCaptureState.DELETED -> RowStatus("Dihapus dari tracker", StatusTone.MUTED, StatusIcon.EYE_OFF)
            else -> if (direction == MoneyDirection.MOVE) RowStatus("Pindah saldo", StatusTone.MOVE, StatusIcon.SWAP)
            else RowStatus("Tercatat", StatusTone.OK, StatusIcon.CHECK)
        }

        private fun epochMillis(value: String): Long? = try {
            OffsetDateTime.parse(value).toInstant().toEpochMilli()
        } catch (_: DateTimeException) {
            null
        }
    }
}

/** The web's Kakeibo pillar names (`kakeibo.ts`). */
fun kakeiboLabel(pillar: String?): String? = when (pillar) {
    "need" -> "Kebutuhan"
    "want" -> "Keinginan"
    "culture" -> "Budaya"
    "unexpected" -> "Tak Terduga"
    "saving" -> "Tabungan"
    else -> null
}
