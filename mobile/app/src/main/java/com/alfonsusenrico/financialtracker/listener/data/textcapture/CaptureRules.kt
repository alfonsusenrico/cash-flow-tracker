package com.alfonsusenrico.financialtracker.listener.data.textcapture

import com.alfonsusenrico.financialtracker.listener.data.model.TextCaptureEntity
import com.alfonsusenrico.financialtracker.listener.data.model.TextCaptureState
import com.alfonsusenrico.financialtracker.listener.data.network.model.CaptureQuestion
import com.alfonsusenrico.financialtracker.listener.data.network.model.CaptureQuestionTypes
import com.alfonsusenrico.financialtracker.listener.data.network.model.CaptureView
import com.alfonsusenrico.financialtracker.listener.data.sync.validUuid
import java.text.Normalizer
import java.time.DateTimeException
import java.time.OffsetDateTime

/** The backend's text rules (`clean_text`), applied before a capture is stored so a refusal is rare. */
object CaptureText {
    const val MAX_LENGTH = 200
    const val ANSWER_MAX_LENGTH = 50

    /**
     * NFKC-normalised text with every run of whitespace, line breaks included, as one space; null when it
     * is empty, longer than [limit] code points, or holds a control or unassigned character.
     */
    fun normalize(raw: String, limit: Int = MAX_LENGTH): String? {
        val text = Normalizer.normalize(raw, Normalizer.Form.NFKC).trim().split(Regex("\\s+")).joinToString(" ")
        val length = text.codePointCount(0, text.length)
        if (length !in 1..limit) return null
        val refused = text.codePoints().anyMatch { point ->
            when (Character.getType(point).toByte()) {
                Character.CONTROL, Character.FORMAT, Character.SURROGATE, Character.PRIVATE_USE, Character.UNASSIGNED -> true
                else -> false
            }
        }
        return text.takeUnless { refused }
    }
}

/**
 * The 10-second undo window on this phone's clock. The server allows 5 more seconds for the request to
 * arrive, so an undo sent before the local deadline is accepted.
 */
object UndoWindow {
    const val WINDOW_MILLIS = 10_000L

    /**
     * The local deadline of a capture recorded with [undoUntil], seen at [now]. [serverTime] (the
     * response's `Date`) converts the server's deadline to this clock; without it the full window counts.
     */
    fun localDeadline(undoUntil: String?, serverTime: Long?, now: Long): Long? {
        val until = parseInstant(undoUntil) ?: return null
        val remaining = if (serverTime != null) until - serverTime else WINDOW_MILLIS
        return now + remaining.coerceIn(0, WINDOW_MILLIS)
    }

    fun isOpen(capture: TextCaptureEntity, now: Long): Boolean =
        capture.state == TextCaptureState.RECORDED && !capture.edited && capture.undoDeadline != null && now < capture.undoDeadline

    /** Whole seconds left, rounded up, as the countdown shows them. */
    fun secondsLeft(capture: TextCaptureEntity, now: Long): Int {
        val deadline = capture.undoDeadline ?: return 0
        return ((deadline - now + 999) / 1000).toInt().coerceIn(0, (WINDOW_MILLIS / 1000).toInt())
    }

    private fun parseInstant(value: String?): Long? = try {
        value?.let { OffsetDateTime.parse(it).toInstant().toEpochMilli() }
    } catch (_: DateTimeException) {
        null
    }
}

/** A capture view is used only when it belongs to the capture that was sent and is internally whole. */
fun CaptureView.isValidFor(clientCaptureId: String, serverId: String?): Boolean {
    if (!validUuid(id) || (serverId != null && id != serverId)) return false
    if (this.clientCaptureId != null && this.clientCaptureId != clientCaptureId) return false
    return when (TextCaptureState.fromServer(status)) {
        TextCaptureState.NEEDS_DETAIL -> question?.isAnswerable() == true
        TextCaptureState.RECORDED -> transaction?.id == null || validUuid(transaction.id)
        TextCaptureState.UNDONE -> true
        else -> false
    }
}

fun CaptureQuestion.isAnswerable(): Boolean =
    validUuid(questionId) && type in CaptureQuestionTypes.ANSWERED && !prompt.isNullOrBlank()

/** What the owner may do with a capture from this phone. */
object CaptureAbilities {
    private val LEDGER_TYPES = setOf("expense", "income")

    /** The expense or income this capture created, under the pairing that recorded it. */
    fun ownsLedgerRow(capture: TextCaptureEntity, view: CaptureView?, currentPairing: String): Boolean =
        capture.state == TextCaptureState.RECORDED && capture.pairingContext == currentPairing &&
            validUuid(capture.transactionId) && view?.movement == null && view?.transaction?.type in LEDGER_TYPES &&
            view?.transaction?.id == capture.transactionId

    /** "Hapus dari tracker": only for a transaction a capture created, never for a notification row. */
    fun canDeleteFromTracker(capture: TextCaptureEntity, view: CaptureView?, currentPairing: String): Boolean =
        ownsLedgerRow(capture, view, currentPairing)

    fun canEdit(capture: TextCaptureEntity, view: CaptureView?, currentPairing: String): Boolean =
        ownsLedgerRow(capture, view, currentPairing)

    /** A capture still on the phone: never sent, or refused by the tracker. */
    fun canDiscard(capture: TextCaptureEntity): Boolean = capture.serverId == null &&
        (capture.state == TextCaptureState.QUEUED && capture.attempts == 0 || capture.state == TextCaptureState.FAILED)

    fun canSend(capture: TextCaptureEntity): Boolean =
        capture.serverId == null && capture.state in setOf(TextCaptureState.QUEUED, TextCaptureState.FAILED)
}
