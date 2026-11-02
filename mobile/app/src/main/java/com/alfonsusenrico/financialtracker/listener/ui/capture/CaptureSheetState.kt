package com.alfonsusenrico.financialtracker.listener.ui.capture

import com.alfonsusenrico.financialtracker.listener.data.model.TextCaptureEntity
import com.alfonsusenrico.financialtracker.listener.data.model.TextCaptureState
import com.alfonsusenrico.financialtracker.listener.data.network.model.AccountItemResponse
import com.alfonsusenrico.financialtracker.listener.data.network.model.CategoryItemResponse
import com.alfonsusenrico.financialtracker.listener.data.textcapture.CaptureAnswer
import com.alfonsusenrico.financialtracker.listener.data.textcapture.TextCaptures
import com.alfonsusenrico.financialtracker.listener.data.textcapture.UndoWindow
import com.alfonsusenrico.financialtracker.listener.ui.detail.TransactionDraft
import com.alfonsusenrico.financialtracker.listener.ui.model.CapturePresentation

/** The capture sheet's screens (prototype flow A in the E visual system). */
enum class SheetPhase { INPUT, PROCESSING, RECORDED, SAVED, QUESTION, QUEUED, ERROR, FAILED, UNDONE, DELETED }

/** Which screen a capture shows at [now]; [sending] is a send this sheet started and has not finished. */
fun sheetPhase(capture: TextCaptureEntity?, sending: Boolean, now: Long): SheetPhase = when {
    capture == null -> SheetPhase.INPUT
    sending || capture.state == TextCaptureState.SENDING -> SheetPhase.PROCESSING
    capture.state == TextCaptureState.QUEUED -> when (capture.errorCode) {
        TextCaptures.SERVER, TextCaptures.AUTH -> SheetPhase.ERROR
        else -> SheetPhase.QUEUED
    }
    capture.state == TextCaptureState.RECORDED -> if (UndoWindow.isOpen(capture, now)) SheetPhase.RECORDED else SheetPhase.SAVED
    capture.state == TextCaptureState.NEEDS_DETAIL -> SheetPhase.QUESTION
    capture.state == TextCaptureState.FAILED -> SheetPhase.FAILED
    capture.state == TextCaptureState.UNDONE -> SheetPhase.UNDONE
    else -> SheetPhase.DELETED
}

/** Everything the stateless sheet content shows; [now] drives the undo countdown. */
data class CaptureSheetState(
    val capture: CapturePresentation?,
    val phase: SheetPhase,
    val suggestions: SuggestionState,
    val now: Long,
    /** Text to put into the input once, such as a refused capture's text after "Tulis ulang". */
    val prefill: String? = null,
    val busy: Boolean = false,
    val notice: String? = null,
    val autoSync: Boolean = true,
    val online: Boolean = true,
    val reducedMotion: Boolean = false,
    val canEdit: Boolean = false,
    val accounts: List<AccountItemResponse> = emptyList(),
    val categories: List<CategoryItemResponse> = emptyList()
) {
    val secondsLeft: Int get() = capture?.let { UndoWindow.secondsLeft(it.capture, now) } ?: 0

    /** The part of the undo window still left, for the draining bar. */
    val undoFraction: Float
        get() {
            val deadline = capture?.capture?.undoDeadline ?: return 0f
            return ((deadline - now).toFloat() / UndoWindow.WINDOW_MILLIS).coerceIn(0f, 1f)
        }
}

/** The sheet's callbacks; each one names the capture it acts on. */
class CaptureSheetActions(
    val onClose: () -> Unit,
    val onPrefilled: () -> Unit,
    val onSubmit: (text: String, onInvalid: () -> Unit) -> Unit,
    val onAnother: () -> Unit,
    val onUndo: (Long) -> Unit,
    val onEdit: (Long, TransactionDraft, (Boolean) -> Unit) -> Unit,
    val onAnswer: (Long, CaptureAnswer) -> Unit,
    val onResendAnswer: (Long) -> Unit,
    val onSend: (Long) -> Unit,
    /** "Tulis ulang": removes a refused capture and puts its text back in the input. */
    val onRewrite: (Long, String) -> Unit,
    val onOpenQueue: () -> Unit,
    val onOpenDetail: (Long) -> Unit,
    val onInvalid: (String) -> Unit
)
