package com.alfonsusenrico.financialtracker.listener.ui.capture

import android.app.Application
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.model.TextCaptureEntity
import com.alfonsusenrico.financialtracker.listener.data.model.TextCaptureState
import com.alfonsusenrico.financialtracker.listener.data.network.model.CaptureSuggestion
import com.alfonsusenrico.financialtracker.listener.data.network.model.TransactionUpdateRequest
import com.alfonsusenrico.financialtracker.listener.data.textcapture.CaptureActionResult
import com.alfonsusenrico.financialtracker.listener.data.textcapture.CaptureAnswer
import com.alfonsusenrico.financialtracker.listener.data.textcapture.TextCaptures
import com.alfonsusenrico.financialtracker.listener.ui.detail.TransactionDraft
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.flatMapLatest
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.stateIn
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch

/** History suggestions for the input: loading, loaded (possibly empty), or not available right now. */
sealed interface SuggestionState {
    data object Loading : SuggestionState
    data class Loaded(val items: List<CaptureSuggestion>) : SuggestionState
    data object Unavailable : SuggestionState
}

/** A message for the open sheet, shown in place as a live region rather than behind the sheet. */
data class CaptureNotice(val captureId: Long?, val text: String)

class CaptureViewModel @JvmOverloads constructor(
    application: Application,
    db: AppDatabase = AppDatabase.getDatabase(application),
    private val captures: TextCaptures = TextCaptures(application, db)
) : AndroidViewModel(application) {

    private val dao = db.textCaptureDao()

    /** Null until Room has answered once, so Beranda never shows an unread count as 0. */
    val visible: StateFlow<List<TextCaptureEntity>?> =
        dao.observeVisible().stateIn(viewModelScope, SharingStarted.Eagerly, null)

    private val _sheetOpen = MutableStateFlow(false)
    val sheetOpen: StateFlow<Boolean> = _sheetOpen.asStateFlow()

    private val activeId = MutableStateFlow<Long?>(null)

    @OptIn(ExperimentalCoroutinesApi::class)
    val active: StateFlow<TextCaptureEntity?> = activeId
        .flatMapLatest { id -> if (id == null) flowOf(null) else dao.observeById(id) }
        .stateIn(viewModelScope, SharingStarted.Eagerly, null)

    /** Captures whose first send this sheet started and has not finished: shown as processing. */
    private val _inFlight = MutableStateFlow<Set<Long>>(emptySet())
    val inFlight: StateFlow<Set<Long>> = _inFlight.asStateFlow()

    private val _busy = MutableStateFlow(false)
    val busy: StateFlow<Boolean> = _busy.asStateFlow()

    private val _suggestions = MutableStateFlow<SuggestionState>(SuggestionState.Loading)
    val suggestions: StateFlow<SuggestionState> = _suggestions.asStateFlow()
    private var suggestionsJob: Job? = null

    private val _prefill = MutableStateFlow<String?>(null)
    val prefill: StateFlow<String?> = _prefill.asStateFlow()

    fun prefilled() {
        _prefill.value = null
    }

    private val _notice = MutableStateFlow<CaptureNotice?>(null)
    val notice: StateFlow<CaptureNotice?> = _notice.asStateFlow()

    fun observe(id: Long): Flow<TextCaptureEntity?> = dao.observeById(id)

    /** Opens the sheet on a new capture, or on [captureId] to continue it (answer, undo, retry). */
    fun open(captureId: Long? = null) {
        _notice.value = null
        activeId.value = captureId
        _sheetOpen.value = true
        if (captureId == null) loadSuggestions() else refreshIfAsking(captureId)
    }

    fun say(text: String) {
        _notice.value = CaptureNotice(activeId.value, text)
    }

    fun close() {
        _sheetOpen.value = false
        _notice.value = null
        activeId.value = null
    }

    /** "Catat lagi": back to an empty input with fresh suggestions. */
    fun startAnother() {
        _notice.value = null
        activeId.value = null
        loadSuggestions()
    }

    fun loadSuggestions() {
        suggestionsJob?.cancel()
        _suggestions.value = SuggestionState.Loading
        suggestionsJob = viewModelScope.launch {
            _suggestions.value = captures.suggestions()?.let { SuggestionState.Loaded(it) } ?: SuggestionState.Unavailable
        }
    }

    private fun refreshIfAsking(id: Long) {
        viewModelScope.launch {
            val capture = dao.getById(id) ?: return@launch
            if (capture.state == TextCaptureState.NEEDS_DETAIL && capture.pendingAnswerJson == null) captures.refresh(id)
        }
    }

    /** Stores the capture, then sends it at once; [onInvalid] runs when the text breaks the rules. */
    fun submit(text: String, onInvalid: () -> Unit) {
        viewModelScope.launch {
            val id = captures.create(text)
            if (id == null) {
                onInvalid()
                return@launch
            }
            _notice.value = null
            activeId.value = id
            _inFlight.update { it + id }
            try {
                captures.send(id, ownerAsked = false)
            } finally {
                _inFlight.update { it - id }
            }
        }
    }

    /** "Kirim" or "Coba lagi": an owner action, so a failed attempt is resent once online. */
    fun send(id: Long) = act(id, inFlight = true) {
        captures.retry(id)
        null
    }

    fun answer(id: Long, answer: CaptureAnswer) = act(id) {
        CaptureMessages.answer(captures.answer(id, answer))
    }

    fun resendAnswer(id: Long) = act(id) {
        CaptureMessages.answer(captures.resendAnswer(id))
    }

    fun undo(id: Long) = act(id) {
        CaptureMessages.undo(captures.undo(id))
    }

    fun edit(id: Long, draft: TransactionDraft, done: (Boolean) -> Unit) = act(id) {
        val result = captures.edit(id, TransactionUpdateRequest(draft.type, draft.amount, draft.accountId, draft.categoryId, draft.notes))
        done(result == CaptureActionResult.DONE)
        CaptureMessages.edit(result)
    }

    /** Captures on the phone only; [onDone] gets whether it was removed. */
    fun discard(id: Long, onDone: (Boolean) -> Unit) {
        viewModelScope.launch {
            val removed = captures.discard(id)
            if (removed && activeId.value == id) activeId.value = null
            onDone(removed)
        }
    }

    /** "Tulis ulang": a refused capture leaves the phone and its text returns to the input. */
    fun rewrite(id: Long, text: String) {
        viewModelScope.launch {
            if (captures.discard(id)) {
                _prefill.value = text
                startAnother()
            }
        }
    }

    fun deleteFromTracker(id: Long, onDone: (String) -> Unit) {
        viewModelScope.launch {
            _busy.value = true
            try {
                onDone(CaptureMessages.delete(captures.deleteFromTracker(id)))
            } finally {
                _busy.value = false
            }
        }
    }

    fun refresh(id: Long) {
        viewModelScope.launch { captures.refresh(id) }
    }

    private fun act(id: Long, inFlight: Boolean = false, block: suspend () -> String?) {
        viewModelScope.launch {
            _busy.value = true
            _notice.value = null
            if (inFlight) _inFlight.update { it + id }
            try {
                block()?.let { _notice.value = CaptureNotice(id, it) }
            } finally {
                if (inFlight) _inFlight.update { it - id }
                _busy.value = false
            }
        }
    }
}
