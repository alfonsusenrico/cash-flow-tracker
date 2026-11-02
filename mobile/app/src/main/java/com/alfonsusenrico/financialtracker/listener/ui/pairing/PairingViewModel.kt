package com.alfonsusenrico.financialtracker.listener.ui.pairing

import android.app.Application
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import com.alfonsusenrico.financialtracker.listener.data.health.HealthReports
import com.alfonsusenrico.financialtracker.listener.data.health.SelfTestSession
import com.alfonsusenrico.financialtracker.listener.data.health.SelfTestStep
import com.alfonsusenrico.financialtracker.listener.data.health.selfTestRunner
import com.alfonsusenrico.financialtracker.listener.data.pairing.DeviceName
import com.alfonsusenrico.financialtracker.listener.data.pairing.PairingCode
import com.alfonsusenrico.financialtracker.listener.data.pairing.PairingOutcome
import com.alfonsusenrico.financialtracker.listener.data.pairing.PairingRepository
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.data.sync.SenderReplies
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch

/**
 * The setup story, in order: welcome, pairing (QR, typed code or API key), notification access, battery and
 * background, the notification permission, the self-test, and "HP-mu siap". A re-pair from Pengaturan uses
 * the pairing screens only.
 */
enum class PairingScreen(val step: Int) {
    WELCOME(0), CHOOSE(1), SCANNER(1), CODE(1), CONFIRM(1), MANUAL(1), RESULT(1),
    LISTENER(2), BATTERY(3), NOTIFICATIONS(4), SELF_TEST(5), READY(6)
}

data class PairingUiState(
    val screen: PairingScreen = PairingScreen.CHOOSE,
    val deviceName: String,
    val busy: Boolean = false,
    val outcome: PairingOutcome? = null,
    val replacing: Boolean = false,
    /** The full first-run story; false for a re-pair or the manual key from Pengaturan. */
    val setup: Boolean = false,
    val codeError: String? = null,
    /** The notification step was answered: true when allowed, false when refused, null before. */
    val notificationsAnswered: Boolean? = null,
    /** The owner skipped the self-test's notification step; "HP-mu siap" says it will check on its own. */
    val postSkipped: Boolean = false
)

class PairingViewModel(application: Application) : AndroidViewModel(application) {
    private val prefs = AppPreferences(application)
    private val repository = PairingRepository(application)
    private var code: PairingCode? = null
    private val _state = MutableStateFlow(PairingUiState(deviceName = DeviceName.detect(application), replacing = prefs.isConfigured))
    val state = _state.asStateFlow()
    val selfTest = SelfTestSession { selfTestRunner(getApplication()) }

    /** Where the activity starts: the welcome card on first run, the manual key, or the pairing choice. */
    fun start(setup: Boolean, manual: Boolean) {
        _state.value = _state.value.copy(setup = setup)
        show(
            when {
                setup -> PairingScreen.WELCOME
                manual -> PairingScreen.MANUAL
                else -> PairingScreen.CHOOSE
            }
        )
    }

    fun show(screen: PairingScreen) {
        if (_state.value.busy) return
        code = null
        _state.value = _state.value.copy(screen = screen, outcome = null, codeError = null)
    }

    fun accept(raw: String) {
        if (_state.value.busy) return
        code = PairingCode.parse(raw, prefs.serverUrl)
        _state.value = _state.value.copy(
            screen = if (code != null) PairingScreen.CONFIRM else PairingScreen.RESULT,
            outcome = if (code != null) null else PairingOutcome(false, "Ini bukan QR dari Cash Flow Tracker. Pindai QR di halaman Hubungkan HP, ya.")
        )
    }

    /** The code typed from under the web's QR. */
    fun typed(raw: String) {
        if (_state.value.busy) return
        val parsed = PairingCode.typed(raw)
        if (parsed == null) {
            _state.value = _state.value.copy(codeError = "Kodenya ${PairingCode.LENGTH} huruf atau angka, ada di bawah QR.")
            return
        }
        code = parsed
        _state.value = _state.value.copy(screen = PairingScreen.CONFIRM, outcome = null, codeError = null)
    }

    fun cameraUnavailable(message: String = "Kamera nggak diizinin. Nggak apa-apa, ketik kodenya aja.") {
        show(PairingScreen.CODE)
        _state.value = _state.value.copy(outcome = PairingOutcome(false, message))
    }

    fun showSettingsError() {
        _state.value = _state.value.copy(outcome = PairingOutcome(false,
            "Halaman Android tidak tersedia. Buka Pengaturan Android untuk mengatur izin aplikasi."))
    }

    fun exchange() {
        val pending = code ?: return
        run { repository.exchange(pending, _state.value.deviceName) }
        code = null
    }

    fun manual(key: String) = run { repository.connectManual(key) }
    fun verify() = run { repository.verify() }

    fun notificationsAnswered(granted: Boolean) {
        _state.value = _state.value.copy(notificationsAnswered = granted)
    }

    /** The self-test card: on success the story moves on to "HP-mu siap". */
    fun runSelfTest() {
        viewModelScope.launch {
            val outcome = selfTest.run() ?: return@launch
            if (outcome.passed) show(PairingScreen.READY)
        }
    }

    fun retryServer() {
        viewModelScope.launch {
            val outcome = selfTest.retryServer() ?: return@launch
            if (outcome.passed) show(PairingScreen.READY)
        }
    }

    /** "Lewati bagian ini" when the test notification cannot show: setup ends, the first bank notification checks instead. */
    fun skipPost() {
        if (selfTest.progress.value.outcome?.failedStep != SelfTestStep.POST) return
        _state.value = _state.value.copy(postSkipped = true)
        show(PairingScreen.READY)
    }

    private fun run(action: suspend () -> PairingOutcome) {
        if (_state.value.busy) return
        _state.value = _state.value.copy(busy = true, outcome = null)
        viewModelScope.launch {
            val outcome = action()
            val connectedInSetup = outcome.connected && _state.value.setup
            _state.value = _state.value.copy(
                busy = false,
                screen = when {
                    connectedInSetup -> PairingScreen.LISTENER
                    _state.value.screen == PairingScreen.MANUAL && !outcome.keySaved -> PairingScreen.MANUAL
                    else -> PairingScreen.RESULT
                },
                outcome = outcome.takeUnless { connectedInSetup }
            )
            if (outcome.connected) {
                SenderReplies(getApplication()).recover()
                HealthReports.schedulePeriodic(getApplication())
                HealthReports.request(getApplication())
            }
        }
    }
}
