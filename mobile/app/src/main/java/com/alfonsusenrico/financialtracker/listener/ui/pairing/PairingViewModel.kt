package com.alfonsusenrico.financialtracker.listener.ui.pairing

import android.app.Application
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import com.alfonsusenrico.financialtracker.listener.data.pairing.DeviceName
import com.alfonsusenrico.financialtracker.listener.data.pairing.PairingCode
import com.alfonsusenrico.financialtracker.listener.data.pairing.PairingOutcome
import com.alfonsusenrico.financialtracker.listener.data.pairing.PairingRepository
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.data.sync.SenderReplies
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch

enum class PairingScreen { LISTENER, BATTERY, CHOOSE, SCANNER, CONFIRM, MANUAL, RESULT }

data class PairingUiState(
    val screen: PairingScreen = PairingScreen.CHOOSE,
    val deviceName: String,
    val busy: Boolean = false,
    val outcome: PairingOutcome? = null,
    val replacing: Boolean = false
)

class PairingViewModel(application: Application) : AndroidViewModel(application) {
    private val prefs = AppPreferences(application)
    private val repository = PairingRepository(application)
    private var code: PairingCode? = null
    private val _state = MutableStateFlow(PairingUiState(deviceName = DeviceName.detect(application), replacing = prefs.isConfigured))
    val state = _state.asStateFlow()

    fun show(screen: PairingScreen) {
        if (_state.value.busy) return
        code = null
        _state.value = _state.value.copy(screen = screen, outcome = null)
    }

    fun accept(raw: String) {
        if (_state.value.busy) return
        code = PairingCode.parse(raw, prefs.serverUrl)
        _state.value = _state.value.copy(
            screen = if (code != null) PairingScreen.CONFIRM else PairingScreen.RESULT,
            outcome = if (code != null) null else PairingOutcome(false, "QR tidak valid. Pindai kode dari Pengaturan web tracker.")
        )
    }

    fun cameraUnavailable(message: String = "Izin kamera belum aktif. Masukkan API key manual atau coba Pindai lagi.") {
        show(PairingScreen.MANUAL)
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

    private fun run(action: suspend () -> PairingOutcome) {
        if (_state.value.busy) return
        _state.value = _state.value.copy(busy = true, outcome = null)
        viewModelScope.launch {
            val outcome = action()
            _state.value = _state.value.copy(
                busy = false,
                screen = if (_state.value.screen == PairingScreen.MANUAL && !outcome.keySaved) PairingScreen.MANUAL else PairingScreen.RESULT,
                outcome = outcome
            )
            if (outcome.connected) SenderReplies(getApplication()).recover()
        }
    }
}
