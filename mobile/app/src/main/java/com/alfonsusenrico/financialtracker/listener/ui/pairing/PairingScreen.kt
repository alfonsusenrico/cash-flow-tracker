package com.alfonsusenrico.financialtracker.listener.ui.pairing

import androidx.activity.compose.BackHandler
import androidx.compose.foundation.background
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.safeDrawingPadding
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.automirrored.outlined.ArrowBack
import androidx.compose.material.icons.outlined.PhoneAndroid
import androidx.compose.material.icons.outlined.QrCodeScanner
import androidx.compose.material.icons.outlined.WarningAmber
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.semantics.LiveRegionMode
import androidx.compose.ui.semantics.contentDescription
import androidx.compose.ui.semantics.heading
import androidx.compose.ui.semantics.liveRegion
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.text.input.PasswordVisualTransformation
import androidx.compose.ui.unit.dp
import com.alfonsusenrico.financialtracker.listener.data.pairing.PairingOutcome
import com.alfonsusenrico.financialtracker.listener.ui.components.ButtonKind
import com.alfonsusenrico.financialtracker.listener.ui.components.CompanionButton
import com.alfonsusenrico.financialtracker.listener.ui.components.CompanionCard
import com.alfonsusenrico.financialtracker.listener.ui.settings.HiosNote
import com.alfonsusenrico.financialtracker.listener.ui.onboarding.PermissionScreen
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionTheme
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionType

class PairingActions(
    val show: (PairingScreen) -> Unit,
    val scan: () -> Unit,
    val confirm: () -> Unit,
    val manual: (String) -> Unit,
    val verify: () -> Unit,
    val finish: () -> Unit,
    val notificationAccess: () -> Unit,
    val batteryAccess: () -> Unit,
    val appInfo: () -> Unit
)

/** Both scanned codes and external links reach this same explicit confirmation. */
@Composable
fun PairingContent(
    state: PairingUiState,
    actions: PairingActions,
    listenerGranted: Boolean = false,
    batteryExempt: Boolean = false,
    scanner: @Composable () -> Unit = {}
) {
    val colors = CompanionTheme.colors
    val step = when (state.screen) {
        PairingScreen.LISTENER -> 1
        PairingScreen.BATTERY -> 2
        else -> 3
    }
    BackHandler(enabled = state.busy || state.screen in setOf(PairingScreen.SCANNER, PairingScreen.CONFIRM, PairingScreen.MANUAL)) {
        if (!state.busy) actions.show(PairingScreen.CHOOSE)
    }
    if (state.screen == PairingScreen.SCANNER) {
        scanner()
        return
    }
    Column(
        Modifier.fillMaxSize().background(colors.bg).safeDrawingPadding()
            .verticalScroll(rememberScrollState()).padding(18.dp),
        verticalArrangement = Arrangement.spacedBy(16.dp)
    ) {
        Row(Modifier.fillMaxWidth(), verticalAlignment = Alignment.CenterVertically) {
            IconButton(onClick = {
                when (state.screen) {
                    PairingScreen.BATTERY -> actions.show(PairingScreen.LISTENER)
                    PairingScreen.LISTENER, PairingScreen.CHOOSE, PairingScreen.RESULT -> actions.finish()
                    else -> actions.show(PairingScreen.CHOOSE)
                }
            }, enabled = !state.busy) { Icon(Icons.AutoMirrored.Outlined.ArrowBack, "Kembali", tint = colors.text) }
            Text("Pengaturan awal", style = CompanionType.meta, color = colors.text2)
        }
        Text("0$step / 03", style = CompanionType.todayTotal, color = colors.text2, modifier = Modifier.semantics { contentDescription = "Langkah $step dari 3" })
        when (state.screen) {
            PairingScreen.LISTENER -> {
                PermissionScreen(
                    onContinue = { actions.show(PairingScreen.BATTERY) },
                    permissionGranted = listenerGranted,
                    onOpenAccess = actions.notificationAccess
                )
            }
            PairingScreen.BATTERY -> {
                PairingTitle("Tetap jalan di latar")
                PairingCopy("Supaya Android tidak menghentikan pencatat untuk hemat baterai.")
                if (batteryExempt) PairingCopy("Dikecualikan dari optimasi baterai")
                else CompanionButton("Kecualikan dari optimasi baterai", actions.batteryAccess, Modifier.fillMaxWidth(), ButtonKind.ACCENT)
                HiosNote()
                CompanionButton("Buka pengaturan aplikasi", actions.appInfo, Modifier.fillMaxWidth(), ButtonKind.GHOST)
                CompanionButton(if (batteryExempt) "Lanjut" else "Lewati dulu", { actions.show(PairingScreen.CHOOSE) }, Modifier.fillMaxWidth())
            }
            PairingScreen.CHOOSE -> {
                PairingTitle("Hubungkan ke tracker")
                PairingCopy("Di komputer, buka Tracker Web → Pengaturan → Hubungkan ponsel. Lalu pindai kode QR yang muncul.")
                DeviceCard(state.deviceName)
                ScanButton(actions.scan)
                ManualButton(actions)
                PairingCopy("Izin kamera diminta saat memindai. Kamera hanya dipakai untuk membaca kode.")
            }
            PairingScreen.CONFIRM -> {
                PairingTitle("QR terbaca")
                PairingCopy("Hubungkan ponsel ini ke akun yang menampilkan kode QR?")
                DeviceCard(state.deviceName)
                if (state.replacing) PairingCopy("Pemasangan ulang langsung mengganti kunci lama perangkat ini. Jika koneksi terputus, buat kode QR baru di web.")
                CompanionButton("Hubungkan", actions.confirm, Modifier.fillMaxWidth(), ButtonKind.ACCENT, busy = state.busy, enabled = !state.busy)
                CompanionButton("Batal", { actions.show(PairingScreen.CHOOSE) }, Modifier.fillMaxWidth(), ButtonKind.GHOST, enabled = !state.busy)
            }
            PairingScreen.MANUAL -> {
                PairingTitle("Masukkan API key")
                PairingCopy("Salin kunci dari Tracker Web → Pengaturan → Kunci API. Pakai ini bila tidak bisa memindai QR.")
                DeviceCard(state.deviceName)
                // A key is intentionally not rememberSaveable: it must not enter saved instance state.
                var key by remember { mutableStateOf("") }
                OutlinedTextField(
                    value = key, onValueChange = { key = it }, enabled = !state.busy,
                    label = { Text("API key") }, singleLine = true,
                    visualTransformation = PasswordVisualTransformation(), modifier = Modifier.fillMaxWidth()
                )
                CompanionButton("Tes Koneksi", { actions.manual(key) }, Modifier.fillMaxWidth(), enabled = !state.busy && key.isNotBlank(), busy = state.busy)
                ScanButton(actions.scan, !state.busy)
            }
            PairingScreen.RESULT -> {
                PairingTitle(if (state.outcome?.connected == true) "Terhubung" else "Belum terhubung")
                state.outcome?.let { OutcomeMessage(it) }
                DeviceCard(state.deviceName)
                if (state.outcome?.keySaved == true) {
                    CompanionButton("Tes Koneksi", actions.verify, Modifier.fillMaxWidth(), ButtonKind.GHOST, enabled = !state.busy, busy = state.busy)
                }
                if (state.outcome?.connected == true) {
                    CompanionButton("Selesai, buka beranda", actions.finish, Modifier.fillMaxWidth(), ButtonKind.ACCENT, enabled = !state.busy)
                } else {
                    CompanionButton("Pindai lagi", actions.scan, Modifier.fillMaxWidth(), ButtonKind.ACCENT, enabled = !state.busy)
                    ManualButton(actions, !state.busy)
                }
            }
            PairingScreen.SCANNER -> Unit
        }
        if (state.busy) PairingCopy("Menghubungi tracker…")
        if (state.screen != PairingScreen.RESULT) state.outcome?.let { OutcomeMessage(it) }
    }
}

@Composable private fun PairingTitle(text: String) {
    Text(text, style = CompanionType.onboardingTitle, color = CompanionTheme.colors.text, modifier = Modifier.semantics { heading() })
}
@Composable private fun PairingCopy(text: String) {
    Text(text, style = CompanionType.body, color = CompanionTheme.colors.text2)
}
@Composable private fun DeviceCard(name: String) {
    CompanionCard {
        Icon(Icons.Outlined.PhoneAndroid, null, tint = CompanionTheme.colors.text)
        PairingCopy("Nama perangkat · terdeteksi otomatis")
        Text(name, style = CompanionType.bodyStrong, color = CompanionTheme.colors.text)
    }
}
@Composable private fun ScanButton(scan: () -> Unit, enabled: Boolean = true) {
    CompanionButton("Pindai QR dari Pengaturan web", scan, Modifier.fillMaxWidth(), ButtonKind.ACCENT, Icons.Outlined.QrCodeScanner, enabled)
}
@Composable private fun ManualButton(actions: PairingActions, enabled: Boolean = true) {
    CompanionButton("Masukkan API key manual", { actions.show(PairingScreen.MANUAL) }, Modifier.fillMaxWidth(), ButtonKind.GHOST, enabled = enabled)
}

@Composable private fun OutcomeMessage(outcome: PairingOutcome) {
    val colors = CompanionTheme.colors
    Text(
        outcome.message, style = CompanionType.body,
        color = if (outcome.connected) colors.okFg else colors.danger,
        modifier = Modifier.semantics { liveRegion = LiveRegionMode.Polite }
    )
}
