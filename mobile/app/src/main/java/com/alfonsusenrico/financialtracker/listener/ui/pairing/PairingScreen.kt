package com.alfonsusenrico.financialtracker.listener.ui.pairing

import androidx.activity.compose.BackHandler
import androidx.compose.foundation.background
import androidx.compose.foundation.border
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.ColumnScope
import androidx.compose.foundation.layout.ExperimentalLayoutApi
import androidx.compose.foundation.layout.FlowRow
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.safeDrawingPadding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.foundation.verticalScroll
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.automirrored.outlined.OpenInNew
import androidx.compose.material.icons.automirrored.rounded.ArrowBack
import androidx.compose.material.icons.outlined.BatteryChargingFull
import androidx.compose.material.icons.outlined.Celebration
import androidx.compose.material.icons.outlined.CloudOff
import androidx.compose.material.icons.outlined.Info
import androidx.compose.material.icons.outlined.Key
import androidx.compose.material.icons.outlined.Keyboard
import androidx.compose.material.icons.outlined.NotificationsActive
import androidx.compose.material.icons.outlined.PhoneAndroid
import androidx.compose.material.icons.outlined.QrCodeScanner
import androidx.compose.material.icons.outlined.Shield
import androidx.compose.material.icons.outlined.TaskAlt
import androidx.compose.material.icons.outlined.WarningAmber
import androidx.compose.material.icons.rounded.Check
import androidx.compose.material3.Icon
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.graphics.vector.ImageVector
import androidx.compose.ui.semantics.LiveRegionMode
import androidx.compose.ui.semantics.clearAndSetSemantics
import androidx.compose.ui.semantics.contentDescription
import androidx.compose.ui.semantics.heading
import androidx.compose.ui.semantics.liveRegion
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.input.KeyboardCapitalization
import androidx.compose.ui.text.input.PasswordVisualTransformation
import androidx.compose.ui.unit.dp
import com.alfonsusenrico.financialtracker.listener.data.health.SelfTestProgress
import com.alfonsusenrico.financialtracker.listener.data.health.SelfTestStep
import com.alfonsusenrico.financialtracker.listener.data.pairing.PairingOutcome
import com.alfonsusenrico.financialtracker.listener.ui.components.ButtonKind
import com.alfonsusenrico.financialtracker.listener.ui.components.CompanionButton
import com.alfonsusenrico.financialtracker.listener.ui.components.Note
import com.alfonsusenrico.financialtracker.listener.ui.components.SquareIconButton
import com.alfonsusenrico.financialtracker.listener.ui.components.toneColors
import com.alfonsusenrico.financialtracker.listener.ui.hub.OemHint
import com.alfonsusenrico.financialtracker.listener.ui.model.StatusTone
import com.alfonsusenrico.financialtracker.listener.ui.settings.NO_FINANCE_APPS
import com.alfonsusenrico.financialtracker.listener.ui.settings.SelfTestChecklist
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
    val appInfo: () -> Unit,
    val typed: (String) -> Unit = {},
    val askNotifications: () -> Unit = {},
    val runSelfTest: () -> Unit = {},
    val retryServer: () -> Unit = {},
    val skipPost: () -> Unit = {},
    val openWeb: () -> Unit = {}
)

/** What Android says during setup, read again whenever the owner returns from a settings page. */
data class SetupPermissions(
    val listener: Boolean = false,
    val battery: Boolean = false,
    val notifications: Boolean = false,
    val oem: OemHint? = null,
    /** Labels of the registered finance apps installed on this phone, once each. */
    val financeApps: List<String> = emptyList()
)

/**
 * Setup as story cards (onboarding C): an illustration, five progress segments and a sticky action bar. Both
 * scanned codes and external links reach the same explicit confirmation before anything is exchanged.
 */
@Composable
fun PairingContent(
    state: PairingUiState,
    actions: PairingActions,
    listenerGranted: Boolean = false,
    batteryExempt: Boolean = false,
    scanner: @Composable () -> Unit = {},
    permissions: SetupPermissions = SetupPermissions(listenerGranted, batteryExempt),
    selfTest: SelfTestProgress = SelfTestProgress()
) {
    BackHandler(enabled = state.busy || state.screen in setOf(PairingScreen.SCANNER, PairingScreen.CONFIRM, PairingScreen.MANUAL, PairingScreen.CODE)) {
        if (!state.busy) actions.show(PairingScreen.CHOOSE)
    }
    if (state.screen == PairingScreen.SCANNER) {
        scanner()
        return
    }
    val card = storyCard(state, actions, permissions, selfTest)
    StoryFrame(state, card, actions)
}

/** One card's content: its symbol, words, body and the two actions of the sticky bar. */
private class StoryCard(
    val icon: ImageVector,
    val tone: StatusTone,
    val title: String,
    val lead: String,
    val primary: Pair<String, () -> Unit>?,
    val secondary: Pair<String, () -> Unit>? = null,
    val back: (() -> Unit)? = null,
    val body: @Composable ColumnScope.() -> Unit = {}
)

@OptIn(ExperimentalLayoutApi::class)
@Composable
private fun StoryFrame(state: PairingUiState, card: StoryCard, actions: PairingActions) {
    val colors = CompanionTheme.colors
    val step = state.screen.step
    Column(Modifier.fillMaxSize().background(colors.bg).safeDrawingPadding()) {
        Row(
            Modifier.fillMaxWidth().heightIn(min = 56.dp).padding(horizontal = 16.dp, vertical = 4.dp),
            verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(10.dp)
        ) {
            val back = card.back
            if (back != null) SquareIconButton(Icons.AutoMirrored.Rounded.ArrowBack, "Balik", back, enabled = !state.busy)
            if (state.setup) {
                Row(Modifier.weight(1f).clearAndSetSemantics {}, horizontalArrangement = Arrangement.spacedBy(4.dp)) {
                    (1..5).forEach { n ->
                        val fill = when {
                            step > n -> colors.okFg
                            step == n -> colors.text
                            else -> colors.surface2
                        }
                        Box(Modifier.weight(1f).height(4.dp).clip(RoundedCornerShape(2.dp)).background(fill))
                    }
                }
                Text(
                    when (step) {
                        0 -> "Halo"
                        6 -> "Beres"
                        else -> "$step/5"
                    },
                    style = CompanionType.meta.copy(fontWeight = FontWeight(650)), color = colors.text2,
                    modifier = Modifier.semantics { if (step in 1..5) contentDescription = "Langkah $step dari 5" }
                )
            } else {
                Text("Sambungan", style = CompanionType.cardTitle, color = colors.text, modifier = Modifier.weight(1f))
            }
        }
        val shape = RoundedCornerShape(28.dp)
        Column(
            Modifier.weight(1f).padding(start = 16.dp, end = 16.dp, top = 4.dp, bottom = 16.dp).fillMaxWidth()
                .clip(shape).background(colors.surface).border(1.dp, colors.border, shape)
        ) {
            Column(Modifier.weight(1f).verticalScroll(rememberScrollState())) {
                val (bg, fg) = toneColors(card.tone)
                Box(Modifier.fillMaxWidth().height(120.dp).background(bg).clearAndSetSemantics {}, contentAlignment = Alignment.Center) {
                    Box(Modifier.size(72.dp).clip(RoundedCornerShape(24.dp)).background(colors.surface), contentAlignment = Alignment.Center) {
                        Icon(card.icon, null, tint = fg, modifier = Modifier.size(40.dp))
                    }
                }
                Column(Modifier.padding(18.dp), verticalArrangement = Arrangement.spacedBy(12.dp)) {
                    Text(card.title, style = CompanionType.onboardingTitle, color = colors.text, modifier = Modifier.semantics { heading() })
                    Text(card.lead, style = CompanionType.body, color = colors.text2)
                    card.body(this)
                    if (state.busy) Busy("Lagi ngecek ke server…")
                    if (state.screen != PairingScreen.RESULT) state.outcome?.let { OutcomeMessage(it) }
                }
            }
            if (card.primary != null || card.secondary != null) {
                Column(Modifier.fillMaxWidth().background(colors.surface).padding(horizontal = 18.dp, vertical = 12.dp)) {
                    FlowRow(Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.spacedBy(8.dp, Alignment.End), verticalArrangement = Arrangement.spacedBy(8.dp)) {
                        card.secondary?.let { (label, onClick) -> CompanionButton(label, onClick, kind = ButtonKind.GHOST, enabled = !state.busy) }
                        card.primary?.let { (label, onClick) -> CompanionButton(label, onClick, kind = ButtonKind.ACCENT, enabled = !state.busy, busy = state.busy) }
                    }
                }
            }
        }
    }
}

private fun storyCard(state: PairingUiState, actions: PairingActions, permissions: SetupPermissions, selfTest: SelfTestProgress): StoryCard {
    val setup = state.setup
    return when (state.screen) {
        PairingScreen.WELCOME -> StoryCard(
            Icons.Outlined.PhoneAndroid, StatusTone.OK, "Halo! Aku pendamping Cash Flow Tracker di HP-mu",
            "Aku jalan diam-diam di latar: nyatet notifikasi bank dan e-wallet, terus nanya kalau bingung. Uangmu tetap dibaca di web, bukan di sini.",
            primary = "Mulai" to { actions.show(PairingScreen.CHOOSE) }
        ) {
            Steps(
                "Sambungin ke akunmu pakai QR dari web.",
                "Kasih izin biar aku bisa denger notifikasi.",
                "Tes bentar, terus beres. Sekitar 2 menit."
            )
        }
        PairingScreen.CHOOSE -> StoryCard(
            Icons.Outlined.QrCodeScanner, StatusTone.MOVE, if (state.replacing) "Sambungin ulang, yuk" else "Sambungin ke akunmu",
            "Di laptop, buka web Cash Flow Tracker → Pengaturan → Hubungkan HP. Nanti muncul QR sama kode.",
            primary = "Scan QR" to actions.scan,
            secondary = "Ketik kode aja" to { actions.show(PairingScreen.CODE) },
            back = if (setup) ({ actions.show(PairingScreen.WELCOME) }) else actions.finish
        ) {
            DeviceLine(state.deviceName)
            CompanionButton("Pakai kunci API", { actions.show(PairingScreen.MANUAL) }, kind = ButtonKind.GHOST, icon = Icons.Outlined.Key)
            Hint("Izin kamera cuma diminta pas scan. Kameranya cuma buat baca kode.")
        }
        PairingScreen.CODE -> StoryCard(
            Icons.Outlined.Keyboard, StatusTone.MOVE, "Ketik kode dari web",
            "Kodenya ada di bawah QR. Berlaku 5 menit, sekali pakai.",
            primary = null, secondary = "Scan QR aja" to actions.scan, back = { actions.show(PairingScreen.CHOOSE) }
        ) {
            var code by remember { mutableStateOf("") }
            OutlinedTextField(
                value = code, onValueChange = { code = it }, enabled = !state.busy, singleLine = true,
                label = { Text("Kode") }, isError = state.codeError != null,
                supportingText = { Text(state.codeError ?: "Huruf besar atau kecil sama aja.") },
                keyboardOptions = KeyboardOptions(capitalization = KeyboardCapitalization.Characters, autoCorrectEnabled = false),
                modifier = Modifier.fillMaxWidth()
            )
            CompanionButton("Sambungin", { actions.typed(code) }, Modifier.fillMaxWidth(), ButtonKind.ACCENT, enabled = !state.busy && code.isNotBlank())
        }
        PairingScreen.CONFIRM -> StoryCard(
            Icons.Outlined.QrCodeScanner, StatusTone.MOVE, "Kodenya kebaca",
            "Sambungin HP ini ke akun yang nampilin kode tadi?",
            primary = "Hubungkan" to actions.confirm,
            secondary = "Batal" to { actions.show(PairingScreen.CHOOSE) }
        ) {
            DeviceLine(state.deviceName)
            if (state.replacing) Hint("Pemasangan ulang langsung mengganti kunci lama perangkat ini. Kalau sambungan putus, bikin kode baru di web.")
        }
        PairingScreen.MANUAL -> StoryCard(
            Icons.Outlined.Key, StatusTone.WARN, "Pakai kunci API",
            "Cadangan kalau QR dan kode nggak bisa. Bikin kuncinya di web: Pengaturan → Kunci API. Aku cek dulu sebelum dipakai.",
            primary = null, back = { if (setup || state.replacing) actions.show(PairingScreen.CHOOSE) else actions.finish() }
        ) {
            // A key is intentionally not rememberSaveable: it must not enter saved instance state.
            var key by remember { mutableStateOf("") }
            OutlinedTextField(
                value = key, onValueChange = { key = it }, enabled = !state.busy,
                label = { Text("API key") }, singleLine = true,
                visualTransformation = PasswordVisualTransformation(), modifier = Modifier.fillMaxWidth()
            )
            CompanionButton("Tes Koneksi", { actions.manual(key) }, Modifier.fillMaxWidth(), enabled = !state.busy && key.isNotBlank(), busy = state.busy)
            Hint("Kunci lama baru aku hapus setelah yang baru berhasil masuk ke akun yang sama.")
        }
        PairingScreen.RESULT -> {
            val outcome = state.outcome
            val connected = outcome?.connected == true
            val keySaved = outcome?.keySaved == true && !connected
            StoryCard(
                if (connected) Icons.Rounded.Check else if (keySaved) Icons.Outlined.CloudOff else Icons.Outlined.WarningAmber,
                if (connected) StatusTone.OK else StatusTone.WARN,
                when {
                    connected -> "Terhubung"
                    keySaved -> "Hampir. Sambungannya belum bisa dicek"
                    else -> "Belum terhubung"
                },
                when {
                    connected -> "HP ini udah nyambung ke akunmu."
                    keySaved -> "Kodenya udah kepakai dan kuncinya udah aku simpan. Nggak perlu scan ulang, tinggal tes lagi."
                    else -> "Coba lagi pakai QR atau kode yang baru."
                },
                primary = when {
                    connected -> (if (setup) "Lanjut" else "Selesai") to { if (setup) actions.show(PairingScreen.LISTENER) else actions.finish() }
                    keySaved -> "Tes Koneksi" to actions.verify
                    else -> "Pindai lagi" to actions.scan
                },
                secondary = if (!connected && !keySaved) "Ketik kode aja" to { actions.show(PairingScreen.CODE) } else null,
                back = if (connected || keySaved) null else ({ actions.show(PairingScreen.CHOOSE) })
            ) {
                outcome?.let { OutcomeMessage(it) }
                DeviceLine(state.deviceName)
                if (!connected && !keySaved) {
                    CompanionButton("Masukkan API key manual", { actions.show(PairingScreen.MANUAL) }, Modifier.fillMaxWidth(), ButtonKind.GHOST)
                }
            }
        }
        PairingScreen.LISTENER -> StoryCard(
            Icons.Outlined.Shield, StatusTone.OK, if (permissions.listener) "Sip, aku udah bisa denger" else "Izinin aku baca notifikasi",
            "Cuma dari aplikasi bank dan e-wallet ini. Chat, SMS, kode OTP, dan lainnya langsung aku buang, nggak disimpan.",
            primary = if (permissions.listener) "Lanjut" to { actions.show(PairingScreen.BATTERY) } else "Buka Akses Notifikasi" to actions.notificationAccess,
            secondary = if (permissions.listener) null else "Lewati dulu" to { actions.show(PairingScreen.BATTERY) }
        ) {
            AppBadges(permissions.financeApps)
            if (permissions.listener) Note("Akses notifikasi udah nyala.", StatusTone.OK, Icons.Rounded.Check)
            else Hint("Nanti kebuka pengaturan Android. Cari Cash Flow Tracker, nyalain, terus balik ke sini.")
        }
        PairingScreen.BATTERY -> StoryCard(
            Icons.Outlined.BatteryChargingFull, StatusTone.WARN, "Biar HP nggak nidurin aku",
            "Banyak HP matiin aplikasi di latar biar hemat baterai. Kalau aku ditidurin, notifikasi bisa kelewat.",
            primary = if (permissions.battery) "Lanjut" to { actions.show(PairingScreen.NOTIFICATIONS) }
                else "Kecualikan dari optimasi baterai" to actions.batteryAccess,
            secondary = if (permissions.battery) null else "Lewati dulu" to { actions.show(PairingScreen.NOTIFICATIONS) },
            back = { actions.show(PairingScreen.LISTENER) }
        ) {
            if (permissions.battery) Note("Udah boleh jalan di latar.", StatusTone.OK, Icons.Rounded.Check)
            permissions.oem?.let { Note("HP-mu ${it.brand} (${it.system}). ${it.system} masih bisa ngebekuin aku walau udah diizinin. ${it.steps}", StatusTone.WARN, Icons.Outlined.Info) }
            CompanionButton("Buka pengaturan aplikasi", actions.appInfo, Modifier.fillMaxWidth(), ButtonKind.GHOST, Icons.AutoMirrored.Outlined.OpenInNew)
            Hint("Pakai HP lain? Xiaomi, Redmi, POCO: Info aplikasi → Mulai otomatis, Penghemat baterai → Tanpa batasan. OPPO, realme: Penggunaan baterai → Izinkan aktivitas latar. Samsung: jangan masukin aku ke Aplikasi tidur.")
        }
        PairingScreen.NOTIFICATIONS -> StoryCard(
            Icons.Outlined.NotificationsActive, StatusTone.MOVE, "Boleh aku kirim notifikasi?",
            "Buat nanya kalau aku bingung, sama ngabarin yang udah kecatat. Nggak ada iklan.",
            primary = if (permissions.notifications || state.notificationsAnswered == false) "Lanjut" to { actions.show(PairingScreen.SELF_TEST) }
                else "Izinin" to actions.askNotifications,
            secondary = if (!permissions.notifications && state.notificationsAnswered == false) "Coba izinin lagi" to actions.askNotifications else null,
            back = { actions.show(PairingScreen.BATTERY) }
        ) {
            when {
                permissions.notifications -> Note("Sip, notifikasi nyala.", StatusTone.OK, Icons.Rounded.Check)
                state.notificationsAnswered == false -> Note("Oke. Pertanyaannya tetap ada di aplikasi, tapi kamu yang buka sendiri, ya.", StatusTone.MUTED, Icons.Outlined.Info)
            }
        }
        PairingScreen.SELF_TEST -> {
            val failed = selfTest.outcome?.failedStep
            StoryCard(
                Icons.Outlined.TaskAlt, StatusTone.OK, "Tes bentar, ya",
                "Aku kirim notifikasi tes ke diriku sendiri. Ini bukan transaksi, jadi nggak bakal kecatat.",
                primary = when {
                    selfTest.running -> null
                    failed == null -> "Mulai tes" to actions.runSelfTest
                    failed == SelfTestStep.POST -> "Izinin notifikasi" to actions.askNotifications
                    failed == SelfTestStep.CAPTURE -> "Nyalain akses" to actions.notificationAccess
                    else -> "Coba lagi" to actions.retryServer
                },
                secondary = when {
                    selfTest.running -> null
                    failed == SelfTestStep.POST -> "Lewati bagian ini" to actions.skipPost
                    failed != null -> "Tes ulang" to actions.runSelfTest
                    else -> null
                },
                back = if (selfTest.running) null else ({ actions.show(PairingScreen.NOTIFICATIONS) })
            ) {
                SelfTestChecklist(selfTest)
                when (failed) {
                    SelfTestStep.POST -> Note("Notifikasi tesnya nggak bisa muncul. Izin kirim notifikasi belum ada.", StatusTone.ASK, Icons.Outlined.WarningAmber)
                    SelfTestStep.CAPTURE -> Note("Notifikasinya muncul, tapi aku nggak bisa nangkep. Akses notifikasi masih mati, atau HP-mu nidurin aku.", StatusTone.ASK, Icons.Outlined.WarningAmber)
                    SelfTestStep.SERVER -> Note("Aku nggak bisa ngabarin web. Server nggak kejangkau, cek internetmu. Yang lain udah oke.", StatusTone.ASK, Icons.Outlined.WarningAmber)
                    null -> Unit
                }
            }
        }
        PairingScreen.READY -> StoryCard(
            Icons.Outlined.Celebration, StatusTone.OK, "HP-mu siap",
            "Mulai sekarang aku jalan di latar. Kamu nggak perlu buka aplikasi ini tiap hari.",
            primary = "Oke, sip" to actions.finish,
            secondary = "Buka web" to actions.openWeb
        ) {
            if (state.postSkipped) Note("Tes nangkep notifikasi dilewati. Aku cek sendiri pas notifikasi bank pertama masuk.", StatusTone.MUTED, Icons.Outlined.Info)
            Steps(
                "Kalau aku bingung, aku nanya lewat notifikasi.",
                "Makin sering kamu jawab, makin jarang aku nanya.",
                "Saldo dan laporan dibaca di web."
            )
        }
        PairingScreen.SCANNER -> StoryCard(Icons.Outlined.QrCodeScanner, StatusTone.MOVE, "", "", null)
    }
}

@Composable
private fun Steps(vararg lines: String) {
    val colors = CompanionTheme.colors
    Column(verticalArrangement = Arrangement.spacedBy(8.dp)) {
        lines.forEachIndexed { index, line ->
            Row(horizontalArrangement = Arrangement.spacedBy(10.dp), modifier = Modifier.semantics(mergeDescendants = true) {}) {
                Text("${index + 1}.", style = CompanionType.bodyStrong, color = colors.text)
                Text(line, style = CompanionType.body, color = colors.text)
            }
        }
    }
}

@OptIn(ExperimentalLayoutApi::class)
@Composable
private fun AppBadges(labels: List<String>) {
    val colors = CompanionTheme.colors
    if (labels.isEmpty()) {
        Hint(NO_FINANCE_APPS)
        return
    }
    FlowRow(horizontalArrangement = Arrangement.spacedBy(6.dp), verticalArrangement = Arrangement.spacedBy(6.dp)) {
        labels.forEach { label ->
            Text(
                label, style = CompanionType.meta.copy(fontWeight = FontWeight(650)), color = colors.text,
                modifier = Modifier.clip(RoundedCornerShape(999.dp)).background(colors.surface2).padding(horizontal = 10.dp, vertical = 4.dp)
            )
        }
    }
}

@Composable
private fun Hint(text: String) {
    Text(text, style = CompanionType.small, color = CompanionTheme.colors.text2)
}

@Composable
private fun Busy(text: String) {
    Text(text, style = CompanionType.body, color = CompanionTheme.colors.text2, modifier = Modifier.semantics { liveRegion = LiveRegionMode.Polite })
}

@Composable
private fun DeviceLine(name: String) {
    val colors = CompanionTheme.colors
    Row(
        Modifier.fillMaxWidth().clip(RoundedCornerShape(14.dp)).background(colors.surface2).padding(12.dp).semantics(mergeDescendants = true) {},
        verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(10.dp)
    ) {
        Icon(Icons.Outlined.PhoneAndroid, null, tint = colors.text)
        Column {
            Text(name, style = CompanionType.bodyStrong, color = colors.text)
            Text("Nama HP ini · kebaca otomatis", style = CompanionType.small, color = colors.text2)
        }
    }
}

@Composable
private fun OutcomeMessage(outcome: PairingOutcome) {
    val colors = CompanionTheme.colors
    Text(
        outcome.message, style = CompanionType.body,
        color = if (outcome.connected) colors.okFg else colors.danger,
        modifier = Modifier.semantics { liveRegion = LiveRegionMode.Polite }
    )
}
