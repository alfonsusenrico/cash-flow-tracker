package com.alfonsusenrico.financialtracker.listener.ui.settings

import android.app.TimePickerDialog
import android.content.ActivityNotFoundException
import android.content.Context
import android.content.Intent
import android.net.Uri
import android.os.Build
import android.os.PowerManager
import android.provider.Settings
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.outlined.Bedtime
import androidx.compose.material.icons.outlined.DarkMode
import androidx.compose.material.icons.outlined.Info
import androidx.compose.material.icons.outlined.NotificationsNone
import androidx.compose.material.icons.outlined.Shield
import androidx.compose.material.icons.rounded.Refresh
import androidx.compose.runtime.Composable
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableIntStateOf
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.unit.dp
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.compose.LifecycleEventEffect
import com.alfonsusenrico.financialtracker.listener.BuildConfig
import com.alfonsusenrico.financialtracker.listener.data.health.SelfTestOutcome
import com.alfonsusenrico.financialtracker.listener.data.health.SelfTestProgress
import com.alfonsusenrico.financialtracker.listener.data.health.SelfTestStep
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.data.reminder.EveningReminder
import com.alfonsusenrico.financialtracker.listener.ui.hub.OemHint
import com.alfonsusenrico.financialtracker.listener.ui.hub.OemHints
import com.alfonsusenrico.financialtracker.listener.data.sync.AutoSync
import com.alfonsusenrico.financialtracker.listener.registry.InstalledPackages
import com.alfonsusenrico.financialtracker.listener.service.FinancialNotificationListenerService
import com.alfonsusenrico.financialtracker.listener.ui.components.CardTitle
import com.alfonsusenrico.financialtracker.listener.ui.components.CompanionButton
import com.alfonsusenrico.financialtracker.listener.ui.components.CompanionCard
import com.alfonsusenrico.financialtracker.listener.ui.shell.CompanionViewModel
import com.alfonsusenrico.financialtracker.listener.ui.shell.ShellActions
import com.alfonsusenrico.financialtracker.listener.ui.theme.ThemeMode

/** What Pengaturan shows besides the connection section, which manages its own pairing actions. */
data class SettingsState(
    val autoSync: Boolean,
    val apps: List<AppToggle>,
    val listenerPermissionGranted: Boolean,
    val batteryExempt: Boolean,
    val themeMode: ThemeMode,
    val reminderEnabled: Boolean = true,
    val reminderMinute: Int = AppPreferences.DEFAULT_REMINDER_MINUTE,
    val oemHint: OemHint? = null,
    val selfTest: SelfTestProgress = SelfTestProgress(),
    val version: String = BuildConfig.VERSION_NAME
)

data class AppToggle(val packageName: String, val label: String, val enabled: Boolean)

class SettingsActions(
    val onAutoSync: (Boolean) -> Unit,
    val onSyncNow: () -> Unit,
    val onToggleApp: (packageName: String, enabled: Boolean) -> Unit,
    val onOpenListenerAccess: () -> Unit,
    val onOpenBattery: () -> Unit,
    val onOpenAppInfo: () -> Unit,
    val onThemeMode: (ThemeMode) -> Unit,
    val onReminder: (Boolean) -> Unit = {},
    val onReminderTime: () -> Unit = {},
    val onSelfTest: () -> Unit = {}
)

@Composable
fun SettingsRoute(viewModel: CompanionViewModel, actions: ShellActions) {
    val context = LocalContext.current
    val prefs = remember { AppPreferences(context) }
    val reminder = remember { EveningReminder(context) }
    val themeMode by viewModel.themeMode.collectAsState()
    val selfTest by viewModel.selfTest.progress.collectAsState()
    var autoSync by remember { mutableStateOf(prefs.isAutoSyncEnabled) }
    val installed = remember { InstalledPackages(context) }
    var apps by remember { mutableStateOf(appToggles(prefs, installed)) }
    // Read again on every return, so an app installed while Pengaturan is open appears.
    LifecycleEventEffect(Lifecycle.Event.ON_RESUME) { apps = appToggles(prefs, installed) }
    var reminderOn by remember { mutableStateOf(prefs.isEveningReminderEnabled) }
    var reminderMinute by remember { mutableIntStateOf(prefs.eveningReminderMinute) }
    val state = SettingsState(
        autoSync = autoSync,
        apps = apps,
        listenerPermissionGranted = FinancialNotificationListenerService.isPermissionGranted(context),
        batteryExempt = (context.getSystemService(Context.POWER_SERVICE) as PowerManager).isIgnoringBatteryOptimizations(context.packageName),
        themeMode = themeMode,
        reminderEnabled = reminderOn,
        reminderMinute = reminderMinute,
        oemHint = OemHints.of(Build.MANUFACTURER),
        selfTest = selfTest
    )
    SettingsScreen(
        state = state,
        actions = SettingsActions(
            onAutoSync = {
                autoSync = it
                AutoSync.setEnabled(context, it)
            },
            onSyncNow = {
                viewModel.syncNow()
                actions.message("Lagi ngirim yang nunggu")
            },
            onToggleApp = { packageName, enabled ->
                prefs.setPackageEnabled(packageName, enabled)
                apps = appToggles(prefs, installed)
            },
            onOpenListenerAccess = { open(context, actions, Intent(Settings.ACTION_NOTIFICATION_LISTENER_SETTINGS)) },
            onOpenBattery = {
                open(
                    context, actions,
                    Intent(Settings.ACTION_REQUEST_IGNORE_BATTERY_OPTIMIZATIONS, Uri.parse("package:${context.packageName}")),
                    fallback = Intent(Settings.ACTION_IGNORE_BATTERY_OPTIMIZATION_SETTINGS)
                )
            },
            onOpenAppInfo = {
                open(context, actions, Intent(Settings.ACTION_APPLICATION_DETAILS_SETTINGS, Uri.parse("package:${context.packageName}")))
            },
            onThemeMode = viewModel::setThemeMode,
            onReminder = {
                reminderOn = it
                reminder.setEnabled(it)
                actions.message(if (it) "Pengingat malam nyala, jam ${EveningReminder.label(reminderMinute)}" else "Pengingat malam mati. Pertanyaan ringan langsung ditanya.")
            },
            onReminderTime = {
                TimePickerDialog(context, { _, hour, minute ->
                    reminderMinute = hour * 60 + minute
                    reminder.setTime(reminderMinute)
                    actions.message("Pengingat malam jam ${EveningReminder.label(reminderMinute)}")
                }, reminderMinute / 60, reminderMinute % 60, true).show()
            },
            onSelfTest = {
                viewModel.runSelfTest { outcome -> outcome?.let { actions.message(selfTestMessage(it)) } }
            }
        ),
        connection = { ConnectionSection(onSaved = viewModel::loadMetadata) }
    )
}

private fun appToggles(prefs: AppPreferences, installed: InstalledPackages) = installed.financeApps().map {
    AppToggle(it.packageName, it.appLabel, prefs.isPackageEnabled(it.packageName))
}

/** Said in Pengaturan and setup when none of the registered finance apps is on this phone. */
const val NO_FINANCE_APPS = "Belum ada aplikasi bank atau e-wallet yang aku kenal di HP ini. Begitu kamu pasang, nanti muncul di sini."

/** Opens an Android settings page; some vendors drop a page, so a missing one is said, never ignored. */
fun open(context: Context, actions: ShellActions, intent: Intent, fallback: Intent? = null) {
    try {
        context.startActivity(intent)
    } catch (_: ActivityNotFoundException) {
        try {
            if (fallback == null) throw ActivityNotFoundException()
            context.startActivity(fallback)
        } catch (_: ActivityNotFoundException) {
            actions.message("Halaman pengaturan ini tidak tersedia di ponsel ini. Buka Pengaturan Android secara manual.")
        }
    }
}

/** What a finished self-test says, the same words as the setup card's checklist. */
fun selfTestMessage(outcome: SelfTestOutcome): String = when (outcome.failedStep) {
    null -> "Tes oke. Aku bisa nangkep notifikasi dan web udah dikabarin."
    SelfTestStep.POST -> "Notifikasi tesnya nggak bisa muncul. Nyalain izin notifikasi dulu."
    SelfTestStep.CAPTURE -> "Notifikasinya muncul, tapi aku nggak bisa nangkep. Cek akses notifikasi."
    SelfTestStep.SERVER -> "Aku nggak bisa ngabarin web. Cek internetmu, nanti aku coba lagi."
}

/** Pengaturan: plain grouped lists, as Android's own settings. */
@Composable
fun SettingsScreen(state: SettingsState, actions: SettingsActions, connection: @Composable () -> Unit) {
    Column(
        Modifier.fillMaxSize().verticalScroll(rememberScrollState()).padding(start = 12.dp, end = 12.dp, top = 4.dp, bottom = 32.dp),
        verticalArrangement = Arrangement.spacedBy(10.dp)
    ) {
        CompanionCard(spacing = 6.dp) {
            CardTitle("Aplikasi yang dipantau", Icons.Outlined.NotificationsNone)
            if (state.apps.isEmpty()) {
                SettingsNote(NO_FINANCE_APPS)
            } else {
                SettingsNote("Cuma dari aplikasi ini. Chat, SMS, dan kode OTP langsung dibuang. Notifikasi yang sama dari dua aplikasi satu bank dalam 2 menit dikirim sekali.")
            }
            state.apps.forEach { app ->
                SwitchRow(
                    app.label, if (app.enabled) "Dipantau" else "Tidak dipantau", app.enabled,
                    onChange = { actions.onToggleApp(app.packageName, it) }, detail = app.packageName.lowercase()
                )
            }
        }
        CompanionCard {
            CardTitle("Kirim", Icons.Rounded.Refresh)
            SwitchRow(
                "Kirim otomatis",
                if (state.autoSync) "Notifikasi baru langsung dikirim" else "Mati: disimpan di ponsel dulu",
                state.autoSync, actions.onAutoSync
            )
            CompanionButton("Sinkron sekarang", actions.onSyncNow, Modifier.fillMaxWidth(), icon = Icons.Rounded.Refresh)
        }
        CompanionCard(spacing = 6.dp) {
            CardTitle("Pengingat malam", Icons.Outlined.Bedtime)
            SwitchRow(
                "Pertanyaan ringan nunggu malam",
                if (state.reminderEnabled) "Satu pengingat jam ${EveningReminder.label(state.reminderMinute)}" else "Mati: pertanyaan ringan langsung ditanya",
                state.reminderEnabled, actions.onReminder
            )
            if (state.reminderEnabled) {
                LinkRow("Jam pengingat", EveningReminder.label(state.reminderMinute), actions.onReminderTime, "Ubah jam pengingat")
            }
            SettingsNote("“Apa ini?” dan tebakan kebiasaan nunggu pengingat. Pengirim, kantong, dan rekening tetap ditanya langsung.")
        }
        connection()
        CompanionCard(spacing = 6.dp) {
            CardTitle("Izin & baterai", Icons.Outlined.Shield)
            LinkRow("Akses notifikasi Android", if (state.listenerPermissionGranted) "Aktif" else "Belum aktif", actions.onOpenListenerAccess)
            LinkRow("Optimasi baterai", if (state.batteryExempt) "Dikecualikan" else "Belum dikecualikan", actions.onOpenBattery)
            LinkRow("Info aplikasi", "Izin latar dan peluncuran", actions.onOpenAppInfo)
            state.oemHint?.let { OemNote(it) } ?: HiosNote()
            SelfTestRow(state.selfTest, actions.onSelfTest)
        }
        CompanionCard {
            CardTitle("Tampilan", Icons.Outlined.DarkMode)
            ThemeChooser(state.themeMode, actions.onThemeMode)
        }
        CompanionCard(spacing = 6.dp) {
            CardTitle("Tentang", Icons.Outlined.Info)
            SettingsNote("Cash Flow Tracker untuk Android, versi ${state.version}")
            SettingsNote("Uangmu dibaca di web. HP ini cuma nyatet notifikasi bank dan e-wallet, terus nanya kalau bingung.")
        }
    }
}
