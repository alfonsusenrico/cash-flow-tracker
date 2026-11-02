package com.alfonsusenrico.financialtracker.listener.ui.settings

import android.content.ActivityNotFoundException
import android.content.Context
import android.content.Intent
import android.net.Uri
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
import androidx.compose.material.icons.outlined.DarkMode
import androidx.compose.material.icons.outlined.NotificationsNone
import androidx.compose.material.icons.outlined.Shield
import androidx.compose.material.icons.rounded.Refresh
import androidx.compose.runtime.Composable
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.unit.dp
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.data.sync.AutoSync
import com.alfonsusenrico.financialtracker.listener.registry.FinancialSourceRegistry
import com.alfonsusenrico.financialtracker.listener.service.FinancialNotificationListenerService
import com.alfonsusenrico.financialtracker.listener.ui.components.CardTitle
import com.alfonsusenrico.financialtracker.listener.ui.components.CompanionButton
import com.alfonsusenrico.financialtracker.listener.ui.components.CompanionCard
import com.alfonsusenrico.financialtracker.listener.ui.components.PageHeader
import com.alfonsusenrico.financialtracker.listener.ui.model.SyncHealth
import com.alfonsusenrico.financialtracker.listener.ui.shell.CompanionViewModel
import com.alfonsusenrico.financialtracker.listener.ui.shell.ShellActions
import com.alfonsusenrico.financialtracker.listener.ui.theme.ThemeMode

/** What Atur shows besides the connection section, which manages its own pairing actions. */
data class SettingsState(
    val health: SyncHealth,
    val autoSync: Boolean,
    val apps: List<AppToggle>,
    val listenerPermissionGranted: Boolean,
    val batteryExempt: Boolean,
    val themeMode: ThemeMode
)

data class AppToggle(val packageName: String, val label: String, val enabled: Boolean)

class SettingsActions(
    val onAutoSync: (Boolean) -> Unit,
    val onSyncNow: () -> Unit,
    val onToggleApp: (packageName: String, enabled: Boolean) -> Unit,
    val onOpenListenerAccess: () -> Unit,
    val onOpenBattery: () -> Unit,
    val onOpenAppInfo: () -> Unit,
    val onThemeMode: (ThemeMode) -> Unit
)

@Composable
fun SettingsRoute(viewModel: CompanionViewModel, actions: ShellActions) {
    val context = LocalContext.current
    val prefs = remember { AppPreferences(context) }
    val connected by FinancialNotificationListenerService.isConnected.collectAsState()
    val themeMode by viewModel.themeMode.collectAsState()
    var autoSync by remember { mutableStateOf(prefs.isAutoSyncEnabled) }
    var apps by remember { mutableStateOf(appToggles(prefs)) }
    val state = SettingsState(
        health = SyncHealth.from(prefs, connected),
        autoSync = autoSync,
        apps = apps,
        listenerPermissionGranted = FinancialNotificationListenerService.isPermissionGranted(context),
        batteryExempt = (context.getSystemService(Context.POWER_SERVICE) as PowerManager).isIgnoringBatteryOptimizations(context.packageName),
        themeMode = themeMode
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
                actions.message("Sinkronisasi dimulai")
            },
            onToggleApp = { packageName, enabled ->
                prefs.setPackageEnabled(packageName, enabled)
                apps = appToggles(prefs)
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
            onThemeMode = viewModel::setThemeMode
        ),
        connection = { ConnectionSection(onSaved = viewModel::loadMetadata) }
    )
}

private fun appToggles(prefs: AppPreferences) = FinancialSourceRegistry.SELECTABLE_APPS.map {
    AppToggle(it.packageName, it.appLabel, prefs.isPackageEnabled(it.packageName))
}

/** Opens an Android settings page; some vendors drop a page, so a missing one is said, never ignored. */
private fun open(context: Context, actions: ShellActions, intent: Intent, fallback: Intent? = null) {
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

/** Atur: health, sending, monitored apps, connection, Android settings and appearance. */
@Composable
fun SettingsScreen(state: SettingsState, actions: SettingsActions, connection: @Composable () -> Unit) {
    Column(Modifier.fillMaxSize()) {
        PageHeader("Sinkron & atur", "Pencatat notifikasi")
        Column(
            Modifier.fillMaxSize().verticalScroll(rememberScrollState()).padding(start = 12.dp, end = 12.dp, top = 4.dp, bottom = 32.dp),
            verticalArrangement = Arrangement.spacedBy(10.dp)
        ) {
            HealthTiles(state.health)
            ListenerDetails(state.health)
            CompanionCard {
                SwitchRow(
                    "Kirim otomatis",
                    if (state.autoSync) "Notifikasi baru langsung dikirim" else "Mati: disimpan di ponsel dulu",
                    state.autoSync, actions.onAutoSync
                )
                CompanionButton("Sinkron sekarang", actions.onSyncNow, Modifier.fillMaxWidth(), icon = Icons.Rounded.Refresh)
            }
            CompanionCard(spacing = 6.dp) {
                CardTitle("Aplikasi yang dipantau", Icons.Outlined.NotificationsNone)
                SettingsNote("Notifikasi yang sama dari dua aplikasi satu bank dalam 2 menit dikirim sekali.")
                state.apps.forEach { app ->
                    SwitchRow(
                        app.label, if (app.enabled) "Dipantau" else "Tidak dipantau", app.enabled,
                        onChange = { actions.onToggleApp(app.packageName, it) }, detail = app.packageName.lowercase()
                    )
                }
            }
            connection()
            CompanionCard(spacing = 6.dp) {
                CardTitle("Pengaturan Android", Icons.Outlined.Shield)
                LinkRow("Akses notifikasi Android", if (state.listenerPermissionGranted) "Aktif" else "Belum aktif", actions.onOpenListenerAccess)
                LinkRow("Optimasi baterai", if (state.batteryExempt) "Dikecualikan" else "Belum dikecualikan", actions.onOpenBattery)
                LinkRow("Info aplikasi", "Izin latar dan peluncuran", actions.onOpenAppInfo)
                HiosNote()
            }
            CompanionCard {
                CardTitle("Tampilan", Icons.Outlined.DarkMode)
                ThemeChooser(state.themeMode, actions.onThemeMode)
            }
        }
    }
}
