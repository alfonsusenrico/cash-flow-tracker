package com.alfonsusenrico.financialtracker.listener

import android.Manifest
import android.content.ActivityNotFoundException
import android.content.Intent
import android.content.pm.PackageManager
import android.graphics.Color
import android.net.Uri
import android.os.Bundle
import android.os.PowerManager
import android.provider.Settings
import androidx.activity.ComponentActivity
import androidx.activity.SystemBarStyle
import androidx.activity.compose.setContent
import androidx.activity.enableEdgeToEdge
import androidx.activity.result.contract.ActivityResultContracts
import androidx.activity.viewModels
import androidx.compose.runtime.DisposableEffect
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.service.FinancialNotificationListenerService
import com.alfonsusenrico.financialtracker.listener.ui.pairing.CameraScanner
import com.alfonsusenrico.financialtracker.listener.ui.pairing.PairingActions
import com.alfonsusenrico.financialtracker.listener.ui.pairing.PairingContent
import com.alfonsusenrico.financialtracker.listener.ui.pairing.PairingScreen
import com.alfonsusenrico.financialtracker.listener.ui.pairing.PairingViewModel
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionTheme
import com.alfonsusenrico.financialtracker.listener.ui.theme.ThemeMode
import com.alfonsusenrico.financialtracker.listener.ui.theme.isDark

class PairingActivity : ComponentActivity() {
    private val model: PairingViewModel by viewModels()
    private val listener = mutableStateOf(false)
    private val battery = mutableStateOf(false)
    private val cameraPermission = registerForActivityResult(ActivityResultContracts.RequestPermission()) { granted ->
        if (granted) model.show(PairingScreen.SCANNER) else model.cameraUnavailable()
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        if (savedInstanceState == null) {
            if (intent.action == Intent.ACTION_VIEW) model.accept(intent.dataString.orEmpty())
            else model.show(if (intent.getBooleanExtra("setup", false)) PairingScreen.LISTENER else PairingScreen.CHOOSE)
        }
        setContent {
            val state by model.state.collectAsState()
            val dark = ThemeMode.fromCode(AppPreferences(this).themeMode).isDark()
            DisposableEffect(dark, state.screen) {
                val bars = if (dark || state.screen == PairingScreen.SCANNER) SystemBarStyle.dark(Color.TRANSPARENT)
                    else SystemBarStyle.light(Color.TRANSPARENT, Color.TRANSPARENT)
                enableEdgeToEdge(statusBarStyle = bars, navigationBarStyle = bars)
                onDispose {}
            }
            CompanionTheme(dark = dark) {
                PairingContent(
                    state,
                    PairingActions(
                        show = model::show, scan = ::scan, confirm = model::exchange,
                        manual = model::manual, verify = model::verify,
                        finish = {
                            AppPreferences(this).setupCompleted = true
                            finish()
                        },
                        notificationAccess = { openSettings(Intent(Settings.ACTION_NOTIFICATION_LISTENER_SETTINGS)) },
                        batteryAccess = {
                            openSettings(Intent(Settings.ACTION_REQUEST_IGNORE_BATTERY_OPTIMIZATIONS, Uri.parse("package:$packageName")))
                        },
                        appInfo = { openSettings(Intent(Settings.ACTION_APPLICATION_DETAILS_SETTINGS, Uri.parse("package:$packageName"))) }
                    ),
                    listenerGranted = listener.value, batteryExempt = battery.value,
                    scanner = {
                        CameraScanner(model::accept, { model.show(PairingScreen.CHOOSE) },
                            { model.show(PairingScreen.MANUAL) }, model::cameraUnavailable)
                    }
                )
            }
        }
    }

    override fun onNewIntent(intent: Intent) {
        super.onNewIntent(intent)
        if (intent.action == Intent.ACTION_VIEW) model.accept(intent.dataString.orEmpty())
    }

    override fun onResume() {
        super.onResume()
        listener.value = FinancialNotificationListenerService.isPermissionGranted(this)
        battery.value = (getSystemService(POWER_SERVICE) as PowerManager).isIgnoringBatteryOptimizations(packageName)
    }

    private fun scan() {
        if (checkSelfPermission(Manifest.permission.CAMERA) == PackageManager.PERMISSION_GRANTED) model.show(PairingScreen.SCANNER)
        else cameraPermission.launch(Manifest.permission.CAMERA)
    }

    private fun openSettings(intent: Intent) {
        try {
            startActivity(intent)
        } catch (_: ActivityNotFoundException) {
            model.showSettingsError()
        }
    }
}
