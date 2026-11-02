package com.alfonsusenrico.financialtracker.listener

import android.Manifest
import android.content.ActivityNotFoundException
import android.content.Intent
import android.content.pm.PackageManager
import android.graphics.Color
import android.net.Uri
import android.os.Build
import android.os.Bundle
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
import androidx.core.app.NotificationManagerCompat
import com.alfonsusenrico.financialtracker.listener.data.health.HealthSnapshots
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.data.report.WebLinks
import com.alfonsusenrico.financialtracker.listener.registry.InstalledPackages
import com.alfonsusenrico.financialtracker.listener.service.FinancialNotificationListenerService
import com.alfonsusenrico.financialtracker.listener.ui.hub.OemHints
import com.alfonsusenrico.financialtracker.listener.ui.pairing.CameraScanner
import com.alfonsusenrico.financialtracker.listener.ui.pairing.PairingActions
import com.alfonsusenrico.financialtracker.listener.ui.pairing.PairingContent
import com.alfonsusenrico.financialtracker.listener.ui.pairing.PairingScreen
import com.alfonsusenrico.financialtracker.listener.ui.pairing.PairingViewModel
import com.alfonsusenrico.financialtracker.listener.ui.pairing.SetupPermissions
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionTheme
import com.alfonsusenrico.financialtracker.listener.ui.theme.ThemeMode
import com.alfonsusenrico.financialtracker.listener.ui.theme.isDark

class PairingActivity : ComponentActivity() {
    companion object {
        /** The full first-run story, from the welcome card to "HP-mu siap". */
        const val EXTRA_SETUP = "setup"
        /** Straight to the manual API key, from Pengaturan. */
        const val EXTRA_MANUAL = "manual"
    }

    private val model: PairingViewModel by viewModels()
    private val permissions = mutableStateOf(SetupPermissions())
    private val cameraPermission = registerForActivityResult(ActivityResultContracts.RequestPermission()) { granted ->
        if (granted) model.show(PairingScreen.SCANNER) else model.cameraUnavailable()
    }
    private val notificationPermission = registerForActivityResult(ActivityResultContracts.RequestPermission()) { granted ->
        model.notificationsAnswered(granted)
        readPermissions()
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        if (savedInstanceState == null) {
            model.start(intent.getBooleanExtra(EXTRA_SETUP, false), intent.getBooleanExtra(EXTRA_MANUAL, false))
            if (intent.action == Intent.ACTION_VIEW) model.accept(intent.dataString.orEmpty())
        }
        setContent {
            val state by model.state.collectAsState()
            val selfTest by model.selfTest.progress.collectAsState()
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
                        appInfo = { openSettings(Intent(Settings.ACTION_APPLICATION_DETAILS_SETTINGS, Uri.parse("package:$packageName"))) },
                        typed = model::typed,
                        askNotifications = ::askNotifications,
                        runSelfTest = model::runSelfTest,
                        retryServer = model::retryServer,
                        skipPost = model::skipPost,
                        openWeb = { WebLinks.url("/")?.let { openSettings(Intent(Intent.ACTION_VIEW, Uri.parse(it))) } }
                    ),
                    scanner = {
                        CameraScanner(model::accept, { model.show(PairingScreen.CHOOSE) },
                            { model.show(PairingScreen.CODE) }, model::cameraUnavailable)
                    },
                    permissions = permissions.value,
                    selfTest = selfTest
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
        readPermissions()
    }

    private fun readPermissions() {
        permissions.value = SetupPermissions(
            listener = FinancialNotificationListenerService.isPermissionGranted(this),
            battery = HealthSnapshots.batteryUnrestricted(this),
            notifications = NotificationManagerCompat.from(this).areNotificationsEnabled(),
            oem = OemHints.of(Build.MANUFACTURER),
            financeApps = InstalledPackages(this).financeApps().map { it.appLabel }.distinct()
        )
    }

    /** Android 13+ asks once in a dialog; older phones, or a refusal Android remembers, go to the app's page. */
    private fun askNotifications() {
        if (Build.VERSION.SDK_INT >= 33 && checkSelfPermission(Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED &&
            (model.state.value.notificationsAnswered == null || shouldShowRequestPermissionRationale(Manifest.permission.POST_NOTIFICATIONS))
        ) {
            notificationPermission.launch(Manifest.permission.POST_NOTIFICATIONS)
        } else {
            openSettings(Intent(Settings.ACTION_APP_NOTIFICATION_SETTINGS).putExtra(Settings.EXTRA_APP_PACKAGE, packageName))
        }
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
