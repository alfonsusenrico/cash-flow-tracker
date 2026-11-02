package com.alfonsusenrico.financialtracker.listener

import android.content.Intent
import android.graphics.Color
import android.net.ConnectivityManager
import android.net.Network
import android.net.NetworkCapabilities
import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.SystemBarStyle
import androidx.activity.compose.setContent
import androidx.activity.enableEdgeToEdge
import androidx.activity.viewModels
import androidx.compose.runtime.DisposableEffect
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.core.app.NotificationManagerCompat
import androidx.lifecycle.lifecycleScope
import com.alfonsusenrico.financialtracker.listener.data.health.HealthReports
import com.alfonsusenrico.financialtracker.listener.data.health.HealthSnapshots
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.data.reminder.EveningReminderNotifier
import com.alfonsusenrico.financialtracker.listener.data.sync.QuestionRefresh
import com.alfonsusenrico.financialtracker.listener.data.sync.ReviewResultRefresh
import com.alfonsusenrico.financialtracker.listener.data.sync.SenderReplies
import com.alfonsusenrico.financialtracker.listener.data.textcapture.TextCaptures
import com.alfonsusenrico.financialtracker.listener.service.FinancialNotificationListenerService
import com.alfonsusenrico.financialtracker.listener.service.MappingConfirmationNotifier
import com.alfonsusenrico.financialtracker.listener.ui.capture.CaptureViewModel
import com.alfonsusenrico.financialtracker.listener.ui.shell.CompanionApp
import com.alfonsusenrico.financialtracker.listener.ui.shell.CompanionViewModel
import com.alfonsusenrico.financialtracker.listener.ui.shell.PhoneState
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionTheme
import com.alfonsusenrico.financialtracker.listener.ui.theme.isDark
import kotlinx.coroutines.launch

class MainActivity : ComponentActivity() {

    private val viewModel: CompanionViewModel by viewModels()
    private val captureViewModel: CaptureViewModel by viewModels()
    private val phone = mutableStateOf(PhoneState())
    private val connectivity by lazy { getSystemService(ConnectivityManager::class.java) }
    private val networkWatch = object : ConnectivityManager.NetworkCallback() {
        override fun onAvailable(network: Network) = runOnUiThread { readPhone() }
        override fun onLost(network: Network) = runOnUiThread { readPhone() }
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        val prefs = AppPreferences(this)
        if (savedInstanceState == null && !prefs.isConfigured && !prefs.setupCompleted) {
            startActivity(Intent(this, PairingActivity::class.java).putExtra(PairingActivity.EXTRA_SETUP, true))
        }
        openRequested(intent)

        setContent {
            val themeMode by viewModel.themeMode.collectAsState()
            val dark = themeMode.isDark()
            DisposableEffect(dark) {
                val bars = if (dark) SystemBarStyle.dark(Color.TRANSPARENT) else SystemBarStyle.light(Color.TRANSPARENT, Color.TRANSPARENT)
                enableEdgeToEdge(statusBarStyle = bars, navigationBarStyle = bars)
                onDispose {}
            }
            CompanionTheme(dark = dark) {
                CompanionApp(viewModel = viewModel, captureViewModel = captureViewModel, phone = phone.value)
            }
        }
    }

    override fun onNewIntent(intent: Intent) {
        super.onNewIntent(intent)
        openRequested(intent)
    }

    /** A notification's tap: a row's question or Rincian, or the question sheet from the evening reminder. */
    private fun openRequested(intent: Intent?) {
        val rowId = intent?.getLongExtra(MappingConfirmationNotifier.EXTRA_ROW_ID, -1L) ?: -1L
        if (rowId >= 0) viewModel.requestOpen(rowId)
        else if (intent?.getBooleanExtra(EveningReminderNotifier.EXTRA_OPEN_QUESTIONS, false) == true) viewModel.requestQuestions()
    }

    override fun onStart() {
        super.onStart()
        try {
            connectivity.registerDefaultNetworkCallback(networkWatch)
        } catch (_: RuntimeException) {
            // Without the callback the online state is still read on every resume.
        }
    }

    override fun onStop() {
        try {
            connectivity.unregisterNetworkCallback(networkWatch)
        } catch (_: IllegalArgumentException) {
            // Never registered.
        }
        super.onStop()
    }

    override fun onResume() {
        super.onResume()
        lifecycleScope.launch { SenderReplies(this@MainActivity).recover() }
        lifecycleScope.launch { TextCaptures(this@MainActivity).resumeQueued() }
        lifecycleScope.launch {
            QuestionRefresh.refreshOpen(this@MainActivity)
            ReviewResultRefresh.refresh(this@MainActivity)
        }
        viewModel.loadMetadata()
        viewModel.refreshReport()
        HealthReports.request(this)
        readPhone()
        if (phone.value.notificationAccess && !FinancialNotificationListenerService.isConnected.value) {
            android.service.notification.NotificationListenerService.requestRebind(
                android.content.ComponentName(this, FinancialNotificationListenerService::class.java)
            )
        }
    }

    private fun readPhone() {
        val network = connectivity.activeNetwork?.let(connectivity::getNetworkCapabilities)
        phone.value = PhoneState(
            paired = AppPreferences(this).isConfigured,
            notificationAccess = FinancialNotificationListenerService.isPermissionGranted(this),
            batteryUnrestricted = HealthSnapshots.batteryUnrestricted(this),
            postNotifications = NotificationManagerCompat.from(this).areNotificationsEnabled(),
            online = network?.hasCapability(NetworkCapabilities.NET_CAPABILITY_INTERNET) == true
        )
    }
}
