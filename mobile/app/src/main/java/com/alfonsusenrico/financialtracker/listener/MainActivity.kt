package com.alfonsusenrico.financialtracker.listener

import android.Manifest
import android.content.Intent
import android.content.pm.PackageManager
import android.graphics.Color
import android.os.Build
import android.os.Bundle
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
import androidx.lifecycle.lifecycleScope
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.data.sync.QuestionRefresh
import com.alfonsusenrico.financialtracker.listener.data.sync.ReviewResultRefresh
import com.alfonsusenrico.financialtracker.listener.data.sync.SenderReplies
import com.alfonsusenrico.financialtracker.listener.data.textcapture.TextCaptures
import com.alfonsusenrico.financialtracker.listener.service.FinancialNotificationListenerService
import com.alfonsusenrico.financialtracker.listener.service.MappingConfirmationNotifier
import com.alfonsusenrico.financialtracker.listener.ui.capture.CaptureViewModel
import com.alfonsusenrico.financialtracker.listener.ui.shell.CompanionApp
import com.alfonsusenrico.financialtracker.listener.ui.shell.CompanionViewModel
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionTheme
import com.alfonsusenrico.financialtracker.listener.ui.theme.isDark
import kotlinx.coroutines.launch

class MainActivity : ComponentActivity() {

    private val viewModel: CompanionViewModel by viewModels()
    private val captureViewModel: CaptureViewModel by viewModels()
    private val isPermissionGrantedState = mutableStateOf(false)

    private val requestNotificationPermissionLauncher = registerForActivityResult(
        ActivityResultContracts.RequestPermission()
    ) { _ -> }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
            if (checkSelfPermission(Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED) {
                requestNotificationPermissionLauncher.launch(Manifest.permission.POST_NOTIFICATIONS)
            }
        }

        val prefs = AppPreferences(this)
        if (savedInstanceState == null && !prefs.isConfigured && !prefs.setupCompleted) {
            startActivity(Intent(this, PairingActivity::class.java).putExtra("setup", true))
        }
        openRequestedRow(intent)

        setContent {
            val themeMode by viewModel.themeMode.collectAsState()
            val dark = themeMode.isDark()
            DisposableEffect(dark) {
                val bars = if (dark) SystemBarStyle.dark(Color.TRANSPARENT) else SystemBarStyle.light(Color.TRANSPARENT, Color.TRANSPARENT)
                enableEdgeToEdge(statusBarStyle = bars, navigationBarStyle = bars)
                onDispose {}
            }
            CompanionTheme(dark = dark) {
                CompanionApp(viewModel = viewModel, captureViewModel = captureViewModel, listenerPermissionGranted = isPermissionGrantedState.value)
            }
        }
    }

    override fun onNewIntent(intent: Intent) {
        super.onNewIntent(intent)
        openRequestedRow(intent)
    }

    private fun openRequestedRow(intent: Intent?) {
        val rowId = intent?.getLongExtra(MappingConfirmationNotifier.EXTRA_ROW_ID, -1L) ?: -1L
        if (rowId >= 0) viewModel.requestOpen(rowId)
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
        val granted = FinancialNotificationListenerService.isPermissionGranted(this)
        isPermissionGrantedState.value = granted
        if (granted && !FinancialNotificationListenerService.isConnected.value) {
            android.service.notification.NotificationListenerService.requestRebind(
                android.content.ComponentName(this, FinancialNotificationListenerService::class.java)
            )
        }
    }
}
