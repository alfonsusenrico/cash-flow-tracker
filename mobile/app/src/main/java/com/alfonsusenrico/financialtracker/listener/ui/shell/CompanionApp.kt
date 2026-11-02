package com.alfonsusenrico.financialtracker.listener.ui.shell

import android.content.ActivityNotFoundException
import android.content.ComponentName
import android.content.Intent
import android.net.Uri
import android.os.Build
import android.provider.Settings
import android.service.notification.NotificationListenerService
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.navigationBarsPadding
import androidx.compose.foundation.layout.padding
import androidx.compose.material3.Snackbar
import androidx.compose.material3.SnackbarDuration
import androidx.compose.material3.SnackbarHost
import androidx.compose.material3.SnackbarHostState
import androidx.compose.material3.SnackbarResult
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.semantics.clearAndSetSemantics
import androidx.compose.ui.unit.dp
import com.alfonsusenrico.financialtracker.listener.PairingActivity
import com.alfonsusenrico.financialtracker.listener.data.health.SelfTestState
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.data.reminder.EveningReminder
import com.alfonsusenrico.financialtracker.listener.data.reminder.QuestionUrgency
import com.alfonsusenrico.financialtracker.listener.data.report.WebLinks
import com.alfonsusenrico.financialtracker.listener.service.FinancialNotificationListenerService
import com.alfonsusenrico.financialtracker.listener.ui.activity.ActivityRoute
import com.alfonsusenrico.financialtracker.listener.ui.capture.CaptureDetailRoute
import com.alfonsusenrico.financialtracker.listener.ui.capture.CaptureSheetHost
import com.alfonsusenrico.financialtracker.listener.ui.capture.CaptureViewModel
import com.alfonsusenrico.financialtracker.listener.ui.components.Formatting
import com.alfonsusenrico.financialtracker.listener.ui.detail.DetailRoute
import com.alfonsusenrico.financialtracker.listener.ui.hub.FixAction
import com.alfonsusenrico.financialtracker.listener.ui.hub.HubActions
import com.alfonsusenrico.financialtracker.listener.ui.hub.HubCounts
import com.alfonsusenrico.financialtracker.listener.ui.hub.HubInputs
import com.alfonsusenrico.financialtracker.listener.ui.hub.HubScreen
import com.alfonsusenrico.financialtracker.listener.ui.hub.HubStatus
import com.alfonsusenrico.financialtracker.listener.ui.hub.HubUiState
import com.alfonsusenrico.financialtracker.listener.ui.hub.OemHints
import com.alfonsusenrico.financialtracker.listener.ui.hub.ProblemKind
import com.alfonsusenrico.financialtracker.listener.ui.hub.SheetFrame
import com.alfonsusenrico.financialtracker.listener.ui.hub.SheetKind
import com.alfonsusenrico.financialtracker.listener.ui.hub.StatusFacts
import com.alfonsusenrico.financialtracker.listener.ui.hub.StatusSheet
import com.alfonsusenrico.financialtracker.listener.ui.hub.WebSheet
import com.alfonsusenrico.financialtracker.listener.ui.hub.rememberSheetStack
import com.alfonsusenrico.financialtracker.listener.ui.hub.webLinks
import com.alfonsusenrico.financialtracker.listener.ui.model.ActivityFilter
import com.alfonsusenrico.financialtracker.listener.ui.model.CapturePresentation
import com.alfonsusenrico.financialtracker.listener.ui.model.OpenQuestions
import com.alfonsusenrico.financialtracker.listener.ui.model.SyncHealth
import com.alfonsusenrico.financialtracker.listener.ui.questions.HubQuestion
import com.alfonsusenrico.financialtracker.listener.ui.questions.QuestionsRoute
import com.alfonsusenrico.financialtracker.listener.ui.settings.SettingsRoute
import com.alfonsusenrico.financialtracker.listener.ui.settings.open
import com.alfonsusenrico.financialtracker.listener.ui.settings.selfTestMessage
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionTheme
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.launch

/** Shell-level actions every screen shares: messages, and delete with undo. */
class ShellActions(
    val message: (String) -> Unit,
    val deleteWithUndo: (Long) -> Unit
)

/** What Android says about this phone, read again whenever the app comes back to the front. */
data class PhoneState(
    val paired: Boolean = true,
    val notificationAccess: Boolean = true,
    val batteryUnrestricted: Boolean = true,
    val postNotifications: Boolean = true,
    val online: Boolean = true
)

/**
 * The companion: the status hub (direction C) with every other screen as a sheet over it, and the Catat sheet.
 * There is no bottom bar.
 */
@Composable
fun CompanionApp(
    viewModel: CompanionViewModel,
    captureViewModel: CaptureViewModel,
    phone: PhoneState,
    now: () -> Long = System::currentTimeMillis,
    listener: StateFlow<Boolean> = FinancialNotificationListenerService.isConnected
) {
    val context = LocalContext.current
    val colors = CompanionTheme.colors
    val prefs = remember { AppPreferences(context) }
    val lists by viewModel.lists.collectAsState()
    val captureRows by captureViewModel.visible.collectAsState()
    val catatOpen by captureViewModel.sheetOpen.collectAsState()
    val accounts by viewModel.accounts.collectAsState()
    val categories by viewModel.categories.collectAsState()
    val openRequest by viewModel.openRequest.collectAsState()
    val report by viewModel.readyReport.collectAsState()
    val selfTest by viewModel.selfTest.progress.collectAsState()
    val connected by listener.collectAsState()
    val snackbar = remember { SnackbarHostState() }
    val scope = rememberCoroutineScope()
    val sheets = rememberSheetStack()
    var note by remember { mutableStateOf<String?>(null) }

    val actions = remember(viewModel) {
        ShellActions(
            message = { text ->
                scope.launch {
                    snackbar.currentSnackbarData?.dismiss()
                    snackbar.showSnackbar(text, withDismissAction = true, duration = SnackbarDuration.Long)
                }
            },
            deleteWithUndo = { rowId ->
                viewModel.deleteFromPhone(rowId) { removed, previous ->
                    if (removed && previous != null) scope.launch {
                        snackbar.currentSnackbarData?.dismiss()
                        val result = snackbar.showSnackbar(
                            "Dihapus dari aplikasi", actionLabel = "Urungkan", withDismissAction = true,
                            duration = SnackbarDuration.Long
                        )
                        if (result == SnackbarResult.ActionPerformed) viewModel.undoDelete(previous)
                    }
                }
            }
        )
    }

    fun openWeb(path: String) {
        val url = WebLinks.url(path) ?: return
        try {
            context.startActivity(Intent(Intent.ACTION_VIEW, Uri.parse(url)).addFlags(Intent.FLAG_ACTIVITY_NEW_TASK))
        } catch (_: ActivityNotFoundException) {
            actions.message("Nggak ada browser di HP ini. Buka $url di laptop.")
        }
    }

    fun openCapture(item: CapturePresentation) {
        if (item.question != null) captureViewModel.open(item.id) else sheets.openDetail(SheetKind.CAPTURE_DETAIL, item.id)
    }

    val reminderLabel = if (prefs.isEveningReminderEnabled) EveningReminder.label(prefs.eveningReminderMinute) else null
    val questions = remember(lists.questions) { QuestionUrgency.ordered(lists.questions, OpenQuestions::kindOf) }
    val today = Formatting.localDate(now())
    val counts = remember(lists, captureRows, today) { HubCounts.of(lists, captureRows, today) }
    val status = HubStatus.of(
        HubInputs(
            paired = phone.paired, notificationAccess = phone.notificationAccess, listenerConnected = connected,
            listenerDisconnectedAt = prefs.listenerDisconnectedAt, listenerConnectedAt = prefs.listenerConnectedAt,
            serverContact = prefs.serverContact, online = phone.online, autoSync = prefs.isAutoSyncEnabled,
            batteryUnrestricted = phone.batteryUnrestricted, postNotifications = phone.postNotifications,
            capturedToday = counts.capturedToday, lastCaptureApp = counts.lastCaptureApp, lastCaptureAt = counts.lastCaptureAt,
            lastSyncAt = prefs.lastSyncTime, waiting = counts.waiting, oldestWaitingAt = counts.oldestWaitingAt,
            selfTestPassedAt = if (prefs.selfTestStatus == SelfTestState.PASSED) prefs.selfTestAt else 0L,
            oem = OemHints.of(Build.MANUFACTURER), now = now()
        )
    )

    fun fix(action: FixAction) {
        when (action) {
            FixAction.PAIR -> context.startActivity(Intent(context, PairingActivity::class.java))
            FixAction.OPEN_ACCESS -> open(context, actions, Intent(Settings.ACTION_NOTIFICATION_LISTENER_SETTINGS))
            FixAction.OPEN_BATTERY -> open(
                context, actions,
                Intent(Settings.ACTION_REQUEST_IGNORE_BATTERY_OPTIMIZATIONS, Uri.parse("package:${context.packageName}")),
                fallback = Intent(Settings.ACTION_APPLICATION_DETAILS_SETTINGS, Uri.parse("package:${context.packageName}"))
            )
            FixAction.OPEN_NOTIFICATION_SETTINGS -> open(
                context, actions,
                Intent(Settings.ACTION_APP_NOTIFICATION_SETTINGS).putExtra(Settings.EXTRA_APP_PACKAGE, context.packageName)
            )
            FixAction.RECONNECT_LISTENER -> {
                NotificationListenerService.requestRebind(ComponentName(context, FinancialNotificationListenerService::class.java))
                actions.message("Aku coba nyambung lagi. Kalau masih putus, matiin lalu nyalain akses notifikasi.")
            }
            FixAction.RETRY_SERVER, FixAction.SEND_NOW -> {
                viewModel.syncNow()
                viewModel.reportHealthNow()
                actions.message("Lagi ngirim yang nunggu")
            }
            FixAction.RUN_SELF_TEST -> viewModel.runSelfTest { outcome ->
                if (outcome == null) return@runSelfTest
                if (outcome.passed && status.problem?.kind == ProblemKind.QUIET_DAY) note = HubStatus.QUIET_DAY_PASSED
                else actions.message(selfTestMessage(outcome))
            }
        }
    }

    LaunchedEffect(openRequest) {
        val request = openRequest ?: return@LaunchedEffect
        if (request.toQuestions) sheets.open(SheetKind.QUESTIONS, request.rowId) else sheets.openDetail(SheetKind.DETAIL, request.rowId)
        viewModel.consumeOpenRequest()
    }

    // The hub's top question needs its source-pocket options while no sheet has taken the focus.
    val topId = questions.firstOrNull()?.id
    LaunchedEffect(sheets.isOpen, topId) { if (!sheets.isOpen) viewModel.focusRow(topId) }

    Box(Modifier.fillMaxSize()) {
        Box(Modifier.fillMaxSize().then(if (sheets.isOpen || catatOpen) Modifier.clearAndSetSemantics {} else Modifier)) {
            HubScreen(
                state = HubUiState(
                    status = status, fixing = selfTest.running, note = note, report = report,
                    questionCount = if (lists.loaded) questions.size else null,
                    lightCount = questions.count { QuestionUrgency.isLight(OpenQuestions.kindOf(it)) },
                    reminderLabel = reminderLabel
                ),
                actions = HubActions(
                    onFix = ::fix,
                    onStatusDetails = { sheets.open(SheetKind.STATUS) },
                    onCatat = { captureViewModel.open() },
                    onOpenReport = { stored -> viewModel.openReport(stored)?.let { openWeb(stored.url) } },
                    onAllQuestions = { sheets.open(SheetKind.QUESTIONS) },
                    onLog = { sheets.open(SheetKind.LOG) },
                    onSettings = { sheets.open(SheetKind.SETTINGS) },
                    onWeb = { sheets.open(SheetKind.WEB) }
                ),
                topQuestion = {
                    questions.firstOrNull()?.let { row ->
                        HubQuestion(viewModel, actions, row, reminderLabel, onOpenDetail = { sheets.openDetail(SheetKind.DETAIL, it) })
                    }
                }
            )
        }

        sheets.top?.let { entry ->
            val previous = sheets.entries.getOrNull(sheets.entries.lastIndex - 1)
            Box(Modifier.fillMaxSize().then(if (catatOpen) Modifier.clearAndSetSemantics {} else Modifier)) {
                SheetFrame(entry, previous, onBack = sheets::back, onClose = sheets::close) {
                    when (entry.kind) {
                        SheetKind.QUESTIONS -> QuestionsRoute(
                            viewModel = viewModel, actions = actions, requestedRowId = entry.id.takeIf { it >= 0 },
                            reminderLabel = reminderLabel,
                            onOpenDetail = { sheets.openDetail(SheetKind.DETAIL, it) }, onHome = sheets::close
                        )
                        SheetKind.LOG -> ActivityRoute(
                            viewModel = viewModel, captureViewModel = captureViewModel, actions = actions,
                            onOpenRow = { row, toQuestions ->
                                if (toQuestions) sheets.open(SheetKind.QUESTIONS, row) else sheets.openDetail(SheetKind.DETAIL, row)
                            },
                            onOpenCapture = ::openCapture
                        )
                        SheetKind.DETAIL -> DetailRoute(
                            viewModel = viewModel, actions = actions, rowId = entry.id,
                            onBack = { if (previous != null) sheets.back() else sheets.close() },
                            onOpenQuestion = { sheets.open(SheetKind.QUESTIONS, it) },
                            onOpenLedger = { openWeb(WebLinks.LEDGER) }
                        )
                        SheetKind.CAPTURE_DETAIL -> CaptureDetailRoute(
                            viewModel = captureViewModel, accounts = accounts, categories = categories, actions = actions,
                            captureId = entry.id, onBack = { if (previous != null) sheets.back() else sheets.close() }
                        )
                        SheetKind.SETTINGS -> SettingsRoute(viewModel = viewModel, actions = actions)
                        SheetKind.STATUS -> StatusSheet(
                            facts = StatusFacts(
                                health = SyncHealth.from(prefs, connected),
                                notificationAccess = phone.notificationAccess,
                                batteryUnrestricted = phone.batteryUnrestricted,
                                postNotifications = phone.postNotifications,
                                selfTestLine = selfTestLine(prefs)
                            ),
                            selfTest = selfTest,
                            onOpenAccess = { fix(FixAction.OPEN_ACCESS) },
                            onOpenBattery = { fix(FixAction.OPEN_BATTERY) },
                            onOpenNotifications = { fix(FixAction.OPEN_NOTIFICATION_SETTINGS) },
                            onSelfTest = { fix(FixAction.RUN_SELF_TEST) }
                        )
                        SheetKind.WEB -> WebSheet(webLinks(report?.url)) { openWeb(it.path) }
                    }
                }
            }
        }

        if (catatOpen) {
            CaptureSheetHost(
                viewModel = captureViewModel,
                accounts = accounts,
                categories = categories,
                onOpenQueue = {
                    viewModel.setFilter(ActivityFilter.LOCAL)
                    sheets.open(SheetKind.LOG)
                },
                onOpenDetail = { sheets.openDetail(SheetKind.CAPTURE_DETAIL, it) }
            )
        }

        SnackbarHost(snackbar, Modifier.align(Alignment.BottomCenter).navigationBarsPadding().padding(12.dp)) { data ->
            Snackbar(
                data, containerColor = colors.snackbarBg, contentColor = colors.snackbarFg,
                actionColor = colors.snackbarAction, dismissActionContentColor = colors.snackbarFg
            )
        }
    }
}

/** "Tes terakhir" in Rincian status: the self-test's result and when it ran. */
private fun selfTestLine(prefs: AppPreferences): String {
    val at = prefs.selfTestAt.takeIf { it > 0 }?.let { " · ${Formatting.dateTime(it)}" }.orEmpty()
    return when (prefs.selfTestStatus) {
        SelfTestState.PASSED -> "Lulus$at"
        SelfTestState.FAILED -> "Gagal: " + when (prefs.selfTestFailedStep) {
            "post" -> "nampilin notifikasi"
            "capture" -> "nangkep notifikasi"
            else -> "ngabarin web"
        } + at
        else -> "Belum pernah"
    }
}
