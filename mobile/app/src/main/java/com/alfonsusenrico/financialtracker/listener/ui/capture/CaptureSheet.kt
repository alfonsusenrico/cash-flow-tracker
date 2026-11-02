package com.alfonsusenrico.financialtracker.listener.ui.capture

import android.content.Context
import android.net.ConnectivityManager
import android.net.NetworkCapabilities
import androidx.activity.compose.BackHandler
import androidx.compose.foundation.background
import androidx.compose.foundation.border
import androidx.compose.foundation.gestures.detectTapGestures
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.BoxWithConstraints
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.imePadding
import androidx.compose.foundation.layout.navigationBarsPadding
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.verticalScroll
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.outlined.AutoAwesome
import androidx.compose.material.icons.rounded.Close
import androidx.compose.material3.Icon
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableLongStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.input.pointer.pointerInput
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.semantics.heading
import androidx.compose.ui.semantics.paneTitle
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.unit.dp
import com.alfonsusenrico.financialtracker.listener.data.network.model.AccountItemResponse
import com.alfonsusenrico.financialtracker.listener.data.network.model.CategoryItemResponse
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.data.textcapture.CaptureAbilities
import com.alfonsusenrico.financialtracker.listener.data.textcapture.UndoWindow
import com.alfonsusenrico.financialtracker.listener.ui.components.SquareIconButton
import com.alfonsusenrico.financialtracker.listener.ui.model.CapturePresentation
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionTheme
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionType
import com.alfonsusenrico.financialtracker.listener.ui.theme.rememberReducedMotion
import kotlinx.coroutines.delay

const val CAPTURE_SHEET_TITLE = "Catat cepat"

/**
 * The capture sheet over the current tab. The shell hides the tab from TalkBack while it is open, and
 * back closes it. It ticks the clock only while an undo countdown runs.
 */
@Composable
fun CaptureSheetHost(
    viewModel: CaptureViewModel,
    accounts: List<AccountItemResponse>,
    categories: List<CategoryItemResponse>,
    onOpenQueue: () -> Unit,
    onOpenDetail: (Long) -> Unit
) {
    val context = LocalContext.current
    val capture by viewModel.active.collectAsState()
    val inFlight by viewModel.inFlight.collectAsState()
    val suggestions by viewModel.suggestions.collectAsState()
    val busy by viewModel.busy.collectAsState()
    val notice by viewModel.notice.collectAsState()
    val prefill by viewModel.prefill.collectAsState()
    val reducedMotion = rememberReducedMotion()
    val prefs = remember { AppPreferences(context) }
    var now by remember { mutableLongStateOf(System.currentTimeMillis()) }
    val deadline = capture?.takeIf { UndoWindow.isOpen(it, now) }?.undoDeadline
    LaunchedEffect(deadline) {
        while (deadline != null && now < deadline) {
            delay(if (reducedMotion) 1000 else 100)
            now = System.currentTimeMillis()
        }
    }
    LaunchedEffect(capture?.state, capture?.undoDeadline) { now = System.currentTimeMillis() }
    val presentation = remember(capture) { capture?.let(CapturePresentation::of) }
    val state = CaptureSheetState(
        capture = presentation,
        phase = sheetPhase(capture, capture?.id in inFlight, now),
        suggestions = suggestions,
        now = now,
        prefill = prefill,
        busy = busy,
        notice = notice?.takeIf { it.captureId == null || it.captureId == capture?.id }?.text,
        autoSync = prefs.isAutoSyncEnabled,
        online = remember(capture?.state) { isOnline(context) },
        reducedMotion = reducedMotion,
        canEdit = capture != null && CaptureAbilities.canEdit(capture!!, presentation?.view, prefs.pairingContext),
        accounts = accounts,
        categories = categories
    )
    val actions = remember(viewModel) {
        CaptureSheetActions(
            onClose = viewModel::close,
            onPrefilled = viewModel::prefilled,
            onSubmit = viewModel::submit,
            onAnother = viewModel::startAnother,
            onUndo = viewModel::undo,
            onEdit = viewModel::edit,
            onAnswer = viewModel::answer,
            onResendAnswer = viewModel::resendAnswer,
            onSend = viewModel::send,
            onRewrite = viewModel::rewrite,
            onOpenQueue = {
                viewModel.close()
                onOpenQueue()
            },
            onOpenDetail = { id ->
                viewModel.close()
                onOpenDetail(id)
            },
            // Read in the sheet itself: the tab's messages are hidden behind it.
            onInvalid = viewModel::say
        )
    }
    BackHandler(onBack = viewModel::close)
    CaptureSheetFrame(onClose = viewModel::close) { CaptureSheetContent(state, actions) }
}

/**
 * Scrim and a bottom sheet (`.f-scrim`, `.f-sheet`), at most 90% of the height and scrollable. Only the
 * scrim above the sheet closes it on a tap; TalkBack closes it with the close button or back.
 */
@Composable
fun CaptureSheetFrame(onClose: () -> Unit, content: @Composable () -> Unit) {
    val colors = CompanionTheme.colors
    val shape = RoundedCornerShape(topStart = 26.dp, topEnd = 26.dp)
    BoxWithConstraints(Modifier.fillMaxSize().background(Color.Black.copy(alpha = 0.5f))) {
        val sheetMax = maxHeight * 0.9f
        Column(Modifier.fillMaxSize()) {
            Box(Modifier.fillMaxWidth().weight(1f).pointerInput(onClose) { detectTapGestures { onClose() } })
            // A shaped background and border rather than a clip: the content keeps clear of the corners.
            Column(
                Modifier.fillMaxWidth().heightIn(max = sheetMax)
                    .background(colors.bg, shape)
                    .border(1.dp, colors.border, shape)
                    .semantics { paneTitle = CAPTURE_SHEET_TITLE }
                    .verticalScroll(rememberScrollState())
                    .navigationBarsPadding()
                    .imePadding()
                    .padding(start = 14.dp, end = 14.dp, top = 8.dp, bottom = 22.dp),
                verticalArrangement = Arrangement.spacedBy(12.dp)
            ) { content() }
        }
    }
}

/** The sheet's head and the screen of the current phase. */
@Composable
fun CaptureSheetContent(state: CaptureSheetState, actions: CaptureSheetActions) {
    val colors = CompanionTheme.colors
    Box(Modifier.fillMaxWidth(), contentAlignment = Alignment.Center) {
        Box(Modifier.size(width = 40.dp, height = 4.dp).clip(RoundedCornerShape(4.dp)).background(colors.borderStrong))
    }
    Row(verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(8.dp)) {
        Icon(Icons.Outlined.AutoAwesome, contentDescription = null, tint = colors.text, modifier = Modifier.size(18.dp))
        Text(CAPTURE_SHEET_TITLE, style = CompanionType.pageTitle, color = colors.text, modifier = Modifier.weight(1f).semantics { heading() })
        SquareIconButton(Icons.Rounded.Close, "Tutup catat cepat", actions.onClose)
    }
    when (state.phase) {
        SheetPhase.INPUT -> CaptureInput(state, actions)
        SheetPhase.PROCESSING -> CaptureProcessing(state.capture?.capture?.text.orEmpty(), state.reducedMotion)
        SheetPhase.RECORDED, SheetPhase.SAVED -> CaptureRecorded(state, actions)
        SheetPhase.QUESTION -> CaptureQuestionBody(state, actions)
        SheetPhase.QUEUED -> CaptureQueued(state, actions)
        SheetPhase.ERROR -> CaptureError(state, actions)
        SheetPhase.FAILED -> CaptureFailed(state, actions)
        SheetPhase.UNDONE, SheetPhase.DELETED -> CaptureClosed(state, actions)
    }
}

/** Whether Android reports a network that can reach the internet; used only to explain the queue. */
fun isOnline(context: Context): Boolean {
    val manager = context.getSystemService(ConnectivityManager::class.java) ?: return true
    return manager.getNetworkCapabilities(manager.activeNetwork)?.hasCapability(NetworkCapabilities.NET_CAPABILITY_INTERNET) == true
}
