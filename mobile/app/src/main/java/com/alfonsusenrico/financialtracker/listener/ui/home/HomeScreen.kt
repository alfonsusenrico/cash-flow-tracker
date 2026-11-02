package com.alfonsusenrico.financialtracker.listener.ui.home

import android.content.Intent
import android.provider.Settings
import androidx.compose.foundation.background
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.ExperimentalLayoutApi
import androidx.compose.foundation.layout.FlowRow
import androidx.compose.foundation.layout.IntrinsicSize
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxHeight
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.verticalScroll
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.outlined.Tune
import androidx.compose.material.icons.outlined.WarningAmber
import androidx.compose.material.icons.rounded.Check
import androidx.compose.material3.Icon
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.remember
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.semantics.clearAndSetSemantics
import androidx.compose.ui.semantics.contentDescription
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.service.FinancialNotificationListenerService
import com.alfonsusenrico.financialtracker.listener.ui.components.BentoTile
import com.alfonsusenrico.financialtracker.listener.ui.components.ButtonKind
import com.alfonsusenrico.financialtracker.listener.ui.components.CompanionButton
import com.alfonsusenrico.financialtracker.listener.ui.components.CompanionCard
import com.alfonsusenrico.financialtracker.listener.ui.components.Formatting
import com.alfonsusenrico.financialtracker.listener.ui.components.Note
import com.alfonsusenrico.financialtracker.listener.ui.components.PageHeader
import com.alfonsusenrico.financialtracker.listener.ui.components.SquareIconButton
import com.alfonsusenrico.financialtracker.listener.ui.capture.CaptureViewModel
import com.alfonsusenrico.financialtracker.listener.ui.model.ActivityFilter
import com.alfonsusenrico.financialtracker.listener.ui.model.CapturePresentation
import com.alfonsusenrico.financialtracker.listener.ui.model.HomeFigures
import com.alfonsusenrico.financialtracker.listener.ui.model.RowPresentation
import com.alfonsusenrico.financialtracker.listener.ui.model.StatusTone
import com.alfonsusenrico.financialtracker.listener.ui.model.SyncHealth
import com.alfonsusenrico.financialtracker.listener.ui.shell.CompanionViewModel
import com.alfonsusenrico.financialtracker.listener.ui.shell.LedgerSummaryState
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionTheme
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionType
import com.alfonsusenrico.financialtracker.listener.ui.theme.TABULAR_FIGURES
import java.time.LocalDate

data class HomeState(
    val today: LocalDate,
    val figures: HomeFigures,
    /** Null until Room has answered; like the other counts here it is never shown as 0 before that. */
    val questionCount: Int?,
    val nextQuestion: RowPresentation?,
    val health: SyncHealth,
    val needsCheck: Int?,
    val onPhone: Int?,
    val listenerPermissionGranted: Boolean,
    val ledger: LedgerSummaryState = LedgerSummaryState()
)

@Composable
fun HomeRoute(
    viewModel: CompanionViewModel,
    listenerPermissionGranted: Boolean,
    onOpenQuestions: (Long?) -> Unit,
    onOpenActivity: (ActivityFilter) -> Unit,
    onOpenSettings: () -> Unit,
    captureViewModel: CaptureViewModel? = null
) {
    val context = LocalContext.current
    val lists by viewModel.lists.collectAsState()
    val captureRows = captureViewModel?.visible?.collectAsState()?.value
    val ledgerState by viewModel.ledgerSummary.collectAsState()
    val ledger = viewModel.currentLedgerSummary(ledgerState)
    LaunchedEffect(viewModel) { viewModel.refreshLedgerSummary() }
    val connected by FinancialNotificationListenerService.isConnected.collectAsState()
    val today = LocalDate.now()
    val state = remember(lists, captureRows, connected, today, listenerPermissionGranted, ledger) {
        val captures = captureRows?.map(CapturePresentation::of)
        val loaded = lists.takeIf { it.loaded }
        HomeState(
            today = today,
            figures = HomeFigures.of(loaded?.let { it.visible + it.local + it.ignored }, today, captures),
            questionCount = loaded?.questions?.size,
            nextQuestion = lists.questions.firstOrNull()?.let(RowPresentation::of),
            health = SyncHealth.from(AppPreferences(context), connected),
            needsCheck = HomeFigures.needsCheck(loaded?.visible, captures),
            onPhone = HomeFigures.onPhone(loaded?.local, captures),
            listenerPermissionGranted = listenerPermissionGranted,
            ledger = ledger
        )
    }
    HomeScreen(
        state = state,
        onOpenQuestions = { onOpenQuestions(state.nextQuestion?.row?.id) },
        onOpenActivity = onOpenActivity,
        onOpenSettings = onOpenSettings,
        onEnableListener = { context.startActivity(Intent(Settings.ACTION_NOTIFICATION_LISTENER_SETTINGS)) },
        onRefreshLedger = viewModel::refreshLedgerSummary
    )
}

@Composable
fun HomeScreen(
    state: HomeState,
    onOpenQuestions: () -> Unit,
    onOpenActivity: (ActivityFilter) -> Unit,
    onOpenSettings: () -> Unit,
    onEnableListener: () -> Unit,
    onRefreshLedger: () -> Unit = {}
) {
    // Two columns stop fitting large text; the tiles then stack.
    val singleColumn = LocalDensity.current.fontScale >= 1.5f
    Column(Modifier.fillMaxSize()) {
        PageHeader("Kas", Formatting.dayMonth(state.today)) {
            SquareIconButton(Icons.Outlined.Tune, "Pengaturan", onOpenSettings)
        }
        Column(
            Modifier.fillMaxSize().verticalScroll(rememberScrollState()).padding(start = 12.dp, end = 12.dp, top = 6.dp, bottom = 24.dp),
            verticalArrangement = Arrangement.spacedBy(10.dp)
        ) {
            if (!state.listenerPermissionGranted) {
                CompanionCard(spacing = 10.dp) {
                    Note("Akses notifikasi belum aktif, jadi transaksi baru tidak tercatat.", StatusTone.WARN, Icons.Outlined.WarningAmber)
                    CompanionButton("Aktifkan akses notifikasi", onEnableListener, Modifier.fillMaxWidth(), ButtonKind.ACCENT)
                }
            }
            LedgerTile(state.ledger, onRefreshLedger)
            TopCategoriesTile(state.ledger)
            ActivityTile(state.figures, onClick = { onOpenActivity(ActivityFilter.ALL) })
            TilePair(
                singleColumn,
                { QuestionsTile(state, onOpenQuestions, it) },
                { SyncTile(state.health, onOpenSettings, it) }
            )
            TilePair(
                singleColumn,
                { CountTile("Perlu cek", state.needsCheck, "Belum tuntas di tracker", { onOpenActivity(ActivityFilter.PENDING) }, it) },
                { CountTile("Di ponsel", state.onPhone, "Belum dikirim", { onOpenActivity(ActivityFilter.LOCAL) }, it) }
            )
            Text(
                "Uang di Beranda hanya dari arus kas tracker. Hitungan lain di sini berasal dari notifikasi dan catatan di ponsel ini.",
                style = CompanionType.meta, color = CompanionTheme.colors.text2, modifier = Modifier.padding(horizontal = 4.dp)
            )
        }
    }
}

@Composable
private fun TilePair(singleColumn: Boolean, first: @Composable (Modifier) -> Unit, second: @Composable (Modifier) -> Unit) {
    if (singleColumn) {
        first(Modifier.fillMaxWidth())
        second(Modifier.fillMaxWidth())
    } else {
        Row(Modifier.fillMaxWidth().height(IntrinsicSize.Min), horizontalArrangement = Arrangement.spacedBy(10.dp)) {
            first(Modifier.weight(1f).fillMaxHeight())
            second(Modifier.weight(1f).fillMaxHeight())
        }
    }
}

@OptIn(ExperimentalLayoutApi::class)
@Composable
private fun LedgerTile(state: LedgerSummaryState, onRefresh: () -> Unit) {
    val colors = CompanionTheme.colors
    val summary = state.summary
    BentoTile("Arus kas tracker", Modifier.fillMaxWidth()) {
        if (summary == null) {
            val message = if (state.loading) "Memuat arus kas…" else state.error ?: "Arus kas belum tersedia."
            Text(message, style = CompanionType.small, color = colors.text2)
        } else {
            val outflow = Formatting.rupiah(summary.outflow!!)
            Column(Modifier.clearAndSetSemantics { contentDescription = "Uang keluar tracker $outflow" }) {
                Text("Uang keluar", style = CompanionType.small, color = colors.text2)
                Text(outflow, style = CompanionType.todayTotal, color = colors.text)
            }
            FlowRow(horizontalArrangement = Arrangement.spacedBy(14.dp), verticalArrangement = Arrangement.spacedBy(2.dp)) {
                Figure("Masuk", Formatting.rupiah(summary.inflow!!))
                Figure("Neto", Formatting.signedRupiah(summary.net!!))
                if (summary.reimbursementOffset!! > 0) {
                    Figure("Keluar kotor", Formatting.rupiah(summary.grossOutflow!!))
                    Figure("Dikurangi penggantian", Formatting.rupiah(summary.reimbursementOffset))
                }
            }
            Text(ledgerPeriod(summary), style = CompanionType.meta, color = colors.text2)
            Text(ledgerFreshness(state, summary), style = CompanionType.meta, color = colors.text2)
            if (state.error != null) Text(state.error, style = CompanionType.meta, color = colors.text2)
        }
        CompanionButton(
            if (state.loading) "Memperbarui arus kas…" else "Perbarui arus kas", onRefresh,
            Modifier.fillMaxWidth(), ButtonKind.GHOST, enabled = !state.loading
        )
    }
}

/**
 * A label and its value, spoken as one. The value moves below the label when both do not fit on one line,
 * and an amount ([wholeValue]) is never broken: at large text "Rp50.000" must not split across lines.
 */
@OptIn(ExperimentalLayoutApi::class)
@Composable
private fun Figure(label: String, value: String, wholeValue: Boolean = true) {
    val colors = CompanionTheme.colors
    FlowRow(Modifier.clearAndSetSemantics { contentDescription = "$label $value" }, horizontalArrangement = Arrangement.spacedBy(4.dp)) {
        Text(label, style = CompanionType.small.copy(fontSize = 14.sp), color = colors.text2, modifier = Modifier.alignByBaseline())
        Text(
            value, style = CompanionType.small.copy(fontSize = 14.sp, fontWeight = FontWeight(700), fontFeatureSettings = TABULAR_FIGURES),
            color = colors.text, softWrap = !wholeValue, modifier = Modifier.alignByBaseline()
        )
    }
}

/** This phone's activity today, as counts only: its money is in the tracker's summary above. */
@OptIn(ExperimentalLayoutApi::class)
@Composable
private fun ActivityTile(figures: HomeFigures, onClick: () -> Unit) {
    val colors = CompanionTheme.colors
    val captured = figures.capturedToday
    val waiting = figures.waitingToday
    val waitingCaptures = figures.waitingCapturesToday
    val shownCaptures = when (waitingCaptures) {
        null -> " · $UNREAD catatan"
        0 -> ""
        else -> " · $waitingCaptures catatan"
    }
    val spokenCaptures = when (waitingCaptures) {
        null -> ", catatan $UNREAD_SPOKEN"
        0 -> ""
        else -> ", $waitingCaptures catatan"
    }
    val description = "Aktivitas ponsel hari ini, " +
        (captured?.let { "$it notifikasi tertangkap" } ?: "notifikasi tertangkap $UNREAD_SPOKEN") +
        ". Menunggu " + (waiting?.let { "$it notifikasi" } ?: "notifikasi $UNREAD_SPOKEN") + spokenCaptures +
        ". $FROM_THIS_PHONE"
    BentoTile("Aktivitas ponsel hari ini", Modifier.fillMaxWidth(), onClick = onClick, description = description) {
        FlowRow(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
            Text(captured?.toString() ?: UNREAD, style = CompanionType.tileCount, color = colors.text, modifier = Modifier.alignByBaseline())
            Text("notifikasi tertangkap", style = CompanionType.small, color = colors.text2, modifier = Modifier.alignByBaseline())
        }
        // A count phrase, not an amount: it may wrap between words.
        Figure("Menunggu", "${waiting ?: UNREAD} notifikasi$shownCaptures", wholeValue = false)
        Text(FROM_THIS_PHONE, style = CompanionType.meta, color = colors.text2)
    }
}

/** Shown, and spoken, for a count not yet read from the phone's database; never 0. */
private const val UNREAD = "—"
private const val UNREAD_SPOKEN = "belum dimuat"
private const val FROM_THIS_PHONE = "Dari notifikasi dan catatan di ponsel ini"

@Composable
private fun QuestionsTile(state: HomeState, onOpen: () -> Unit, modifier: Modifier) {
    val colors = CompanionTheme.colors
    val next = state.nextQuestion
    if (state.questionCount == null) {
        BentoTile("Pertanyaan", modifier) {
            // The dash is not read aloud; the caption says the count is not known yet.
            Text(UNREAD, style = CompanionType.tileCount, color = colors.text, modifier = Modifier.clearAndSetSemantics {})
            Text("Belum dimuat", style = CompanionType.small, color = colors.text2)
        }
        return
    }
    if (state.questionCount == 0 || next == null) {
        BentoTile("Pertanyaan", modifier) {
            Text("0", style = CompanionType.tileCount, color = colors.text)
            Text("Semua terjawab", style = CompanionType.small, color = colors.text2)
        }
        return
    }
    val nextText = listOfNotNull(next.question?.title, next.amount?.let(Formatting::rupiah)).joinToString(" ")
    BentoTile(
        "Pertanyaan", modifier, container = colors.accent, contentColor = colors.onAccent, borderColor = colors.accent,
        goBackground = colors.onAccent.copy(alpha = 0.12f), goTint = colors.onAccent,
        onClick = onOpen, description = "Pertanyaan, ${state.questionCount} menunggu. Berikutnya: $nextText"
    ) {
        Text("${state.questionCount}", style = CompanionType.questionCount, color = colors.onAccent)
        Text(nextText, style = CompanionType.small.copy(fontSize = 14.sp, fontWeight = FontWeight(600)), color = colors.onAccent)
    }
}

@Composable
private fun SyncTile(health: SyncHealth, onOpen: () -> Unit, modifier: Modifier) {
    val colors = CompanionTheme.colors
    val state = if (health.listenerConnected) "Aktif" else "Terputus"
    val last = if (health.lastSyncAt > 0) "Terakhir ${Formatting.time(health.lastSyncAt)}" else "Belum pernah sinkron"
    val failed = if (health.failedCaptures > 0) "${health.failedCaptures} gagal disimpan" else null
    BentoTile("Sinkron", modifier, onClick = onOpen, description = listOfNotNull("Sinkron", state, last, failed).joinToString(", ")) {
        Row(verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(6.dp)) {
            Icon(
                if (health.listenerConnected) Icons.Rounded.Check else Icons.Outlined.WarningAmber, null,
                tint = colors.text, modifier = Modifier.size(20.dp)
            )
            Text(state, style = CompanionType.tileState, color = colors.text)
        }
        Text(last, style = CompanionType.small, color = colors.text2)
        if (failed != null) {
            Row(
                Modifier.clip(RoundedCornerShape(8.dp)).background(colors.warnBg).padding(horizontal = 8.dp, vertical = 2.dp),
                verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(4.dp)
            ) {
                Icon(Icons.Outlined.WarningAmber, null, tint = colors.warnFg, modifier = Modifier.size(14.dp))
                Text(failed, style = CompanionType.meta.copy(fontWeight = FontWeight(700)), color = colors.warnFg)
            }
        }
    }
}

@Composable
private fun CountTile(label: String, count: Int?, caption: String, onClick: () -> Unit, modifier: Modifier) {
    val colors = CompanionTheme.colors
    BentoTile(label, modifier, onClick = onClick, description = "$label, ${count ?: UNREAD_SPOKEN}. $caption") {
        Text(count?.toString() ?: UNREAD, style = CompanionType.tileCount, color = colors.text)
        Text(caption, style = CompanionType.small, color = colors.text2)
    }
}
