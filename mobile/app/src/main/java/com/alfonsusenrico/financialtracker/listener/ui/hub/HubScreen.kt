package com.alfonsusenrico.financialtracker.listener.ui.hub

import androidx.compose.foundation.Canvas
import androidx.compose.foundation.background
import androidx.compose.foundation.border
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.IntrinsicSize
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxHeight
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.statusBarsPadding
import androidx.compose.foundation.layout.navigationBarsPadding
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.verticalScroll
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.automirrored.outlined.List
import androidx.compose.material.icons.automirrored.outlined.OpenInNew
import androidx.compose.material.icons.automirrored.rounded.ArrowForward
import androidx.compose.material.icons.outlined.Bedtime
import androidx.compose.material.icons.outlined.CloudOff
import androidx.compose.material.icons.outlined.CloudUpload
import androidx.compose.material.icons.outlined.Description
import androidx.compose.material.icons.outlined.HourglassEmpty
import androidx.compose.material.icons.outlined.Inbox
import androidx.compose.material.icons.outlined.Info
import androidx.compose.material.icons.outlined.Keyboard
import androidx.compose.material.icons.outlined.LinkOff
import androidx.compose.material.icons.outlined.NotificationsOff
import androidx.compose.material.icons.outlined.Schedule
import androidx.compose.material.icons.outlined.Settings
import androidx.compose.material.icons.outlined.WarningAmber
import androidx.compose.material.icons.rounded.Check
import androidx.compose.material.icons.rounded.Wallet
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.Icon
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.geometry.CornerRadius
import androidx.compose.ui.geometry.Offset
import androidx.compose.ui.geometry.Size
import androidx.compose.ui.graphics.drawscope.Stroke
import androidx.compose.ui.graphics.vector.ImageVector
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.semantics.LiveRegionMode
import androidx.compose.ui.semantics.Role
import androidx.compose.ui.semantics.clearAndSetSemantics
import androidx.compose.ui.semantics.contentDescription
import androidx.compose.ui.semantics.heading
import androidx.compose.ui.semantics.liveRegion
import androidx.compose.ui.semantics.role
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.style.TextAlign
import androidx.compose.ui.unit.Dp
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import com.alfonsusenrico.financialtracker.listener.data.prefs.StoredReport
import com.alfonsusenrico.financialtracker.listener.ui.components.ButtonKind
import com.alfonsusenrico.financialtracker.listener.ui.components.CompanionButton
import com.alfonsusenrico.financialtracker.listener.ui.components.Note
import com.alfonsusenrico.financialtracker.listener.ui.model.StatusTone
import com.alfonsusenrico.financialtracker.listener.ui.components.toneColors
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionTheme
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionType

/** What the hub shows; everything comes from this phone, except the report's label from the tracker. */
data class HubUiState(
    val status: HubStatus,
    /** True while the fix runs (the self-test, a retry); the fix button then says "Bentar…". */
    val fixing: Boolean = false,
    /** A one-off note under the status, such as the quiet-day test result. */
    val note: String? = null,
    val report: StoredReport? = null,
    /** Null until the questions have been read; never shown as "no questions" before then. */
    val questionCount: Int? = null,
    val lightCount: Int = 0,
    val reminderLabel: String? = null
)

class HubActions(
    val onFix: (FixAction) -> Unit,
    val onStatusDetails: () -> Unit,
    val onCatat: () -> Unit,
    val onOpenReport: (StoredReport) -> Unit,
    val onAllQuestions: () -> Unit,
    val onLog: () -> Unit,
    val onSettings: () -> Unit,
    val onWeb: () -> Unit
)

/**
 * Beranda, direction C ("Satu layar status"): the status card first, then one large Catat button, the next
 * question with the rest stacked behind it, and three tiles. No money total appears here.
 */
@Composable
fun HubScreen(state: HubUiState, actions: HubActions, topQuestion: @Composable () -> Unit = {}) {
    val colors = CompanionTheme.colors
    Column(
        Modifier.fillMaxSize().background(colors.bg).verticalScroll(rememberScrollState())
            .statusBarsPadding().navigationBarsPadding().padding(start = 16.dp, end = 16.dp, bottom = 32.dp),
        verticalArrangement = Arrangement.spacedBy(16.dp)
    ) {
        Brand()
        StatusCard(state, actions)
        CompanionButton(
            "Catat transaksi", actions.onCatat,
            Modifier.fillMaxWidth().heightIn(min = 60.dp).semantics { contentDescription = "Catat transaksi" },
            // While a fix button shows, Catat steps back so two strong buttons never compete.
            if (state.status.problem != null) ButtonKind.GHOST else ButtonKind.ACCENT, Icons.Outlined.Keyboard
        )
        state.report?.takeIf { !it.opened }?.let { ReportCard(it) { actions.onOpenReport(it) } }
        Questions(state, actions, topQuestion)
        Tiles(actions)
    }
}

@Composable
private fun Brand() {
    val colors = CompanionTheme.colors
    Row(Modifier.fillMaxWidth().heightIn(min = 56.dp), verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(10.dp)) {
        Box(Modifier.size(32.dp).clip(RoundedCornerShape(10.dp)).background(colors.okBg), contentAlignment = Alignment.Center) {
            Icon(Icons.Rounded.Wallet, null, tint = colors.okFg, modifier = Modifier.size(18.dp))
        }
        Text("Cash Flow Tracker", style = CompanionType.bodyStrong, color = colors.text)
    }
}

@Composable
private fun StatusCard(state: HubUiState, actions: HubActions) {
    val colors = CompanionTheme.colors
    val status = state.status
    val problem = status.problem
    val tone = when (problem?.tone) {
        null -> StatusTone.OK
        ProblemTone.BAD -> StatusTone.ASK
        ProblemTone.WARN -> StatusTone.WARN
    }
    val shape = RoundedCornerShape(28.dp)
    Column(
        Modifier.fillMaxWidth().clip(shape).background(colors.surface).border(1.dp, colors.border, shape),
        verticalArrangement = Arrangement.spacedBy(8.dp)
    ) {
        Illustration(problem?.kind, tone)
        Column(Modifier.padding(start = 18.dp, end = 18.dp, bottom = 16.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
            Text(
                status.title, style = CompanionType.onboardingTitle, color = colors.text,
                modifier = Modifier.semantics { heading(); liveRegion = LiveRegionMode.Polite }
            )
            Text(problem?.body ?: status.todayLine, style = CompanionType.body, color = colors.text2)
            problem?.oemHint?.let { Note("HP-mu ${it.brand} (${it.system}). ${it.steps}", StatusTone.WARN, Icons.Outlined.Info) }
            Row(
                Modifier.semantics(mergeDescendants = true) {},
                verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(6.dp)
            ) {
                Icon(if (status.syncOk) Icons.Rounded.Check else Icons.Outlined.Schedule, null, tint = colors.text2, modifier = Modifier.size(16.dp))
                Text(status.syncLine, style = CompanionType.small.copy(fontWeight = FontWeight(650)), color = colors.text2)
            }
            state.note?.let {
                Text(it, style = CompanionType.small, color = colors.okFg, modifier = Modifier.semantics { liveRegion = LiveRegionMode.Polite })
            }
            if (problem != null) {
                if (state.fixing) {
                    Row(
                        Modifier.heightIn(min = 48.dp).semantics(mergeDescendants = true) { liveRegion = LiveRegionMode.Polite },
                        verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(10.dp)
                    ) {
                        CircularProgressIndicator(Modifier.size(18.dp), strokeWidth = 2.dp, color = colors.text)
                        Text("Bentar…", style = CompanionType.bodyStrong, color = colors.text)
                    }
                } else {
                    CompanionButton(problem.fixLabel, { actions.onFix(problem.fix) }, Modifier.fillMaxWidth(), ButtonKind.ACCENT)
                }
            }
            status.softIssues.forEach { issue -> SoftIssueNote(issue) { actions.onFix(issue.fix) } }
            TextLink("Rincian", actions.onStatusDetails, "Buka rincian status")
        }
    }
}

@Composable
private fun SoftIssueNote(issue: SoftIssue, onFix: () -> Unit) {
    val (bg, fg) = toneColors(StatusTone.WARN)
    Column(
        Modifier.fillMaxWidth().clip(RoundedCornerShape(14.dp)).background(bg).padding(horizontal = 14.dp, vertical = 10.dp),
        verticalArrangement = Arrangement.spacedBy(8.dp)
    ) {
        Row(horizontalArrangement = Arrangement.spacedBy(10.dp)) {
            Icon(Icons.Outlined.WarningAmber, null, tint = fg, modifier = Modifier.padding(top = 2.dp).size(18.dp))
            Text(issue.text, style = CompanionType.small, color = fg)
        }
        CompanionButton(issue.fixLabel, onFix, kind = ButtonKind.GHOST)
    }
}

@Composable
private fun TextLink(text: String, onClick: () -> Unit, description: String) {
    val colors = CompanionTheme.colors
    Row(
        Modifier.heightIn(min = 48.dp).clip(RoundedCornerShape(12.dp))
            .clickable(role = Role.Button, onClick = onClick)
            .clearAndSetSemantics { contentDescription = description; this.role = Role.Button }
            .padding(horizontal = 4.dp),
        verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(4.dp)
    ) {
        Text(text, style = CompanionType.button, color = colors.accentText)
        Icon(Icons.AutoMirrored.Rounded.ArrowForward, null, tint = colors.accentText, modifier = Modifier.size(16.dp))
    }
}

/** Decorative: a phone listening when calm, the problem's symbol otherwise. TalkBack skips it. */
@Composable
private fun Illustration(problem: ProblemKind?, tone: StatusTone) {
    val colors = CompanionTheme.colors
    val (bg, fg) = toneColors(tone)
    Box(
        Modifier.fillMaxWidth().height(116.dp).background(if (problem == null) colors.surface2 else bg).clearAndSetSemantics {},
        contentAlignment = Alignment.Center
    ) {
        if (problem == null) {
            val stroke = colors.text
            val fills = listOf(colors.okBg, colors.moveBg, colors.warnBg)
            Canvas(Modifier.size(width = 200.dp, height = 96.dp)) {
                val phone = Size(56.dp.toPx(), 92.dp.toPx())
                val left = (size.width - phone.width) / 2
                drawRoundRect(colors.surface, Offset(left, 2.dp.toPx()), phone, CornerRadius(12.dp.toPx()))
                drawRoundRect(stroke, Offset(left, 2.dp.toPx()), phone, CornerRadius(12.dp.toPx()), style = Stroke(2.dp.toPx()))
                fills.forEachIndexed { index, fill ->
                    val top = 20.dp.toPx() + index * 22.dp.toPx()
                    drawRoundRect(fill, Offset(left + 8.dp.toPx(), top), Size(phone.width - 16.dp.toPx(), 16.dp.toPx()), CornerRadius(5.dp.toPx()))
                    drawRoundRect(stroke, Offset(left + 8.dp.toPx(), top), Size(phone.width - 16.dp.toPx(), 16.dp.toPx()), CornerRadius(5.dp.toPx()), style = Stroke(1.5.dp.toPx()))
                }
                val center = Offset(size.width / 2, 2.dp.toPx() + phone.height / 2)
                listOf(14.dp, 28.dp).forEach { gap ->
                    val radius = phone.width / 2 + gap.toPx()
                    val box = Size(2 * radius, 2 * radius)
                    val corner = Offset(center.x - radius, center.y - radius)
                    drawArc(stroke, -30f, 60f, false, corner, box, style = Stroke(2.dp.toPx()))
                    drawArc(stroke, 150f, 60f, false, corner, box, style = Stroke(2.dp.toPx()))
                }
            }
        } else {
            Box(Modifier.size(72.dp).clip(RoundedCornerShape(24.dp)).background(colors.surface), contentAlignment = Alignment.Center) {
                Icon(problemIcon(problem), null, tint = fg, modifier = Modifier.size(40.dp))
            }
        }
    }
}

private fun problemIcon(kind: ProblemKind): ImageVector = when (kind) {
    ProblemKind.NOT_PAIRED, ProblemKind.KEY_REFUSED -> Icons.Outlined.LinkOff
    ProblemKind.ACCESS_OFF -> Icons.Outlined.NotificationsOff
    ProblemKind.LISTENER_ASLEEP -> Icons.Outlined.Bedtime
    ProblemKind.SERVER_UNREACHABLE -> Icons.Outlined.CloudOff
    ProblemKind.WAITING_TO_SEND -> Icons.Outlined.CloudUpload
    ProblemKind.QUIET_DAY -> Icons.Outlined.HourglassEmpty
}

@Composable
private fun ReportCard(report: StoredReport, onOpen: () -> Unit) {
    val colors = CompanionTheme.colors
    val shape = RoundedCornerShape(22.dp)
    Column(
        Modifier.fillMaxWidth().clip(shape).background(colors.surface).border(1.dp, colors.border, shape).padding(16.dp),
        verticalArrangement = Arrangement.spacedBy(10.dp)
    ) {
        Row(horizontalArrangement = Arrangement.spacedBy(10.dp), verticalAlignment = Alignment.CenterVertically) {
            Icon(Icons.Outlined.Description, null, tint = colors.text, modifier = Modifier.size(22.dp))
            Text(
                "Laporan ${report.label} udah siap", style = CompanionType.cardTitle, color = colors.text,
                modifier = Modifier.semantics { heading() }
            )
        }
        Text("Ringkasan bulananmu dibaca di web, bukan di HP.", style = CompanionType.small, color = colors.text2)
        CompanionButton("Buka laporan di web", onOpen, Modifier.fillMaxWidth(), ButtonKind.GHOST, Icons.AutoMirrored.Outlined.OpenInNew)
    }
}

@Composable
private fun Questions(state: HubUiState, actions: HubActions, topQuestion: @Composable () -> Unit) {
    val colors = CompanionTheme.colors
    val count = state.questionCount ?: return
    if (count == 0) {
        val shape = RoundedCornerShape(24.dp)
        Column(
            Modifier.fillMaxWidth().clip(shape).background(colors.surface).border(1.dp, colors.border, shape).padding(18.dp),
            horizontalAlignment = Alignment.CenterHorizontally, verticalArrangement = Arrangement.spacedBy(6.dp)
        ) {
            Icon(Icons.Outlined.Inbox, null, tint = colors.text2, modifier = Modifier.size(32.dp))
            Text("Nggak ada pertanyaan", style = CompanionType.cardTitle, color = colors.text, modifier = Modifier.semantics { heading() })
            Text("Aku kabarin kalau ada yang baru.", style = CompanionType.small, color = colors.text2, textAlign = TextAlign.Center)
        }
        return
    }
    Column(verticalArrangement = Arrangement.spacedBy(10.dp)) {
        Row(Modifier.fillMaxWidth().padding(horizontal = 4.dp), verticalAlignment = Alignment.CenterVertically) {
            Column(Modifier.weight(1f)) {
                Text("Ada $count pertanyaan", style = CompanionType.pageTitle, color = colors.text, modifier = Modifier.semantics { heading() })
                if (state.lightCount > 0 && state.reminderLabel != null) {
                    Text("${state.lightCount} bisa nanti malam, ${state.reminderLabel}", style = CompanionType.small, color = colors.text2)
                }
            }
            TextLink("Lihat semua", actions.onAllQuestions, "Lihat semua pertanyaan, $count")
        }
        Column(Modifier.fillMaxWidth()) {
            topQuestion()
            // The next questions peek out under the top card; decorative, TalkBack hears the count above.
            if (count > 1) Peek(14.dp)
            if (count > 2) Peek(28.dp)
        }
    }
}

@Composable
private fun Peek(inset: Dp) {
    val colors = CompanionTheme.colors
    val shape = RoundedCornerShape(bottomStart = 20.dp, bottomEnd = 20.dp)
    Box(
        Modifier.padding(horizontal = inset).fillMaxWidth().height(10.dp).clip(shape)
            .background(colors.surface).border(1.dp, colors.borderStrong, shape).clearAndSetSemantics {}
    )
}

@Composable
private fun Tiles(actions: HubActions) {
    val stack = LocalDensity.current.fontScale >= 1.5f
    val tiles: List<Triple<String, ImageVector, () -> Unit>> = listOf(
        Triple("Riwayat", Icons.AutoMirrored.Outlined.List, actions.onLog),
        Triple("Pengaturan", Icons.Outlined.Settings, actions.onSettings),
        Triple("Buka web", Icons.AutoMirrored.Outlined.OpenInNew, actions.onWeb)
    )
    if (stack) {
        Column(Modifier.fillMaxWidth(), verticalArrangement = Arrangement.spacedBy(8.dp)) {
            tiles.forEach { (label, icon, onClick) -> Tile(label, icon, onClick, Modifier.fillMaxWidth()) }
        }
    } else {
        Row(Modifier.fillMaxWidth().height(IntrinsicSize.Min), horizontalArrangement = Arrangement.spacedBy(8.dp)) {
            tiles.forEach { (label, icon, onClick) -> Tile(label, icon, onClick, Modifier.weight(1f).fillMaxHeight()) }
        }
    }
}

@Composable
private fun Tile(label: String, icon: ImageVector, onClick: () -> Unit, modifier: Modifier) {
    val colors = CompanionTheme.colors
    val shape = RoundedCornerShape(20.dp)
    Column(
        modifier.heightIn(min = 76.dp).clip(shape).background(colors.surface).border(1.dp, colors.border, shape)
            .clickable(role = Role.Button, onClick = onClick).padding(horizontal = 6.dp, vertical = 12.dp),
        horizontalAlignment = Alignment.CenterHorizontally, verticalArrangement = Arrangement.spacedBy(6.dp, Alignment.CenterVertically)
    ) {
        Icon(icon, null, tint = colors.text2, modifier = Modifier.size(20.dp))
        Text(label, style = CompanionType.button.copy(fontSize = 14.sp), color = colors.text, textAlign = TextAlign.Center)
    }
}
