package com.alfonsusenrico.financialtracker.listener.ui.shell

import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.BoxWithConstraints
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.RowScope
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.navigationBarsPadding
import androidx.compose.foundation.layout.offset
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.widthIn
import androidx.compose.foundation.selection.selectable
import androidx.compose.foundation.selection.selectableGroup
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.automirrored.outlined.HelpOutline
import androidx.compose.material.icons.automirrored.outlined.List
import androidx.compose.material.icons.outlined.GridView
import androidx.compose.material.icons.outlined.Tune
import androidx.compose.material.icons.rounded.Add
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.Icon
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.remember
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.vector.ImageVector
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.semantics.Role
import androidx.compose.ui.semantics.clearAndSetSemantics
import androidx.compose.ui.semantics.contentDescription
import androidx.compose.ui.semantics.onClick
import androidx.compose.ui.semantics.role
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.semantics.stateDescription
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.rememberTextMeasurer
import androidx.compose.ui.text.style.TextAlign
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionTheme
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionType
import com.alfonsusenrico.financialtracker.listener.ui.theme.TABULAR_FIGURES

enum class Tab(val route: String, val label: String, val icon: ImageVector) {
    HOME(Routes.HOME, "Beranda", Icons.Outlined.GridView),
    ACTIVITY(Routes.ACTIVITY, "Aktivitas", Icons.AutoMirrored.Outlined.List),
    QUESTIONS(Routes.QUESTIONS, "Tanya", Icons.AutoMirrored.Outlined.HelpOutline),
    SETTINGS(Routes.SETTINGS, "Atur", Icons.Outlined.Tune)
}

/** How far the Catat disc rises above the bar's top edge (`.f-catat { margin-top: -26px }`). */
private val CATAT_RAISE = 26.dp

/**
 * The bottom bar: two tabs, an optional centre action, two tabs. The raised centre action stands in
 * the bar's own bounds, above its surface, so the whole disc stays touchable.
 */
@Composable
fun CompanionBottomBar(
    current: Tab?,
    questionCount: Int,
    onSelect: (Tab) -> Unit,
    centerAction: (@Composable () -> Unit)? = null
) {
    val colors = CompanionTheme.colors
    val raise = if (centerAction != null) CATAT_RAISE else 0.dp
    Box(Modifier.fillMaxWidth()) {
        Column(Modifier.matchParentSize().padding(top = raise).background(colors.surface)) {
            HorizontalDivider(color = colors.border, thickness = 1.dp)
        }
        Row(
            Modifier.fillMaxWidth().navigationBarsPadding().padding(start = 4.dp, end = 4.dp, bottom = 8.dp).selectableGroup(),
            verticalAlignment = Alignment.Bottom
        ) {
            val (leading, trailing) = Tab.entries.chunked(2)
            Row(Modifier.weight(2f).padding(top = raise + 4.dp)) {
                leading.forEach { TabItem(it, it == current, questionCount, onSelect) }
            }
            if (centerAction != null) Box(Modifier.widthIn(min = 84.dp), contentAlignment = Alignment.Center) { centerAction() }
            Row(Modifier.weight(2f).padding(top = raise + 4.dp)) {
                trailing.forEach { TabItem(it, it == current, questionCount, onSelect) }
            }
        }
    }
}

/**
 * The raised Catat button (`.f-catat`): a 62 dp accent disc with its label, which opens the capture
 * sheet. TalkBack reads it as a button and says whether the sheet is open.
 */
@Composable
fun CatatButton(open: Boolean, onClick: () -> Unit) {
    val colors = CompanionTheme.colors
    Column(
        Modifier.clip(RoundedCornerShape(22.dp))
            .clickable(role = Role.Button, onClickLabel = "Buka catat cepat", onClick = onClick)
            .semantics(mergeDescendants = true) {
                contentDescription = "Catat transaksi"
                role = Role.Button
                stateDescription = if (open) "Terbuka" else "Tertutup"
                onClick(label = "Buka catat cepat") { onClick(); true }
            },
        horizontalAlignment = Alignment.CenterHorizontally,
        verticalArrangement = Arrangement.spacedBy(4.dp)
    ) {
        Box(
            Modifier.size(74.dp).clip(RoundedCornerShape(28.dp)).background(colors.bg).padding(6.dp)
                .clip(RoundedCornerShape(22.dp)).background(if (open) colors.primary else colors.accent),
            contentAlignment = Alignment.Center
        ) {
            Icon(Icons.Rounded.Add, contentDescription = null, tint = if (open) colors.onPrimary else colors.onAccent, modifier = Modifier.size(26.dp))
        }
        Text("Catat", style = CompanionType.meta.copy(fontSize = 12.sp, fontWeight = FontWeight(750)), color = colors.text, maxLines = 1)
    }
}

@Composable
private fun RowScope.TabItem(tab: Tab, selected: Boolean, questionCount: Int, onSelect: (Tab) -> Unit) {
    val colors = CompanionTheme.colors
    val count = if (tab == Tab.QUESTIONS) questionCount else 0
    val description = if (count > 0) "${tab.label}, $count pertanyaan menunggu" else tab.label
    Column(
        modifier = Modifier
            .weight(1f)
            .heightIn(min = 56.dp)
            .clip(RoundedCornerShape(12.dp))
            .selectable(selected = selected, role = Role.Tab, onClick = { onSelect(tab) })
            // The description stays the spoken name; the label merges into the tab instead of being cleared.
            .semantics(mergeDescendants = true) { contentDescription = description }
            .padding(vertical = 4.dp),
        horizontalAlignment = Alignment.CenterHorizontally,
        verticalArrangement = Arrangement.spacedBy(2.dp)
    ) {
        Box {
            Icon(tab.icon, contentDescription = null, tint = if (selected) colors.accentText else colors.text2, modifier = Modifier.size(24.dp))
            if (count > 0) {
                Box(
                    Modifier.align(Alignment.TopEnd).offset(x = 14.dp, y = (-6).dp)
                        .heightIn(min = 20.dp).widthIn(min = 20.dp)
                        .clip(RoundedCornerShape(999.dp)).background(colors.accent).padding(horizontal = 5.dp),
                    contentAlignment = Alignment.Center
                ) {
                    Text(
                        "$count", color = colors.onAccent, fontSize = 11.5.sp, fontWeight = FontWeight(750),
                        style = CompanionType.meta.copy(fontFeatureSettings = TABULAR_FIGURES),
                        modifier = Modifier.clearAndSetSemantics {}
                    )
                }
            }
        }
        TabLabel(tab.label, if (selected) colors.text else colors.text2)
        Box(
            Modifier.size(width = 18.dp, height = 3.dp).clip(RoundedCornerShape(3.dp))
                .background(if (selected) colors.accent else colors.surface)
        )
    }
}

private val TAB_LABEL = CompanionType.meta.copy(fontSize = 11.5.sp, fontWeight = FontWeight(650))

/**
 * A tab label at the owner's font size, made smaller only as far as the whole word needs to fit the
 * tab's width, and never below its size at font scale 1.0, which fits every label in a 320 dp wide bar.
 */
@Composable
private fun TabLabel(text: String, color: Color) {
    val measurer = rememberTextMeasurer()
    BoxWithConstraints(Modifier.fillMaxWidth(), contentAlignment = Alignment.Center) {
        val available = constraints.maxWidth
        val fontScale = LocalDensity.current.fontScale
        val style = remember(text, available, fontScale) {
            val smallest = TAB_LABEL.fontSize.value / fontScale.coerceAtLeast(1f)
            var size = TAB_LABEL.fontSize.value
            fun widthAt(size: Float) = measurer.measure(text, TAB_LABEL.copy(fontSize = size.sp), maxLines = 1, softWrap = false).size.width
            while (size > smallest && widthAt(size) > available) size = (size - 0.25f).coerceAtLeast(smallest)
            TAB_LABEL.copy(fontSize = size.sp, lineHeight = (size * 1.35f).sp)
        }
        Text(text, Modifier.fillMaxWidth(), style = style, color = color, textAlign = TextAlign.Center, maxLines = 1)
    }
}

object Routes {
    const val HOME = "home"
    const val ACTIVITY = "activity"
    const val QUESTIONS = "questions"
    const val SETTINGS = "settings"
    const val DETAIL = "detail"
    const val CAPTURE = "capture"
    const val ROW = "row"

    fun questions(rowId: Long?) = if (rowId == null) QUESTIONS else "$QUESTIONS?$ROW=$rowId"
    fun detail(rowId: Long) = "$DETAIL/$rowId"
    fun capture(captureId: Long) = "$CAPTURE/$captureId"
}
