package com.alfonsusenrico.financialtracker.listener.ui.components

import androidx.compose.foundation.background
import androidx.compose.foundation.border
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.ColumnScope
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.RowScope
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.automirrored.rounded.ArrowBack
import androidx.compose.material3.Icon
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.vector.ImageVector
import androidx.compose.ui.semantics.Role
import androidx.compose.ui.semantics.contentDescription
import androidx.compose.ui.semantics.heading
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.unit.Dp
import androidx.compose.ui.unit.dp
import com.alfonsusenrico.financialtracker.listener.ui.model.StatusTone
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionTheme
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionType

/** A bordered card on the page background (`.f-card`). */
@Composable
fun CompanionCard(
    modifier: Modifier = Modifier,
    spacing: Dp = 12.dp,
    content: @Composable ColumnScope.() -> Unit
) {
    val colors = CompanionTheme.colors
    Column(
        modifier = modifier
            .fillMaxWidth()
            .clip(RoundedCornerShape(20.dp))
            .background(colors.surface)
            .border(1.dp, colors.border, RoundedCornerShape(20.dp))
            .padding(16.dp),
        verticalArrangement = Arrangement.spacedBy(spacing),
        content = content
    )
}

/** A card heading with an optional leading icon; marked as a heading for TalkBack. */
@Composable
fun CardTitle(text: String, icon: ImageVector? = null) {
    Row(verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(8.dp)) {
        if (icon != null) Icon(icon, contentDescription = null, tint = CompanionTheme.colors.text, modifier = Modifier.size(18.dp))
        Text(text, style = CompanionType.cardTitle, color = CompanionTheme.colors.text, modifier = Modifier.semantics { heading() })
    }
}

/** Small uppercase label above a figure or a group. */
@Composable
fun SectionLabel(text: String, modifier: Modifier = Modifier, color: Color = CompanionTheme.colors.text2, softWrap: Boolean = true) {
    Text(text.uppercase(), style = CompanionType.label, color = color, modifier = modifier, softWrap = softWrap)
}

/** Status-coloured box with an icon, a title and a reason (`.f-state`). */
@Composable
fun StateBox(tone: StatusTone, icon: ImageVector, title: String, body: String?, modifier: Modifier = Modifier) {
    val (bg, fg) = toneColors(tone)
    Row(
        modifier = modifier.fillMaxWidth().clip(RoundedCornerShape(16.dp)).background(bg).padding(14.dp)
            .semantics(mergeDescendants = true) {},
        horizontalArrangement = Arrangement.spacedBy(12.dp)
    ) {
        Icon(icon, contentDescription = null, tint = fg, modifier = Modifier.size(22.dp))
        Column(verticalArrangement = Arrangement.spacedBy(2.dp)) {
            Text(title, style = CompanionType.bodyStrong, color = fg, modifier = Modifier.semantics { heading() })
            if (body != null) Text(body, style = CompanionType.small, color = fg)
        }
    }
}

/** An inline note on a tone (`.note`), used for explanations such as the HiOS steps. */
@Composable
fun Note(text: String, tone: StatusTone, icon: ImageVector, modifier: Modifier = Modifier) {
    val (bg, fg) = toneColors(tone)
    Row(
        modifier = modifier.fillMaxWidth().clip(RoundedCornerShape(14.dp)).background(bg).padding(horizontal = 14.dp, vertical = 12.dp),
        horizontalArrangement = Arrangement.spacedBy(10.dp)
    ) {
        Icon(icon, contentDescription = null, tint = fg, modifier = Modifier.padding(top = 2.dp).size(18.dp))
        Text(text, style = CompanionType.small, color = fg)
    }
}

@Composable
fun toneColors(tone: StatusTone): Pair<Color, Color> {
    val c = CompanionTheme.colors
    return when (tone) {
        StatusTone.OK -> c.okBg to c.okFg
        StatusTone.WARN -> c.warnBg to c.warnFg
        StatusTone.ASK -> c.askBg to c.askFg
        StatusTone.MOVE -> c.moveBg to c.moveFg
        StatusTone.PHONE -> c.phoneBg to c.phoneFg
        StatusTone.MUTED -> c.mutedBg to c.mutedFg
    }
}

/** Inner page bar with a back button (`.f-bar`). */
@Composable
fun InnerTopBar(title: String, onBack: () -> Unit, actions: @Composable RowScope.() -> Unit = {}) {
    Row(
        modifier = Modifier.fillMaxWidth().heightIn(min = 64.dp).padding(horizontal = 12.dp, vertical = 8.dp),
        verticalAlignment = Alignment.CenterVertically,
        horizontalArrangement = Arrangement.spacedBy(8.dp)
    ) {
        SquareIconButton(Icons.AutoMirrored.Rounded.ArrowBack, "Kembali", onBack)
        Text(
            title, style = CompanionType.pageTitle, color = CompanionTheme.colors.text,
            modifier = Modifier.weight(1f).semantics { heading() }
        )
        actions()
    }
}

/** A 48 dp icon-only button on a surface square (`.f-iconbtn`); [description] is what TalkBack reads. */
@Composable
fun SquareIconButton(
    icon: ImageVector,
    description: String,
    onClick: () -> Unit,
    modifier: Modifier = Modifier,
    enabled: Boolean = true,
    tint: Color = CompanionTheme.colors.text
) {
    val colors = CompanionTheme.colors
    Box(
        modifier = modifier
            .size(48.dp)
            .clip(RoundedCornerShape(16.dp))
            .background(colors.surface)
            .clickable(enabled = enabled, role = Role.Button, onClick = onClick)
            .semantics { this.contentDescription = description },
        contentAlignment = Alignment.Center
    ) {
        Icon(icon, contentDescription = null, tint = if (enabled) tint else colors.text2, modifier = Modifier.size(22.dp))
    }
}
