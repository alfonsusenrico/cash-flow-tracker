package com.alfonsusenrico.financialtracker.listener.ui.capture

import androidx.compose.foundation.background
import androidx.compose.foundation.border
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.ColumnScope
import androidx.compose.foundation.layout.ExperimentalLayoutApi
import androidx.compose.foundation.layout.FlowRow
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.Icon
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.draw.drawBehind
import androidx.compose.ui.geometry.CornerRadius
import androidx.compose.ui.graphics.PathEffect
import androidx.compose.ui.graphics.drawscope.Stroke
import androidx.compose.ui.graphics.vector.ImageVector
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.semantics.LiveRegionMode
import androidx.compose.ui.semantics.Role
import androidx.compose.ui.semantics.clearAndSetSemantics
import androidx.compose.ui.semantics.contentDescription
import androidx.compose.ui.semantics.liveRegion
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionTheme
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionType
import com.alfonsusenrico.financialtracker.listener.ui.theme.TABULAR_FIGURES

/** The receipt card of the sheet (`.f-receipt`). */
@Composable
fun ReceiptCard(modifier: Modifier = Modifier, content: @Composable ColumnScope.() -> Unit) {
    val colors = CompanionTheme.colors
    val shape = RoundedCornerShape(22.dp)
    Column(
        modifier.fillMaxWidth().clip(shape).background(colors.surface).border(1.dp, colors.border, shape).padding(16.dp),
        verticalArrangement = Arrangement.spacedBy(10.dp),
        content = content
    )
}

/** The typed text as a quote (`“…”`), so the owner sees exactly what was sent. */
@Composable
fun TypedQuote(text: String) {
    Text(
        "“$text”", style = CompanionType.small.copy(fontSize = 13.sp), color = CompanionTheme.colors.text2,
        modifier = Modifier.semantics { contentDescription = "Teks yang diketik: $text" }
    )
}

/** A known fact, or with [missing] the one the tracker still asks for (`.f-tags span.missing`). */
data class FactTag(val text: String, val missing: Boolean = false)

@OptIn(ExperimentalLayoutApi::class)
@Composable
fun FactTags(tags: List<FactTag>) {
    if (tags.isEmpty()) return
    val colors = CompanionTheme.colors
    FlowRow(horizontalArrangement = Arrangement.spacedBy(6.dp), verticalArrangement = Arrangement.spacedBy(6.dp)) {
        tags.forEach { tag ->
            val shape = RoundedCornerShape(11.dp)
            val dashed = Modifier.drawBehind {
                drawRoundRect(
                    color = colors.askFg, cornerRadius = CornerRadius(11.dp.toPx()),
                    style = Stroke(width = 1.dp.toPx(), pathEffect = PathEffect.dashPathEffect(floatArrayOf(4.dp.toPx(), 3.dp.toPx())))
                )
            }
            Text(
                tag.text,
                style = CompanionType.small.copy(fontSize = 13.5.sp, fontWeight = FontWeight(650), fontFeatureSettings = TABULAR_FIGURES),
                color = if (tag.missing) colors.askFg else colors.text,
                modifier = Modifier.clip(shape).background(if (tag.missing) colors.askBg else colors.surface2)
                    .then(if (tag.missing) dashed else Modifier)
                    .padding(horizontal = 11.dp, vertical = 5.dp)
                    .then(if (tag.missing) Modifier.semantics { contentDescription = "Belum diketahui: ${tag.text.trimEnd('?')}" } else Modifier)
            )
        }
    }
}

/** A compact answer chip (`.f-chip`): 48 dp high, answers at once. */
@Composable
fun AnswerChip(label: String, onClick: () -> Unit, enabled: Boolean, description: String? = null, detail: String? = null, icon: ImageVector? = null) {
    val colors = CompanionTheme.colors
    val shape = RoundedCornerShape(14.dp)
    Row(
        Modifier.heightIn(min = 48.dp).clip(shape).background(colors.surface2).border(1.dp, colors.border, shape)
            .clickable(enabled = enabled, role = Role.Button, onClick = onClick)
            .then(if (description != null) Modifier.clearAndSetSemantics { contentDescription = description } else Modifier)
            .padding(horizontal = 14.dp, vertical = 6.dp),
        verticalAlignment = Alignment.CenterVertically,
        horizontalArrangement = Arrangement.spacedBy(6.dp)
    ) {
        if (icon != null) Icon(icon, contentDescription = null, tint = colors.text, modifier = Modifier.size(16.dp))
        Column {
            Text(label, style = CompanionType.bodyStrong.copy(fontSize = 14.5.sp, fontFeatureSettings = TABULAR_FIGURES), color = if (enabled) colors.text else colors.text2)
            if (detail != null) Text(detail, style = CompanionType.meta.copy(fontWeight = FontWeight(600)), color = colors.text2)
        }
    }
}

@OptIn(ExperimentalLayoutApi::class)
@Composable
fun ChipFlow(content: @Composable () -> Unit) {
    FlowRow(horizontalArrangement = Arrangement.spacedBy(8.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) { content() }
}

/** Two actions side by side (`.f-two`), stacked once large text would squeeze them. */
@Composable
fun ActionPair(first: @Composable (Modifier) -> Unit, second: (@Composable (Modifier) -> Unit)?) {
    val stacked = LocalDensity.current.fontScale >= 1.5f
    if (stacked || second == null) {
        Column(verticalArrangement = Arrangement.spacedBy(8.dp)) {
            first(Modifier.fillMaxWidth())
            second?.invoke(Modifier.fillMaxWidth())
        }
    } else {
        Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
            first(Modifier.weight(1f))
            second(Modifier.weight(1f))
        }
    }
}

/** A short explanation line (`.f-why`). */
@Composable
fun WhyLine(text: String, icon: ImageVector? = null) {
    val colors = CompanionTheme.colors
    Row(horizontalArrangement = Arrangement.spacedBy(6.dp), verticalAlignment = Alignment.CenterVertically) {
        if (icon != null) Icon(icon, contentDescription = null, tint = colors.text2, modifier = Modifier.size(14.dp))
        Text(text, style = CompanionType.small.copy(fontSize = 13.sp), color = colors.text2)
    }
}

/** The result of the owner's last action in the sheet, read out by TalkBack. */
@Composable
fun NoticeLine(text: String?) {
    if (text == null) return
    Text(
        text, style = CompanionType.small, color = CompanionTheme.colors.text,
        modifier = Modifier.fillMaxWidth().semantics { liveRegion = LiveRegionMode.Polite }
    )
}
