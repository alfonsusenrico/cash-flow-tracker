package com.alfonsusenrico.financialtracker.listener.ui.questions

import androidx.compose.foundation.background
import androidx.compose.foundation.border
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.selection.selectable
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.rounded.Check
import androidx.compose.material3.Icon
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.alpha
import androidx.compose.ui.draw.clip
import androidx.compose.ui.graphics.vector.ImageVector
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.semantics.Role
import androidx.compose.ui.semantics.clearAndSetSemantics
import androidx.compose.ui.semantics.contentDescription
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionTheme
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionType

/** How an answer option looks: plain, the suggested one (accent), or chosen in a select-then-confirm list. */
enum class OptionLook { PLAIN, SUGGESTED, SELECTED }

/**
 * One answer option (`.f-opt`): at least 56 dp high, a label and an optional detail line. With [selected]
 * it is a radio option; without, a button that answers at once.
 */
@Composable
fun AnswerOption(
    label: String,
    detail: String?,
    look: OptionLook,
    enabled: Boolean,
    onClick: () -> Unit,
    modifier: Modifier = Modifier,
    description: String? = null,
    selected: Boolean? = null,
    leadingIcon: ImageVector? = null
) {
    val colors = CompanionTheme.colors
    val shape = RoundedCornerShape(16.dp)
    val (bg, fg) = when (look) {
        OptionLook.PLAIN -> colors.surface2 to colors.text
        OptionLook.SUGGESTED -> colors.accent to colors.onAccent
        OptionLook.SELECTED -> colors.primary to colors.onPrimary
    }
    val interaction = if (selected != null) {
        Modifier.selectable(selected = selected, enabled = enabled, role = Role.RadioButton, onClick = onClick)
    } else {
        Modifier.clickable(enabled = enabled, role = Role.Button, onClick = onClick)
    }
    Row(
        modifier = modifier
            .heightIn(min = 56.dp)
            .clip(shape)
            .background(bg)
            .border(1.dp, if (look == OptionLook.PLAIN) colors.border else bg, shape)
            .then(interaction)
            .then(if (description != null) Modifier.clearAndSetSemantics { contentDescription = description } else Modifier)
            .alpha(if (enabled) 1f else 0.6f)
            .padding(horizontal = 14.dp, vertical = 8.dp),
        verticalAlignment = Alignment.CenterVertically,
        horizontalArrangement = Arrangement.spacedBy(8.dp)
    ) {
        if (leadingIcon != null) Icon(leadingIcon, contentDescription = null, tint = fg, modifier = Modifier.size(18.dp))
        Column(Modifier.weight(1f)) {
            Text(label, style = CompanionType.bodyStrong, color = fg)
            if (detail != null) Text(detail, style = CompanionType.meta.copy(fontSize = 12.sp, fontWeight = FontWeight(600)), color = fg)
        }
        if (look == OptionLook.SELECTED) Icon(Icons.Rounded.Check, contentDescription = null, tint = fg, modifier = Modifier.size(18.dp))
    }
}

/** Options two per row (`.f-opts`), one per row once large text would squeeze them. */
@Composable
fun OptionGrid(items: List<@Composable (Modifier) -> Unit>) {
    val single = LocalDensity.current.fontScale >= 1.5f
    Column(verticalArrangement = Arrangement.spacedBy(8.dp)) {
        if (single) {
            items.forEach { it(Modifier.fillMaxWidth()) }
        } else {
            items.chunked(2).forEach { pair ->
                Row(Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                    pair.forEach { it(Modifier.weight(1f)) }
                    if (pair.size == 1) Spacer(Modifier.weight(1f))
                }
            }
        }
    }
}
