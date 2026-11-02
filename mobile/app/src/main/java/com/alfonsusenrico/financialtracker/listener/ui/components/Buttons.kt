package com.alfonsusenrico.financialtracker.listener.ui.components

import androidx.compose.foundation.BorderStroke
import androidx.compose.foundation.layout.PaddingValues
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.Button
import androidx.compose.material3.ButtonDefaults
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.Icon
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.vector.ImageVector
import androidx.compose.ui.text.style.TextAlign
import androidx.compose.ui.unit.dp
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionTheme
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionType

/** The prototype's button family: charcoal primary, orange accent, outlined ghost, and red-text danger. */
enum class ButtonKind { PRIMARY, ACCENT, GHOST, DANGER }

@Composable
fun CompanionButton(
    text: String,
    onClick: () -> Unit,
    modifier: Modifier = Modifier,
    kind: ButtonKind = ButtonKind.PRIMARY,
    icon: ImageVector? = null,
    enabled: Boolean = true,
    busy: Boolean = false
) {
    val colors = CompanionTheme.colors
    val shape = RoundedCornerShape(14.dp)
    val padding = PaddingValues(horizontal = 18.dp, vertical = 10.dp)
    val sized = modifier.heightIn(min = 48.dp)
    val content: @Composable () -> Unit = {
        if (busy) {
            CircularProgressIndicator(Modifier.size(18.dp), strokeWidth = 2.dp)
            Spacer(Modifier.width(8.dp))
        } else if (icon != null) {
            Icon(icon, contentDescription = null, modifier = Modifier.size(18.dp))
            Spacer(Modifier.width(8.dp))
        }
        Text(text, style = CompanionType.button, textAlign = TextAlign.Center)
    }
    when (kind) {
        ButtonKind.PRIMARY, ButtonKind.ACCENT -> Button(
            onClick = onClick, enabled = enabled, modifier = sized, shape = shape, contentPadding = padding,
            colors = ButtonDefaults.buttonColors(
                containerColor = if (kind == ButtonKind.PRIMARY) colors.primary else colors.accent,
                contentColor = if (kind == ButtonKind.PRIMARY) colors.onPrimary else colors.onAccent,
                disabledContainerColor = colors.surface2,
                disabledContentColor = colors.text2
            )
        ) { content() }
        ButtonKind.GHOST, ButtonKind.DANGER -> OutlinedButton(
            onClick = onClick, enabled = enabled, modifier = sized, shape = shape, contentPadding = padding,
            border = BorderStroke(1.dp, if (enabled) colors.borderStrong else colors.border),
            colors = ButtonDefaults.outlinedButtonColors(
                contentColor = if (kind == ButtonKind.DANGER) colors.danger else colors.text,
                disabledContentColor = colors.text2
            )
        ) { content() }
    }
}
