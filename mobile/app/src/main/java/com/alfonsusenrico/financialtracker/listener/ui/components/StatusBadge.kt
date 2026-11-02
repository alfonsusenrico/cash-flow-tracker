package com.alfonsusenrico.financialtracker.listener.ui.components

import androidx.compose.foundation.background
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.automirrored.outlined.HelpOutline
import androidx.compose.material.icons.automirrored.outlined.Send
import androidx.compose.material.icons.outlined.PhoneAndroid
import androidx.compose.material.icons.outlined.Schedule
import androidx.compose.material.icons.outlined.VisibilityOff
import androidx.compose.material.icons.outlined.WarningAmber
import androidx.compose.material.icons.rounded.Check
import androidx.compose.material.icons.rounded.Refresh
import androidx.compose.material.icons.rounded.SwapHoriz
import androidx.compose.material3.Icon
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.graphics.vector.ImageVector
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import com.alfonsusenrico.financialtracker.listener.ui.model.RowStatus
import com.alfonsusenrico.financialtracker.listener.ui.model.StatusIcon
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionType

/** A status pill: icon and text on a tone, so the meaning never rests on colour alone. */
@Composable
fun StatusBadge(status: RowStatus, modifier: Modifier = Modifier) {
    val (bg, fg) = toneColors(status.tone)
    Row(
        modifier = modifier.clip(RoundedCornerShape(999.dp)).background(bg)
            .padding(start = 7.dp, end = 9.dp, top = 3.dp, bottom = 3.dp)
            .semantics(mergeDescendants = true) {},
        verticalAlignment = Alignment.CenterVertically,
        horizontalArrangement = Arrangement.spacedBy(5.dp)
    ) {
        Icon(status.icon.vector(), contentDescription = null, tint = fg, modifier = Modifier.size(14.dp))
        Text(status.label, color = fg, style = CompanionType.meta.copy(fontWeight = FontWeight(600), fontSize = 12.5.sp))
    }
}

fun StatusIcon.vector(): ImageVector = when (this) {
    StatusIcon.CHECK -> Icons.Rounded.Check
    StatusIcon.ALERT -> Icons.Outlined.WarningAmber
    StatusIcon.HELP -> Icons.AutoMirrored.Outlined.HelpOutline
    StatusIcon.SWAP -> Icons.Rounded.SwapHoriz
    StatusIcon.PHONE -> Icons.Outlined.PhoneAndroid
    StatusIcon.EYE_OFF -> Icons.Outlined.VisibilityOff
    StatusIcon.CLOCK -> Icons.Outlined.Schedule
    StatusIcon.REFRESH -> Icons.Rounded.Refresh
    StatusIcon.SEND -> Icons.AutoMirrored.Outlined.Send
}
