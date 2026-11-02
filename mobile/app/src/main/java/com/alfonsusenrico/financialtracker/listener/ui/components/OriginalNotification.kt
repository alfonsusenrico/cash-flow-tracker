package com.alfonsusenrico.financialtracker.listener.ui.components

import androidx.compose.foundation.background
import androidx.compose.foundation.border
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxHeight
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.IntrinsicSize
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.sizeIn
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.semantics.clearAndSetSemantics
import androidx.compose.ui.semantics.contentDescription
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import com.alfonsusenrico.financialtracker.listener.data.model.RawNotificationEntity
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionTheme
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionType
import com.alfonsusenrico.financialtracker.listener.ui.theme.TABULAR_FIGURES
import java.time.LocalDate

/** The notification exactly as the app posted it: title, then the full text. */
fun originalText(row: RawNotificationEntity): String =
    listOfNotNull(row.title?.takeIf { it.isNotBlank() }, (row.bigText ?: row.bodyText)?.takeIf { it.isNotBlank() })
        .joinToString("\n")
        .ifBlank { "(Tanpa isi teks)" }

/**
 * The original notification as a quote (`.f-notif`): app tile, app name, day and time, the full text and
 * an accent rule on the left. TalkBack reads it as one item.
 */
@Composable
fun OriginalNotification(row: RawNotificationEntity, today: LocalDate, modifier: Modifier = Modifier) {
    val colors = CompanionTheme.colors
    val shape = RoundedCornerShape(18.dp)
    val whenText = "${Formatting.relativeDay(Formatting.localDate(row.postTime), today)} ${Formatting.time(row.postTime)}"
    val text = originalText(row)
    Row(
        modifier = modifier
            .fillMaxWidth()
            .height(IntrinsicSize.Min)
            .clip(shape)
            .background(colors.surface)
            .border(1.dp, colors.border, shape)
            .semantics(mergeDescendants = true) {}
    ) {
        Box(Modifier.width(4.dp).fillMaxHeight().background(colors.accentLine))
        Column(Modifier.padding(horizontal = 14.dp, vertical = 12.dp), verticalArrangement = Arrangement.spacedBy(6.dp)) {
            Row(
                verticalAlignment = Alignment.CenterVertically,
                horizontalArrangement = Arrangement.spacedBy(8.dp),
                modifier = Modifier.clearAndSetSemantics { contentDescription = "Notifikasi asli dari ${row.appLabel}, $whenText" }
            ) {
                Box(
                    Modifier.sizeIn(minWidth = 28.dp, minHeight = 28.dp).clip(RoundedCornerShape(8.dp)).background(colors.surface2)
                        .padding(horizontal = 3.dp),
                    contentAlignment = Alignment.Center
                ) {
                    Text(appInitials(row.appLabel), color = colors.text, fontSize = 10.5.sp, fontWeight = FontWeight(750), maxLines = 1)
                }
                Text(row.appLabel, style = CompanionType.meta.copy(fontSize = 13.sp, fontWeight = FontWeight(700)), color = colors.text)
                Text(
                    "· $whenText", style = CompanionType.meta.copy(fontSize = 13.sp, fontFeatureSettings = TABULAR_FIGURES),
                    color = colors.text2
                )
            }
            Text(text, style = CompanionType.body.copy(fontSize = 15.5.sp, lineHeight = 23.sp, fontFeatureSettings = TABULAR_FIGURES), color = colors.text)
        }
    }
}

private fun appInitials(label: String): String = label.filter { it.isLetter() }.take(3).ifEmpty { "?" }
