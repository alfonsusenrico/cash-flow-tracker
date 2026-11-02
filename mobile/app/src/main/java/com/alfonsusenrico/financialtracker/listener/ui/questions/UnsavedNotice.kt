package com.alfonsusenrico.financialtracker.listener.ui.questions

import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.compose.ui.semantics.LiveRegionMode
import androidx.compose.ui.semantics.liveRegion
import androidx.compose.ui.semantics.semantics
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionTheme
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionType

/** Says that an answer given in the notification was not stored; the controls below stay open for a new one. */
@Composable
fun UnsavedNotice(text: String?) {
    if (text == null) return
    Text(
        text, style = CompanionType.small, color = CompanionTheme.colors.text,
        modifier = Modifier.semantics { liveRegion = LiveRegionMode.Polite }
    )
}
