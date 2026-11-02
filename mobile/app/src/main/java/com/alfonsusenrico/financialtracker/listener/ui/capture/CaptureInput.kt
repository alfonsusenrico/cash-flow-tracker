package com.alfonsusenrico.financialtracker.listener.ui.capture

import androidx.compose.foundation.background
import androidx.compose.foundation.border
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.text.BasicTextField
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.automirrored.rounded.ArrowForward
import androidx.compose.material.icons.outlined.AutoAwesome
import androidx.compose.material.icons.outlined.Schedule
import androidx.compose.material.icons.outlined.WifiOff
import androidx.compose.material3.Icon
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.focus.FocusRequester
import androidx.compose.ui.focus.focusRequester
import androidx.compose.ui.focus.onFocusChanged
import androidx.compose.ui.graphics.SolidColor
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.semantics.LiveRegionMode
import androidx.compose.ui.semantics.Role
import androidx.compose.ui.semantics.clearAndSetSemantics
import androidx.compose.ui.semantics.contentDescription
import androidx.compose.ui.semantics.heading
import androidx.compose.ui.semantics.liveRegion
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.text.TextRange
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.input.ImeAction
import androidx.compose.ui.text.input.KeyboardCapitalization
import androidx.compose.ui.text.input.TextFieldValue
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import com.alfonsusenrico.financialtracker.listener.data.network.model.CaptureSuggestion
import com.alfonsusenrico.financialtracker.listener.data.textcapture.CaptureText
import com.alfonsusenrico.financialtracker.listener.ui.components.ButtonKind
import com.alfonsusenrico.financialtracker.listener.ui.components.CompanionButton
import com.alfonsusenrico.financialtracker.listener.ui.components.Formatting
import com.alfonsusenrico.financialtracker.listener.ui.components.Note
import com.alfonsusenrico.financialtracker.listener.ui.components.SectionLabel
import com.alfonsusenrico.financialtracker.listener.ui.model.StatusTone
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionTheme
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionType
import com.alfonsusenrico.financialtracker.listener.ui.theme.TABULAR_FIGURES

const val CAPTURE_INPUT_TAG = "capture-input"
const val SUGGESTIONS_HEADING = "Dari riwayatmu · yang biasa kamu catat sekitar jam ini"

/**
 * The input (`.f-capbox`) and, below it, the owner's own history suggestions. A suggestion only fills
 * the input and focuses it; sending is always the owner's tap on "Proses dengan AI".
 */
@Composable
fun CaptureInput(state: CaptureSheetState, actions: CaptureSheetActions) {
    val colors = CompanionTheme.colors
    val focus = remember { FocusRequester() }
    var value by rememberSaveable(stateSaver = TextFieldValue.Saver) { mutableStateOf(TextFieldValue()) }
    LaunchedEffect(state.prefill) {
        val text = state.prefill ?: return@LaunchedEffect
        value = TextFieldValue(text, TextRange(text.length))
        actions.onPrefilled()
    }
    var focused by remember { mutableStateOf(false) }
    var error by rememberSaveable { mutableStateOf<String?>(null) }
    val shape = RoundedCornerShape(20.dp)

    fun submit() {
        val text = CaptureText.normalize(value.text)
        when {
            value.text.isBlank() -> error = "Tulis transaksinya dulu, mis. “bayar parkir 10rb”."
            text == null -> error = "Maksimal ${CaptureText.MAX_LENGTH} karakter, tanpa karakter khusus."
            else -> {
                error = null
                actions.onSubmit(text) { error = "Maksimal ${CaptureText.MAX_LENGTH} karakter, tanpa karakter khusus." }
            }
        }
    }

    Column(verticalArrangement = Arrangement.spacedBy(12.dp)) {
        Column(
            Modifier.fillMaxWidth().clip(shape).background(colors.surface)
                .border(if (focused) 3.dp else 1.dp, if (focused) colors.focus else colors.border, shape)
                .padding(horizontal = 14.dp, vertical = 12.dp),
            verticalArrangement = Arrangement.spacedBy(10.dp)
        ) {
            SectionLabel("Tulis transaksi")
            Box(Modifier.fillMaxWidth().heightIn(min = 76.dp)) {
                BasicTextField(
                    value = value,
                    onValueChange = {
                        value = it
                        error = null
                    },
                    textStyle = CompanionType.body.copy(fontSize = 19.sp, lineHeight = 25.sp, fontWeight = FontWeight(600), color = colors.text),
                    cursorBrush = SolidColor(colors.text),
                    keyboardOptions = KeyboardOptions(capitalization = KeyboardCapitalization.None, imeAction = ImeAction.Default),
                    enabled = !state.busy,
                    modifier = Modifier.fillMaxWidth().heightIn(min = 76.dp).focusRequester(focus)
                        .onFocusChanged { focused = it.isFocused }
                        .semantics { contentDescription = "Tulis transaksi" }
                        .testTag(CAPTURE_INPUT_TAG),
                    decorationBox = { field ->
                        if (value.text.isEmpty()) {
                            Text(
                                "bayar parkir ptc 10rb",
                                style = CompanionType.body.copy(fontSize = 19.sp, lineHeight = 25.sp, fontWeight = FontWeight(600)),
                                color = colors.text2
                            )
                        }
                        field()
                    }
                )
            }
            CompanionButton("Proses dengan AI", ::submit, Modifier.fillMaxWidth(), ButtonKind.ACCENT, Icons.Outlined.AutoAwesome, enabled = !state.busy)
        }
        if (error != null) {
            Text(
                error!!, style = CompanionType.small, color = colors.danger,
                modifier = Modifier.semantics { liveRegion = LiveRegionMode.Assertive }
            )
        }
        WhyLine("AI mengisi nominal, kategori, dan rekening. Yang kurang ditanyakan satu per satu.")
        Suggestions(state.suggestions) { suggestion ->
            val text = suggestion.text ?: return@Suggestions
            value = TextFieldValue(text, TextRange(text.length))
            error = null
            focus.requestFocus()
        }
        if (!state.online) {
            Note(
                if (state.autoSync) "Ponsel offline. Catatan akan antre dan dikirim saat online."
                else "Ponsel offline. Catatan akan antre; Kirim otomatis mati, jadi kirim sendiri lewat Kirim.",
                StatusTone.MUTED, Icons.Outlined.WifiOff
            )
        }
        NoticeLine(state.notice)
    }
}

@Composable
private fun Suggestions(state: SuggestionState, onPick: (CaptureSuggestion) -> Unit) {
    val colors = CompanionTheme.colors
    when (state) {
        is SuggestionState.Loaded -> if (state.items.isNotEmpty()) {
            Column(verticalArrangement = Arrangement.spacedBy(6.dp)) {
                Row(
                    Modifier.semantics(mergeDescendants = true) { heading() },
                    horizontalArrangement = Arrangement.spacedBy(6.dp), verticalAlignment = Alignment.CenterVertically
                ) {
                    Icon(Icons.Outlined.Schedule, contentDescription = null, tint = colors.text2, modifier = Modifier.size(14.dp))
                    Text(SUGGESTIONS_HEADING, style = CompanionType.meta, color = colors.text2)
                }
                state.items.forEach { SuggestionRow(it, onPick) }
            }
        }
        SuggestionState.Unavailable -> Text("Saran dari riwayat belum bisa dimuat.", style = CompanionType.meta, color = colors.text2)
        SuggestionState.Loading -> Unit
    }
}

@Composable
private fun SuggestionRow(suggestion: CaptureSuggestion, onPick: (CaptureSuggestion) -> Unit) {
    val colors = CompanionTheme.colors
    val shape = RoundedCornerShape(14.dp)
    val title = listOfNotNull(suggestion.label, suggestion.amount?.let(Formatting::rupiah)).joinToString(" · ")
    val detail = listOfNotNull(suggestion.accountName, suggestion.categoryName, suggestion.reason).joinToString(" · ").ifEmpty { null }
    val description = "Isi dengan “${suggestion.text}”. $title" + detail?.let { ". $it" }.orEmpty()
    Row(
        Modifier.fillMaxWidth().heightIn(min = 52.dp).clip(shape).background(colors.surface).border(1.dp, colors.border, shape)
            .clickable(role = Role.Button, onClickLabel = "Isi input", onClick = { onPick(suggestion) })
            .clearAndSetSemantics { contentDescription = description }
            .padding(horizontal = 12.dp, vertical = 6.dp),
        verticalAlignment = Alignment.CenterVertically,
        horizontalArrangement = Arrangement.spacedBy(10.dp)
    ) {
        Column(Modifier.weight(1f)) {
            Text(title, style = CompanionType.bodyStrong.copy(fontSize = 14.5.sp, fontFeatureSettings = TABULAR_FIGURES), color = colors.text)
            if (detail != null) Text(detail, style = CompanionType.meta, color = colors.text2)
        }
        Icon(Icons.AutoMirrored.Rounded.ArrowForward, contentDescription = null, tint = colors.text, modifier = Modifier.size(18.dp))
    }
}
