package com.alfonsusenrico.financialtracker.listener.ui.questions

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.ExperimentalLayoutApi
import androidx.compose.foundation.layout.FlowRow
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.outlined.Edit
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.focus.FocusRequester
import androidx.compose.ui.focus.focusRequester
import androidx.compose.ui.semantics.LiveRegionMode
import androidx.compose.ui.semantics.liveRegion
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.unit.dp
import com.alfonsusenrico.financialtracker.listener.data.network.model.PurposeQuestion
import com.alfonsusenrico.financialtracker.listener.data.sync.PurposeAnswers
import com.alfonsusenrico.financialtracker.listener.data.sync.PurposeQuestions
import com.alfonsusenrico.financialtracker.listener.data.sync.purposeReplyMessage
import com.alfonsusenrico.financialtracker.listener.ui.components.ButtonKind
import com.alfonsusenrico.financialtracker.listener.ui.components.CompanionButton
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionTheme
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionType

/** "Apa ini?" in Tanya: every suggestion as an option, plus "Tulis sendiri" for a free-text answer. */
@OptIn(ExperimentalLayoutApi::class)
@Composable
fun PurposeQuestionControls(
    question: PurposeQuestion,
    replyState: String?,
    replyError: String?,
    busy: Boolean,
    canAnswer: Boolean,
    /** Says that an answer from the notification was not stored; the options stay open for a new one. */
    unsavedNotice: String? = null,
    onAnswer: (suggestionId: String?, text: String?) -> Unit
) {
    val colors = CompanionTheme.colors
    var writing by rememberSaveable(question.questionId) { mutableStateOf(false) }
    var text by rememberSaveable(question.questionId) { mutableStateOf("") }
    var invalid by rememberSaveable(question.questionId) { mutableStateOf(false) }
    val focus = remember(question.questionId) { FocusRequester() }
    val limit = question.textMaxLength ?: PurposeQuestions.TEXT_MAX_LENGTH
    val pending = replyState != null && replyState != "failed"
    val enabled = !busy && canAnswer
    Column(Modifier.fillMaxWidth(), verticalArrangement = Arrangement.spacedBy(10.dp)) {
        if (replyState != null) {
            Text(
                purposeReplyMessage(replyState, replyError), style = CompanionType.small, color = colors.text2,
                modifier = Modifier.semantics { liveRegion = LiveRegionMode.Polite }
            )
        }
        if (!pending) {
            UnsavedNotice(unsavedNotice)
            val suggestions = question.suggestions.orEmpty()
            val options: List<@Composable (Modifier) -> Unit> = suggestions.mapIndexed { index, suggestion ->
                { modifier ->
                    val description = "Jawab: ${suggestion.label}" + suggestion.categoryName?.let { ", kategori $it" }.orEmpty()
                    AnswerOption(
                        label = suggestion.label.orEmpty(),
                        detail = listOfNotNull(suggestion.categoryName, "saran".takeIf { index == 0 }).joinToString(" · ").ifEmpty { null },
                        look = if (index == 0) OptionLook.SUGGESTED else OptionLook.PLAIN,
                        enabled = enabled,
                        onClick = { onAnswer(suggestion.id, null) },
                        modifier = modifier,
                        description = description
                    )
                }
            }
            val write: List<@Composable (Modifier) -> Unit> = if (question.allowsText == true) listOf { modifier ->
                AnswerOption(
                    "Tulis sendiri", null, if (writing) OptionLook.SELECTED else OptionLook.PLAIN, enabled,
                    onClick = { writing = !writing }, modifier = modifier, leadingIcon = Icons.Outlined.Edit
                )
            } else emptyList()
            OptionGrid(options + write)
            if (writing && question.allowsText == true) {
                LaunchedEffect(Unit) { focus.requestFocus() }
                OutlinedTextField(
                    value = text,
                    onValueChange = {
                        text = it
                        invalid = false
                    },
                    label = { Text("Jawaban kamu") },
                    placeholder = { Text("mis. sewa lapangan futsal") },
                    enabled = enabled, isError = invalid, maxLines = 3,
                    supportingText = {
                        Text(if (invalid) PurposeAnswers.textRule(limit) else "${text.codePointCount(0, text.length)}/$limit")
                    },
                    shape = RoundedCornerShape(14.dp),
                    modifier = Modifier.fillMaxWidth().focusRequester(focus)
                )
                FlowRow(horizontalArrangement = Arrangement.spacedBy(8.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
                    CompanionButton("Batal", { writing = false }, kind = ButtonKind.GHOST, enabled = enabled)
                    CompanionButton(
                        "Kirim jawaban",
                        onClick = {
                            val normalized = PurposeQuestions.normalizeText(text, limit)
                            if (normalized == null) {
                                invalid = true
                                focus.requestFocus()
                            } else {
                                onAnswer(null, normalized)
                            }
                        },
                        enabled = enabled,
                        modifier = Modifier.weight(1f)
                    )
                }
            }
        }
        if (!canAnswer) {
            Text("Hubungkan akun asal notifikasi di Pengaturan.", style = CompanionType.small, color = colors.text2)
        }
    }
}
