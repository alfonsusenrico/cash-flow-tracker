package com.alfonsusenrico.financialtracker.listener.ui.questions

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.ExperimentalLayoutApi
import androidx.compose.foundation.layout.FlowRow
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.focus.FocusRequester
import androidx.compose.ui.focus.focusRequester
import androidx.compose.ui.semantics.LiveRegionMode
import androidx.compose.ui.semantics.clearAndSetSemantics
import androidx.compose.ui.semantics.contentDescription
import androidx.compose.ui.semantics.liveRegion
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.em
import androidx.compose.ui.unit.sp
import com.alfonsusenrico.financialtracker.listener.data.network.model.ConfirmationQuestion
import com.alfonsusenrico.financialtracker.listener.data.sync.SenderQuestions
import com.alfonsusenrico.financialtracker.listener.data.sync.senderReplyMessage
import com.alfonsusenrico.financialtracker.listener.ui.components.ButtonKind
import com.alfonsusenrico.financialtracker.listener.ui.components.CompanionButton
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionTheme
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionType

/** "Siapa pengirimnya?": the masked sender, a name field, and recording without a name. */
@OptIn(ExperimentalLayoutApi::class)
@Composable
fun SenderQuestionControls(
    question: ConfirmationQuestion,
    replyState: String?,
    busy: Boolean,
    canAnswer: Boolean,
    /** Says that an answer from the notification was not stored; the field stays open for a new one. */
    unsavedNotice: String? = null,
    onAnswer: (String?) -> Unit
) {
    val colors = CompanionTheme.colors
    var name by rememberSaveable(question.questionId) { mutableStateOf("") }
    var invalid by rememberSaveable(question.questionId) { mutableStateOf(false) }
    val focus = remember(question.questionId) { FocusRequester() }
    val pending = replyState != null && replyState != "invalid"
    val enabled = !busy && canAnswer
    Column(Modifier.fillMaxWidth(), verticalArrangement = Arrangement.spacedBy(10.dp)) {
        val masked = question.maskedSender.orEmpty()
        Text(
            masked,
            style = CompanionType.body.copy(fontFamily = FontFamily.Monospace, fontSize = 19.sp, fontWeight = FontWeight(700), letterSpacing = 0.04.em),
            color = colors.text,
            modifier = Modifier.clearAndSetSemantics { contentDescription = "Nama pengirim tersamar: $masked" }
        )
        if (replyState != null) {
            Text(
                senderReplyMessage(replyState), style = CompanionType.small, color = colors.text2,
                modifier = Modifier.semantics { liveRegion = LiveRegionMode.Polite }
            )
        }
        if (!pending) {
            UnsavedNotice(unsavedNotice)
            OutlinedTextField(
                value = name,
                onValueChange = {
                    name = it
                    invalid = false
                },
                label = { Text("Nama pengirim") }, singleLine = true, enabled = enabled,
                isError = invalid,
                supportingText = {
                    Text(
                        if (invalid) "Isi nama pengirim, maksimal 80 karakter."
                        else "Diingat untuk transfer berikutnya dari pengirim yang sama ke ${question.receivingAccount?.label}."
                    )
                },
                shape = RoundedCornerShape(14.dp),
                modifier = Modifier.fillMaxWidth().focusRequester(focus)
            )
            FlowRow(horizontalArrangement = Arrangement.spacedBy(8.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
                CompanionButton("Catat tanpa nama", { onAnswer(null) }, kind = ButtonKind.GHOST, enabled = enabled)
                CompanionButton(
                    "Konfirmasi nama",
                    onClick = {
                        val normalized = SenderQuestions.normalizeName(name)
                        if (normalized == null) {
                            invalid = true
                            focus.requestFocus()
                        } else {
                            onAnswer(normalized)
                        }
                    },
                    enabled = enabled,
                    modifier = Modifier.weight(1f)
                )
            }
        }
        if (!canAnswer) {
            Text("Hubungkan akun asal notifikasi di Pengaturan.", style = CompanionType.small, color = colors.text2)
        }
    }
}
