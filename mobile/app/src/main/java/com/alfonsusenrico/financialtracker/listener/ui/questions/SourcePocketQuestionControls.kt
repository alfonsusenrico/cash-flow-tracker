package com.alfonsusenrico.financialtracker.listener.ui.questions

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.selection.selectableGroup
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.rounded.Refresh
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.semantics.LiveRegionMode
import androidx.compose.ui.semantics.liveRegion
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.unit.dp
import com.alfonsusenrico.financialtracker.listener.data.network.model.AccountChoice
import com.alfonsusenrico.financialtracker.listener.data.network.model.ConfirmationQuestion
import com.alfonsusenrico.financialtracker.listener.data.sync.sourcePocketReplyMessage
import com.alfonsusenrico.financialtracker.listener.ui.components.ButtonKind
import com.alfonsusenrico.financialtracker.listener.ui.components.CompanionButton
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionTheme
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionType

/** The Jago pockets offered for one source-pocket question, as last loaded or cached. */
data class SourcePocketOptionState(
    val rowId: Long? = null,
    val questionId: String? = null,
    val options: List<AccountChoice> = emptyList(),
    val loading: Boolean = false,
    val cached: Boolean = false,
    val error: String? = null
)

/** "Dari kantong mana?": every eligible pocket, none preselected; the owner picks one, then confirms. */
@Composable
fun SourcePocketQuestionControls(
    question: ConfirmationQuestion,
    options: SourcePocketOptionState,
    replyState: String?,
    repliedAccountId: String?,
    busy: Boolean,
    canAnswer: Boolean,
    onRefresh: () -> Unit,
    onAnswer: (String) -> Unit
) {
    val colors = CompanionTheme.colors
    var selected by rememberSaveable(question.questionId, repliedAccountId) { mutableStateOf(repliedAccountId) }
    val pending = replyState != null && replyState !in setOf("invalid", "stale")
    val choices = options.options.takeIf { options.questionId == question.questionId }.orEmpty()
    val eligible = choices.any { it.id == selected }
    val enabled = !busy && canAnswer
    Column(Modifier.fillMaxWidth(), verticalArrangement = Arrangement.spacedBy(8.dp)) {
        if (replyState != null) {
            Text(
                sourcePocketReplyMessage(replyState), style = CompanionType.small, color = colors.text2,
                modifier = Modifier.semantics { liveRegion = LiveRegionMode.Polite }
            )
        }
        if (!pending) {
            if (options.loading) Text("Memuat kantong…", style = CompanionType.small, color = colors.text2)
            options.error?.let { Text(it, style = CompanionType.small, color = colors.text2) }
            if (!options.loading && choices.isEmpty() && options.error == null) {
                Text("Belum ada kantong yang dapat dipilih. Segarkan atau periksa rekening Jago.", style = CompanionType.small, color = colors.text2)
            }
            Column(Modifier.fillMaxWidth().selectableGroup(), verticalArrangement = Arrangement.spacedBy(8.dp)) {
                choices.forEach { choice ->
                    AnswerOption(
                        label = choice.label.orEmpty(), detail = null,
                        look = if (selected == choice.id) OptionLook.SELECTED else OptionLook.PLAIN,
                        enabled = enabled, onClick = { selected = choice.id },
                        modifier = Modifier.fillMaxWidth(), selected = selected == choice.id
                    )
                }
            }
            CompanionButton(
                "Konfirmasi kantong", { selected?.takeIf { eligible }?.let(onAnswer) }, Modifier.fillMaxWidth(),
                enabled = eligible && enabled && (!options.loading || options.cached)
            )
            CompanionButton(
                "Segarkan kantong", onRefresh, Modifier.fillMaxWidth(), ButtonKind.GHOST, Icons.Rounded.Refresh,
                enabled = enabled && !options.loading
            )
        }
        if (!canAnswer) Text("Hubungkan akun asal notifikasi di Pengaturan.", style = CompanionType.small, color = colors.text2)
    }
}
