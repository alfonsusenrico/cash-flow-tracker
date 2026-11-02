package com.alfonsusenrico.financialtracker.listener.ui.questions

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.selection.selectableGroup
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import com.alfonsusenrico.financialtracker.listener.data.network.model.AccountItemResponse
import com.alfonsusenrico.financialtracker.listener.data.network.model.MappingProposal
import com.alfonsusenrico.financialtracker.listener.ui.components.ButtonKind
import com.alfonsusenrico.financialtracker.listener.ui.components.CompanionButton
import com.alfonsusenrico.financialtracker.listener.ui.components.Formatting
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionTheme
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionType

/**
 * "Rekening yang mana?": the proposed account first, its alternatives, and any other account of the
 * tracker; the owner picks one, then confirms. The choice is remembered for later notifications.
 */
@Composable
fun MappingQuestionControls(
    proposal: MappingProposal,
    accounts: List<AccountItemResponse>,
    busy: Boolean,
    canAnswer: Boolean,
    /** Says that a confirmation from the notification did not go through; the choice stays open. */
    unsavedNotice: String? = null,
    onConfirm: (accountId: String) -> Unit
) {
    val colors = CompanionTheme.colors
    var choice by rememberSaveable(proposal.proposed?.id) { mutableStateOf(proposal.proposed?.id) }
    var showOthers by rememberSaveable(proposal.proposed?.id) { mutableStateOf(false) }
    val enabled = !busy && canAnswer
    val offered = listOfNotNull(proposal.proposed) + proposal.alternatives.orEmpty()
    val offeredIds = offered.mapNotNull { it.id }.toSet()
    val others = accounts.filter { !it.isArchived && it.type !in setOf("investment", "stock", "crypto") && it.id !in offeredIds }
    Column(Modifier.fillMaxWidth(), verticalArrangement = Arrangement.spacedBy(8.dp)) {
        UnsavedNotice(unsavedNotice)
        Column(Modifier.fillMaxWidth().selectableGroup(), verticalArrangement = Arrangement.spacedBy(8.dp)) {
            offered.forEach { option ->
                AnswerOption(
                    label = option.label.orEmpty(),
                    detail = "Saran".takeIf { option.id == proposal.proposed?.id },
                    look = if (choice == option.id) OptionLook.SELECTED else OptionLook.PLAIN,
                    enabled = enabled, onClick = { choice = option.id },
                    modifier = Modifier.fillMaxWidth(), selected = choice == option.id
                )
            }
            if (showOthers) {
                if (others.isEmpty()) Text("Tidak ada rekening lain yang dimuat.", style = CompanionType.small, color = colors.text2)
                others.forEach { account ->
                    AnswerOption(
                        label = account.name,
                        detail = account.balance.takeIf { it > 0 }?.let { "Saldo: ${Formatting.rupiah(it)}" },
                        look = if (choice == account.id) OptionLook.SELECTED else OptionLook.PLAIN,
                        enabled = enabled, onClick = { choice = account.id },
                        modifier = Modifier.fillMaxWidth(), selected = choice == account.id
                    )
                }
            }
        }
        if (!showOthers) {
            CompanionButton("Rekening lain", { showOthers = true }, Modifier.fillMaxWidth(), ButtonKind.GHOST, enabled = enabled)
        }
        CompanionButton(
            "Konfirmasi Rekening", { choice?.let(onConfirm) }, Modifier.fillMaxWidth(),
            enabled = enabled && choice != null
        )
        if (!canAnswer) Text("Hubungkan akun asal notifikasi di Pengaturan.", style = CompanionType.small, color = colors.text2)
    }
}
