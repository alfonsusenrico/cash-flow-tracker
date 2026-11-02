package com.alfonsusenrico.financialtracker.listener.ui.capture

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.outlined.MoreHoriz
import androidx.compose.material.icons.rounded.Refresh
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.OutlinedTextFieldDefaults
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.semantics.heading
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.text.input.ImeAction
import androidx.compose.ui.unit.dp
import com.alfonsusenrico.financialtracker.listener.data.network.model.AccountItemResponse
import com.alfonsusenrico.financialtracker.listener.data.network.model.CaptureQuestion
import com.alfonsusenrico.financialtracker.listener.data.textcapture.CaptureAnswer
import com.alfonsusenrico.financialtracker.listener.data.textcapture.CaptureText
import com.alfonsusenrico.financialtracker.listener.data.textcapture.TextCaptures
import com.alfonsusenrico.financialtracker.listener.ui.components.ButtonKind
import com.alfonsusenrico.financialtracker.listener.ui.components.CompanionButton
import com.alfonsusenrico.financialtracker.listener.ui.components.Formatting
import com.alfonsusenrico.financialtracker.listener.ui.model.CapturePresentation
import com.alfonsusenrico.financialtracker.listener.ui.model.kakeiboLabel
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionTheme
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionType

private val KIND_LABELS = mapOf("expense" to "Pengeluaran", "income" to "Pemasukan", "movement" to "Pindah saldo")
private val MISSING = mapOf("amount" to "Nominal?", "time" to "Kapan?", "kind" to "Jenis?", "account" to "Rekening?", "category" to "Kategori?")

/** Account types the backend accepts for a capture (`LIQUID_ACCOUNT_TYPES`). */
private val LIQUID_TYPES = setOf("cash", "bank", "ewallet", "wallet")

/** One follow-up question: what is known as tags, the missing part, then chips or a typed answer. */
@Composable
fun CaptureQuestionBody(state: CaptureSheetState, actions: CaptureSheetActions) {
    val item = state.capture ?: return
    val question = item.question ?: return
    val colors = CompanionTheme.colors
    val pending = TextCaptures.pendingAnswer(item.capture)
    TypedQuote(item.capture.text)
    ReceiptCard {
        FactTags(knownTags(item) + FactTag(MISSING[question.type] ?: "?", missing = true))
        Text(
            question.prompt.orEmpty(), style = CompanionType.questionTitle.copy(fontSize = CompanionType.questionTitle.fontSize * 0.9f),
            color = colors.text, modifier = Modifier.semantics { heading() }
        )
        if (pending != null && !state.busy) {
            Text("Jawaban sebelumnya belum diterima tracker.", style = CompanionType.small, color = colors.text2)
            CompanionButton("Kirim ulang jawaban", { actions.onResendAnswer(item.id) }, Modifier.fillMaxWidth(), ButtonKind.PRIMARY, Icons.Rounded.Refresh)
        } else {
            QuestionControls(item, question, state, actions)
        }
        WhyLine("Hanya bagian ini yang kurang.")
    }
    NoticeLine(state.notice)
}

@Composable
private fun QuestionControls(item: CapturePresentation, question: CaptureQuestion, state: CaptureSheetState, actions: CaptureSheetActions) {
    val questionId = question.questionId ?: return
    val enabled = !state.busy
    fun send(answer: CaptureAnswer) = actions.onAnswer(item.id, answer)
    when (question.type) {
        "amount" -> AmountAnswer(questionId, enabled, ::send)
        "time" -> TimeAnswer(question, questionId, enabled, ::send)
        "kind", "account", "category" -> {
            var other by rememberSaveable(questionId) { mutableStateOf(false) }
            ChipFlow {
                question.options.orEmpty().forEach { option ->
                    val id = option.id ?: return@forEach
                    val label = option.label ?: return@forEach
                    val detail = kakeiboLabel(option.kakeibo)
                    AnswerChip(
                        label, { send(CaptureAnswer(questionId, optionId = id)) }, enabled,
                        description = "Jawab: $label" + detail?.let { ", $it" }.orEmpty(), detail = detail
                    )
                }
                if (question.allowsOther == true && question.type != "kind") {
                    AnswerChip(
                        if (question.type == "account") "Rekening lain" else "Kategori lain", { other = !other }, enabled,
                        icon = Icons.Outlined.MoreHoriz
                    )
                }
            }
            if (other) OtherChoices(item, question, state, questionId, enabled, ::send)
        }
    }
}

/** The owner's own pick among every account or category the tracker allows, beyond the ranked chips. */
@Composable
private fun OtherChoices(
    item: CapturePresentation,
    question: CaptureQuestion,
    state: CaptureSheetState,
    questionId: String,
    enabled: Boolean,
    send: (CaptureAnswer) -> Unit
) {
    val offered = question.options.orEmpty().mapNotNull { it.id ?: it.categoryId }.toSet()
    val choices: List<Pair<String, String>> = if (question.type == "account") {
        liquidLeaves(state.accounts).filter { it.id !in offered }.map { it.id to it.name }
    } else {
        val kind = item.view?.summary?.type
        val offeredCategories = question.options.orEmpty().mapNotNull { it.categoryId }.toSet()
        state.categories.filter { it.kind.equals(kind, ignoreCase = true) && it.id !in offeredCategories }.map { it.id to it.name }
    }
    if (choices.isEmpty()) {
        Text("Daftar belum tersedia. Hubungkan tracker lalu buka lagi.", style = CompanionType.small, color = CompanionTheme.colors.text2)
        return
    }
    ChipFlow {
        choices.forEach { (id, name) ->
            AnswerChip(name, {
                send(
                    if (question.type == "account") CaptureAnswer(questionId, accountId = id)
                    else CaptureAnswer(questionId, categoryId = id)
                )
            }, enabled, description = "Jawab: $name")
        }
    }
}

/** "Jam berapa?" (or "Hari apa?"): the tracker's plausible moments as chips, and the owner's own words. */
@Composable
private fun TimeAnswer(question: CaptureQuestion, questionId: String, enabled: Boolean, send: (CaptureAnswer) -> Unit) {
    ChipFlow {
        question.options.orEmpty().forEach { option ->
            val id = option.id ?: return@forEach
            val label = option.label ?: return@forEach
            AnswerChip(label, { send(CaptureAnswer(questionId, optionId = id)) }, enabled, description = "Jawab: $label")
        }
    }
    if (question.allowsText == true) {
        TypedAnswer(questionId, enabled, "Atau tulis sendiri", "mis. tadi jam 2 siang", "Tulis kapan, maksimal ${CaptureText.ANSWER_MAX_LENGTH} karakter.", send)
    }
}

@Composable
private fun AmountAnswer(questionId: String, enabled: Boolean, send: (CaptureAnswer) -> Unit) =
    TypedAnswer(questionId, enabled, "Nominal", "mis. 25000 atau 25rb", "Tulis nominalnya, maksimal ${CaptureText.ANSWER_MAX_LENGTH} karakter.", send)

@Composable
private fun TypedAnswer(
    questionId: String,
    enabled: Boolean,
    label: String,
    placeholder: String,
    rule: String,
    send: (CaptureAnswer) -> Unit
) {
    val colors = CompanionTheme.colors
    var text by rememberSaveable(questionId) { mutableStateOf("") }
    var invalid by rememberSaveable(questionId) { mutableStateOf(false) }
    Column(verticalArrangement = Arrangement.spacedBy(8.dp)) {
        OutlinedTextField(
            value = text,
            onValueChange = {
                text = it
                invalid = false
            },
            label = { Text(label) },
            placeholder = { Text(placeholder) },
            singleLine = true,
            isError = invalid,
            enabled = enabled,
            supportingText = if (invalid) ({ Text(rule) }) else null,
            keyboardOptions = KeyboardOptions(imeAction = ImeAction.Done),
            shape = RoundedCornerShape(14.dp),
            colors = OutlinedTextFieldDefaults.colors(
                focusedBorderColor = colors.focus, unfocusedBorderColor = colors.borderStrong,
                focusedTextColor = colors.text, unfocusedTextColor = colors.text,
                focusedLabelColor = colors.text2, unfocusedLabelColor = colors.text2
            ),
            modifier = Modifier.fillMaxWidth()
        )
        CompanionButton("Simpan", {
            val normalized = CaptureText.normalize(text, CaptureText.ANSWER_MAX_LENGTH)
            if (normalized == null) invalid = true else send(CaptureAnswer(questionId, text = normalized))
        }, Modifier.fillMaxWidth(), ButtonKind.PRIMARY, enabled = enabled)
    }
}

/** What the tracker already knows, as tags before the missing one. */
private fun knownTags(item: CapturePresentation): List<FactTag> {
    val summary = item.view?.summary
    return listOfNotNull(
        summary?.amount?.let(Formatting::rupiah),
        summary?.timeLabel?.takeIf { it.isNotBlank() },
        summary?.description?.takeIf { it.isNotBlank() },
        KIND_LABELS[summary?.type]
    ).map(::FactTag)
}

/** Accounts a capture can use: liquid, not archived, and without pockets of their own. */
fun liquidLeaves(accounts: List<AccountItemResponse>): List<AccountItemResponse> {
    val parents = accounts.mapNotNull { it.parentId }.toSet()
    return accounts.filter { !it.isArchived && it.type in LIQUID_TYPES && it.id !in parents }
}
