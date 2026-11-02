package com.alfonsusenrico.financialtracker.listener.ui.detail

import androidx.compose.foundation.background
import androidx.compose.foundation.border
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.ExperimentalLayoutApi
import androidx.compose.foundation.layout.FlowRow
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.selection.selectable
import androidx.compose.foundation.selection.selectableGroup
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.automirrored.rounded.CallMade
import androidx.compose.material.icons.automirrored.rounded.CallReceived
import androidx.compose.material3.DropdownMenuItem
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.ExposedDropdownMenuBox
import androidx.compose.material3.ExposedDropdownMenuDefaults
import androidx.compose.material3.Icon
import androidx.compose.material3.MenuAnchorType
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.OutlinedTextFieldDefaults
import androidx.compose.material3.Text
import androidx.compose.material3.TextFieldColors
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.graphics.vector.ImageVector
import androidx.compose.ui.semantics.Role
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.input.KeyboardType
import androidx.compose.ui.unit.dp
import com.alfonsusenrico.financialtracker.listener.data.network.model.AccountItemResponse
import com.alfonsusenrico.financialtracker.listener.data.network.model.CategoryItemResponse
import com.alfonsusenrico.financialtracker.listener.service.NotificationProcessor
import com.alfonsusenrico.financialtracker.listener.ui.components.ButtonKind
import com.alfonsusenrico.financialtracker.listener.ui.components.CompanionButton
import com.alfonsusenrico.financialtracker.listener.ui.components.Formatting
import com.alfonsusenrico.financialtracker.listener.ui.model.RowPresentation
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionTheme
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionType
import com.alfonsusenrico.financialtracker.listener.ui.theme.TABULAR_FIGURES

/** Which of the three existing write paths the inline form uses. */
enum class FormMode(val pageTitle: String, val cardTitle: String) {
    EDIT_RECORDED("Ubah transaksi", "Ubah data di tracker"),
    MANUAL_RECORD("Catat manual", "Isi data yang benar"),
    SEND_UNSENT("Kirim ke tracker", "Kirim ke tracker")
}

/**
 * The form's starting values. [key] identifies the record, so typed values survive recomposition but not a
 * switch to another record.
 */
data class FormSeed(val key: String, val amount: Long?, val type: String, val notes: String) {
    companion object {
        /** A notification's values: the tracker's record, else what the phone detected in the text. */
        fun of(item: RowPresentation): FormSeed {
            val row = item.row
            val summary = NotificationProcessor.extractDetectionSummary(
                packageName = row.packageName, title = row.title, bodyText = row.bodyText, bigText = row.bigText
            )
            val recorded = item.recorded
            val type = recorded?.type ?: when {
                row.eventClass != null -> row.eventClass
                summary.eventClass == "income" -> "income"
                else -> "expense"
            }
            return FormSeed(
                key = "notification-${row.id}",
                amount = recorded?.amount ?: row.expectedAmount?.toLong() ?: summary.expectedAmount?.toLong(),
                type = type,
                notes = recorded?.description ?: row.labelNotes ?: summary.expectedCounterparty ?: row.title ?: ""
            )
        }
    }
}

/** What the form submits; account and category are null when the mode does not carry them. */
data class TransactionDraft(
    val amount: Long,
    val type: String,
    val accountId: String?,
    val categoryId: String?,
    val notes: String?
)

/**
 * The inline form of the detail page and of a typed capture. Its fields, defaults and checks are the
 * detail sheet's: a recorded edit keeps the account and category unless changed, a manual record needs
 * both, and an unsent notification carries only type, amount and note because the ingest request carries
 * nothing else. [canSubmit] is whether the record still allows this write.
 */
@OptIn(ExperimentalMaterial3Api::class, ExperimentalLayoutApi::class)
@Composable
fun TransactionForm(
    seed: FormSeed,
    mode: FormMode,
    canSubmit: Boolean,
    accounts: List<AccountItemResponse>,
    categories: List<CategoryItemResponse>,
    busy: Boolean,
    onCancel: () -> Unit,
    onInvalid: (String) -> Unit,
    onSubmit: (TransactionDraft) -> Unit
) {
    val colors = CompanionTheme.colors
    val key = seed.key
    val initialAmount = seed.amount ?: 0L
    var amountText by rememberSaveable(key) { mutableStateOf(if (initialAmount > 0) initialAmount.toString() else "") }
    var type by rememberSaveable(key) { mutableStateOf(seed.type) }
    var notes by rememberSaveable(key) { mutableStateOf(seed.notes) }
    val pickers = mode != FormMode.SEND_UNSENT
    val activeAccounts = remember(accounts) {
        accounts.filter { !it.isArchived && it.type !in setOf("investment", "stock", "crypto") }
    }
    var account by remember(key) { mutableStateOf<AccountItemResponse?>(null) }
    val typedCategories = remember(categories, type) { categories.filter { it.kind.equals(type, ignoreCase = true) } }
    var category by remember(key, type) { mutableStateOf<CategoryItemResponse?>(null) }
    val fieldColors = OutlinedTextFieldDefaults.colors(
        focusedBorderColor = colors.focus, unfocusedBorderColor = colors.borderStrong,
        focusedTextColor = colors.text, unfocusedTextColor = colors.text,
        focusedContainerColor = colors.surface, unfocusedContainerColor = colors.surface,
        focusedLabelColor = colors.text2, unfocusedLabelColor = colors.text2,
        focusedPrefixColor = colors.text, unfocusedPrefixColor = colors.text
    )

    Column(verticalArrangement = Arrangement.spacedBy(14.dp)) {
        FieldLabel("Jenis transaksi")
        Row(Modifier.fillMaxWidth().selectableGroup(), horizontalArrangement = Arrangement.spacedBy(8.dp)) {
            TypeOption("Keluar", Icons.AutoMirrored.Rounded.CallMade, type == "expense", Modifier.weight(1f)) { type = "expense" }
            TypeOption("Masuk", Icons.AutoMirrored.Rounded.CallReceived, type == "income", Modifier.weight(1f)) { type = "income" }
        }

        OutlinedTextField(
            value = amountText,
            onValueChange = { amountText = it.filter(Char::isDigit) },
            label = { Text("Nominal") },
            prefix = { Text("Rp ", style = CompanionType.bodyStrong) },
            placeholder = { Text("0") },
            singleLine = true,
            textStyle = CompanionType.bodyStrong.copy(fontFeatureSettings = TABULAR_FIGURES),
            keyboardOptions = KeyboardOptions(keyboardType = KeyboardType.Number),
            supportingText = amountText.toLongOrNull()?.takeIf { it > 0 }?.let { { Text(Formatting.rupiah(it)) } },
            shape = RoundedCornerShape(14.dp),
            colors = fieldColors,
            modifier = Modifier.fillMaxWidth()
        )

        if (pickers) {
            Picker(
                label = "Rekening",
                value = account?.name ?: if (mode == FormMode.EDIT_RECORDED) "Pertahankan rekening" else "Pilih Rekening",
                options = activeAccounts.map { PickerOption(it.name, it.balance.takeIf { b -> b > 0 }?.let { b -> "Saldo: ${Formatting.rupiah(b)}" }, it) },
                empty = "Tidak ada rekening",
                colors = fieldColors,
                onPick = { account = it }
            )
            Picker(
                label = "Kategori",
                value = category?.name ?: if (mode == FormMode.EDIT_RECORDED) "Pertahankan kategori" else "Pilih Kategori",
                options = typedCategories.map { PickerOption(it.name, null, it) },
                empty = "Tidak ada kategori",
                colors = fieldColors,
                onPick = { category = it }
            )
        } else {
            Text(
                "Tracker memilih rekening dan kategori saat mencatat notifikasi ini. Ubahlah setelah tercatat.",
                style = CompanionType.small, color = colors.text2
            )
        }

        OutlinedTextField(
            value = notes,
            onValueChange = { notes = it },
            label = { Text("Catatan (opsional)") },
            placeholder = { Text("mis. Kopi Kenangan, Makan Siang") },
            singleLine = true,
            shape = RoundedCornerShape(14.dp),
            colors = fieldColors,
            modifier = Modifier.fillMaxWidth()
        )

        FlowRow(Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.spacedBy(8.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
            CompanionButton("Batal", onCancel, kind = ButtonKind.GHOST, enabled = !busy)
            CompanionButton(
                text = when (mode) {
                    FormMode.EDIT_RECORDED -> "Perbarui"
                    FormMode.MANUAL_RECORD -> "Catat ke Tracker"
                    FormMode.SEND_UNSENT -> "Kirim Notifikasi ke Tracker"
                },
                onClick = {
                    val amount = amountText.toLongOrNull() ?: 0L
                    when {
                        amount <= 0 -> onInvalid("Nominal harus lebih dari 0")
                        mode == FormMode.MANUAL_RECORD && (account == null || category == null) ->
                            onInvalid("Pilih rekening dan kategori")
                        else -> onSubmit(
                            TransactionDraft(amount, type, account?.id.takeIf { pickers }, category?.id.takeIf { pickers }, notes.ifBlank { null })
                        )
                    }
                },
                kind = ButtonKind.PRIMARY,
                enabled = !busy && canSubmit,
                busy = busy,
                modifier = Modifier.weight(1f)
            )
        }
    }
}

@Composable
private fun FieldLabel(text: String) {
    Text(text, style = CompanionType.small.copy(fontWeight = FontWeight(650)), color = CompanionTheme.colors.text2)
}

@Composable
private fun TypeOption(label: String, icon: ImageVector, selected: Boolean, modifier: Modifier, onSelect: () -> Unit) {
    val colors = CompanionTheme.colors
    val shape = RoundedCornerShape(14.dp)
    Row(
        modifier
            .heightIn(min = 48.dp)
            .clip(shape)
            .background(if (selected) colors.primary else colors.surface)
            .border(1.dp, if (selected) colors.primary else colors.borderStrong, shape)
            .selectable(selected = selected, role = Role.RadioButton, onClick = onSelect)
            .padding(horizontal = 10.dp),
        horizontalArrangement = Arrangement.spacedBy(6.dp, Alignment.CenterHorizontally),
        verticalAlignment = Alignment.CenterVertically
    ) {
        val fg = if (selected) colors.onPrimary else colors.text
        Icon(icon, contentDescription = null, tint = fg, modifier = Modifier.size(18.dp))
        Text(label, style = CompanionType.button, color = fg)
    }
}

private class PickerOption<T>(val label: String, val detail: String?, val value: T)

@OptIn(ExperimentalMaterial3Api::class)
@Composable
private fun <T> Picker(
    label: String,
    value: String,
    options: List<PickerOption<T>>,
    empty: String,
    colors: TextFieldColors,
    onPick: (T) -> Unit
) {
    var expanded by remember { mutableStateOf(false) }
    ExposedDropdownMenuBox(expanded = expanded, onExpandedChange = { expanded = !expanded }) {
        OutlinedTextField(
            value = value,
            onValueChange = {},
            readOnly = true,
            label = { Text(label) },
            trailingIcon = { ExposedDropdownMenuDefaults.TrailingIcon(expanded = expanded) },
            shape = RoundedCornerShape(14.dp),
            colors = colors,
            modifier = Modifier.menuAnchor(MenuAnchorType.PrimaryNotEditable).fillMaxWidth()
        )
        ExposedDropdownMenu(expanded = expanded, onDismissRequest = { expanded = false }) {
            if (options.isEmpty()) {
                DropdownMenuItem(text = { Text(empty) }, onClick = { expanded = false })
            }
            options.forEach { option ->
                DropdownMenuItem(
                    text = {
                        Column {
                            Text(option.label)
                            option.detail?.let { Text(it, style = CompanionType.meta, color = CompanionTheme.colors.text2) }
                        }
                    },
                    onClick = {
                        onPick(option.value)
                        expanded = false
                    }
                )
            }
        }
    }
}
