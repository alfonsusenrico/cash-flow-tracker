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
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.rounded.Check
import androidx.compose.material3.Icon
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.OutlinedTextFieldDefaults
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.semantics.Role
import androidx.compose.ui.semantics.heading
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import com.alfonsusenrico.financialtracker.listener.data.capture.LabelChange
import com.alfonsusenrico.financialtracker.listener.data.capture.PillarChoice
import com.alfonsusenrico.financialtracker.listener.data.network.model.CategoryItemResponse
import com.alfonsusenrico.financialtracker.listener.ui.components.ButtonKind
import com.alfonsusenrico.financialtracker.listener.ui.components.CompanionButton
import com.alfonsusenrico.financialtracker.listener.ui.model.kakeiboLabel
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionTheme
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionType

/** "Belum diklasifikasi": the pillar left open, stored as null by the tracker. */
const val UNCLASSIFIED_LABEL = "Belum diklasifikasi"

/**
 * "Perbaiki label": category, Kakeibo pillar (expenses only, with "Belum diklasifikasi") and merchant. Nothing
 * is preselected, so only what the owner touches is sent; the current category is shown as context.
 */
@OptIn(ExperimentalLayoutApi::class)
@Composable
fun LabelForm(
    type: String,
    currentCategory: String?,
    categories: List<CategoryItemResponse>,
    busy: Boolean,
    onCancel: () -> Unit,
    onSubmit: (LabelChange) -> Unit
) {
    val colors = CompanionTheme.colors
    var categoryId by rememberSaveable { mutableStateOf<String?>(null) }
    var pillar by rememberSaveable { mutableStateOf<String?>(null) }
    var merchant by rememberSaveable { mutableStateOf("") }
    var problem by rememberSaveable { mutableStateOf<String?>(null) }
    val choices = categories.filter { it.kind.equals(type, ignoreCase = true) }
    Column(verticalArrangement = Arrangement.spacedBy(12.dp)) {
        Text(
            "Yang kamu benerin di sini bikin aku belajar. Nominal, rekening, dan tanggal diubah di web.",
            style = CompanionType.small, color = colors.text2
        )
        FieldTitle("Kategori")
        currentCategory?.takeIf { it.isNotBlank() }?.let { Text("Sekarang: $it", style = CompanionType.small, color = colors.text2) }
        if (choices.isEmpty()) {
            Text("Daftar kategori belum tersedia. Sambungin tracker lalu buka lagi.", style = CompanionType.small, color = colors.text2)
        } else {
            FlowRow(Modifier.selectableGroup(), horizontalArrangement = Arrangement.spacedBy(8.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
                choices.forEach { category ->
                    ChoiceChip(category.name, categoryId == category.id, !busy) {
                        categoryId = if (categoryId == category.id) null else category.id
                    }
                }
            }
        }
        if (type == "expense") {
            FieldTitle("Pilar Kakeibo")
            FlowRow(Modifier.selectableGroup(), horizontalArrangement = Arrangement.spacedBy(8.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
                (PillarChoice.CODES + UNCLASSIFIED).forEach { code ->
                    val label = if (code == UNCLASSIFIED) UNCLASSIFIED_LABEL else kakeiboLabel(code) ?: code
                    ChoiceChip(label, pillar == code, !busy) { pillar = if (pillar == code) null else code }
                }
            }
        }
        FieldTitle("Merchant")
        OutlinedTextField(
            value = merchant, onValueChange = { merchant = it; problem = null }, enabled = !busy, singleLine = true,
            label = { Text("Nama tempat atau toko") }, placeholder = { Text("Kosongin kalau udah benar") },
            shape = RoundedCornerShape(14.dp),
            colors = OutlinedTextFieldDefaults.colors(
                focusedBorderColor = colors.focus, unfocusedBorderColor = colors.borderStrong,
                focusedTextColor = colors.text, unfocusedTextColor = colors.text,
                focusedLabelColor = colors.text2, unfocusedLabelColor = colors.text2
            ),
            modifier = Modifier.fillMaxWidth()
        )
        problem?.let { Text(it, style = CompanionType.small, color = colors.danger) }
        CompanionButton("Simpan label", {
            val change = LabelChange(
                categoryId = categoryId,
                pillar = when (pillar) {
                    null -> null
                    UNCLASSIFIED -> PillarChoice.Unclassified
                    else -> PillarChoice.Pillar(pillar!!)
                },
                merchant = merchant.trim().takeIf { it.isNotEmpty() }
            )
            val reason = change.problem(type)
            if (reason != null) problem = reason else onSubmit(change)
        }, Modifier.fillMaxWidth(), ButtonKind.PRIMARY, enabled = !busy, busy = busy)
        CompanionButton("Batal", onCancel, Modifier.fillMaxWidth(), ButtonKind.GHOST, enabled = !busy)
    }
}

private const val UNCLASSIFIED = "unclassified"

@Composable
private fun FieldTitle(text: String) {
    Text(
        text, style = CompanionType.small.copy(fontWeight = FontWeight(700)), color = CompanionTheme.colors.text,
        modifier = Modifier.semantics { heading() }
    )
}

/** A chip that is chosen or not, read by TalkBack as a radio button with its state. */
@Composable
private fun ChoiceChip(label: String, selected: Boolean, enabled: Boolean, onClick: () -> Unit) {
    val colors = CompanionTheme.colors
    val shape = RoundedCornerShape(14.dp)
    Row(
        Modifier.heightIn(min = 48.dp).clip(shape)
            .background(if (selected) colors.primary else colors.surface2)
            .border(1.dp, if (selected) colors.primary else colors.borderStrong, shape)
            .selectable(selected = selected, enabled = enabled, role = Role.RadioButton, onClick = onClick)
            .padding(horizontal = 12.dp),
        verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(6.dp)
    ) {
        if (selected) Icon(Icons.Rounded.Check, null, tint = colors.onPrimary, modifier = Modifier.size(16.dp))
        Text(label, style = CompanionType.small.copy(fontWeight = FontWeight(600)), color = if (selected) colors.onPrimary else colors.text)
    }
}
