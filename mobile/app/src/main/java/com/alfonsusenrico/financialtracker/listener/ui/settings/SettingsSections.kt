package com.alfonsusenrico.financialtracker.listener.ui.settings

import androidx.compose.foundation.background
import androidx.compose.foundation.border
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.selection.selectable
import androidx.compose.foundation.selection.selectableGroup
import androidx.compose.foundation.selection.toggleable
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.automirrored.outlined.OpenInNew
import androidx.compose.material.icons.outlined.WarningAmber
import androidx.compose.material.icons.rounded.Check
import androidx.compose.material3.Icon
import androidx.compose.material3.Switch
import androidx.compose.material3.SwitchDefaults
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.semantics.Role
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import androidx.compose.material.icons.outlined.Autorenew
import androidx.compose.material.icons.outlined.ErrorOutline
import androidx.compose.material.icons.outlined.Remove
import androidx.compose.material.icons.outlined.Schedule
import androidx.compose.ui.semantics.LiveRegionMode
import androidx.compose.ui.semantics.liveRegion
import com.alfonsusenrico.financialtracker.listener.data.health.SelfTestProgress
import com.alfonsusenrico.financialtracker.listener.data.health.SelfTestStep
import com.alfonsusenrico.financialtracker.listener.data.health.StepState
import com.alfonsusenrico.financialtracker.listener.ui.components.ButtonKind
import com.alfonsusenrico.financialtracker.listener.ui.components.CompanionButton
import com.alfonsusenrico.financialtracker.listener.ui.hub.OemHint
import com.alfonsusenrico.financialtracker.listener.ui.components.Note
import com.alfonsusenrico.financialtracker.listener.ui.model.StatusTone
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionTheme
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionType
import com.alfonsusenrico.financialtracker.listener.ui.theme.ThemeMode

/** A whole-row switch, so the label and its state are announced together. */
@Composable
fun SwitchRow(label: String, state: String, checked: Boolean, onChange: (Boolean) -> Unit, detail: String? = null) {
    val colors = CompanionTheme.colors
    Row(
        Modifier.fillMaxWidth().heightIn(min = 56.dp).toggleable(value = checked, role = Role.Switch, onValueChange = onChange)
            .padding(vertical = 4.dp),
        verticalAlignment = Alignment.CenterVertically,
        horizontalArrangement = Arrangement.spacedBy(12.dp)
    ) {
        Column(Modifier.weight(1f)) {
            Text(label, style = CompanionType.bodyStrong, color = colors.text)
            Text(state, style = CompanionType.meta.copy(fontSize = 13.sp), color = colors.text2)
            if (detail != null) Text(detail, style = CompanionType.meta.copy(fontFamily = FontFamily.Monospace, fontSize = 11.sp), color = colors.text2)
        }
        Switch(
            checked = checked, onCheckedChange = null,
            colors = SwitchDefaults.colors(
                checkedTrackColor = colors.switchOn, checkedThumbColor = colors.surface, checkedBorderColor = colors.switchOn,
                uncheckedTrackColor = colors.surface, uncheckedThumbColor = colors.switchOff, uncheckedBorderColor = colors.switchOff
            )
        )
    }
}

@Composable
fun SettingsNote(text: String) {
    Text(text, style = CompanionType.meta.copy(fontSize = 13.sp), color = CompanionTheme.colors.text2)
}

/** A row that opens an Android settings page, with its current state. */
@Composable
fun LinkRow(label: String, state: String, onClick: () -> Unit, actionLabel: String = "Buka pengaturan Android") {
    val colors = CompanionTheme.colors
    Row(
        Modifier.fillMaxWidth().heightIn(min = 56.dp).clip(RoundedCornerShape(12.dp))
            .clickable(role = Role.Button, onClickLabel = actionLabel, onClick = onClick)
            .padding(vertical = 6.dp),
        verticalAlignment = Alignment.CenterVertically,
        horizontalArrangement = Arrangement.spacedBy(12.dp)
    ) {
        Column(Modifier.weight(1f)) {
            Text(label, style = CompanionType.bodyStrong, color = colors.text)
            Text(state, style = CompanionType.meta.copy(fontSize = 13.sp), color = colors.text2)
        }
        Icon(Icons.AutoMirrored.Outlined.OpenInNew, null, tint = colors.text2, modifier = Modifier.size(18.dp))
    }
}

@Composable
fun HiosNote() {
    Note(
        "Ponsel TECNO / Infinix (HiOS): buka Phone Master → Kelola peluncuran aplikasi → matikan “Kelola otomatis” " +
            "untuk aplikasi ini. Tanpa ini, HiOS bisa membekukan pencatat meski baterai sudah dikecualikan.",
        StatusTone.WARN, Icons.Outlined.WarningAmber
    )
}

/** Tampilan: follow the system, or keep light or dark. */
@Composable
fun ThemeChooser(current: ThemeMode, onChoose: (ThemeMode) -> Unit) {
    val single = LocalDensity.current.fontScale >= 1.5f
    Column(verticalArrangement = Arrangement.spacedBy(6.dp)) {
        Text("Tema", style = CompanionType.small.copy(fontWeight = FontWeight(650)), color = CompanionTheme.colors.text2)
        if (single) {
            Column(Modifier.selectableGroup(), verticalArrangement = Arrangement.spacedBy(6.dp)) {
                ThemeMode.entries.forEach { ThemeOption(it, it == current, onChoose, Modifier.fillMaxWidth()) }
            }
        } else {
            Row(Modifier.fillMaxWidth().selectableGroup(), horizontalArrangement = Arrangement.spacedBy(6.dp)) {
                ThemeMode.entries.forEach { ThemeOption(it, it == current, onChoose, Modifier.weight(1f)) }
            }
        }
    }
}

@Composable
private fun ThemeOption(mode: ThemeMode, selected: Boolean, onChoose: (ThemeMode) -> Unit, modifier: Modifier) {
    val colors = CompanionTheme.colors
    val shape = RoundedCornerShape(14.dp)
    Row(
        modifier.heightIn(min = 48.dp).clip(shape)
            .then(if (selected) Modifier.background(colors.primary, shape) else Modifier.border(1.dp, colors.borderStrong, shape))
            .selectable(selected = selected, role = Role.RadioButton, onClick = { onChoose(mode) })
            .padding(horizontal = 10.dp),
        horizontalArrangement = Arrangement.Center,
        verticalAlignment = Alignment.CenterVertically
    ) {
        Text(
            mode.label, style = CompanionType.small.copy(fontWeight = FontWeight(600), fontSize = 14.sp),
            color = if (selected) colors.onPrimary else colors.text
        )
    }
}

/** This phone's battery saver and how to keep the listener running, from [OemHints]. */
@Composable
fun OemNote(hint: OemHint) {
    Note("HP-mu ${hint.brand} (${hint.system}). ${hint.steps}", StatusTone.WARN, Icons.Outlined.WarningAmber)
}

/** "Tes HP-ku": the setup self-test again, with its three steps while it runs. */
@Composable
fun SelfTestRow(progress: SelfTestProgress, onRun: () -> Unit) {
    val colors = CompanionTheme.colors
    Column(verticalArrangement = Arrangement.spacedBy(6.dp)) {
        if (progress.running || progress.outcome != null) SelfTestChecklist(progress)
        CompanionButton(
            if (progress.running) "Lagi ngetes…" else "Tes HP-ku", onRun, Modifier.fillMaxWidth(), ButtonKind.GHOST,
            busy = progress.running, enabled = !progress.running
        )
        Text(
            "Aku kirim notifikasi tes ke diriku sendiri. Ini bukan transaksi, jadi nggak bakal kecatat.",
            style = CompanionType.meta.copy(fontSize = 13.sp), color = colors.text2
        )
    }
}

/** The self-test's three lines, each with an icon and a word for its state. */
@Composable
fun SelfTestChecklist(progress: SelfTestProgress) {
    val colors = CompanionTheme.colors
    Column(Modifier.semantics { liveRegion = LiveRegionMode.Polite }, verticalArrangement = Arrangement.spacedBy(4.dp)) {
        SelfTestStep.entries.forEach { step ->
            val state = progress.steps[step] ?: StepState.WAITING
            Row(
                Modifier.fillMaxWidth().heightIn(min = 32.dp).semantics(mergeDescendants = true) {},
                verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(10.dp)
            ) {
                Icon(
                    when (state) {
                        StepState.DONE -> Icons.Rounded.Check
                        StepState.FAILED -> Icons.Outlined.ErrorOutline
                        StepState.RUNNING -> Icons.Outlined.Autorenew
                        StepState.SKIPPED -> Icons.Outlined.Remove
                        StepState.WAITING -> Icons.Outlined.Schedule
                    }, null, tint = if (state == StepState.FAILED) colors.danger else colors.text, modifier = Modifier.size(18.dp)
                )
                Text(stepLabel(step), style = CompanionType.small, color = colors.text, modifier = Modifier.weight(1f))
                Text(stateLabel(state), style = CompanionType.small.copy(fontWeight = FontWeight(650)), color = colors.text2)
            }
        }
    }
}

fun stepLabel(step: SelfTestStep) = when (step) {
    SelfTestStep.POST -> "Nampilin notifikasi tes"
    SelfTestStep.CAPTURE -> "Nangkep pakai pendengarku"
    SelfTestStep.SERVER -> "Ngabarin web kalau HP-mu siap"
}

fun stateLabel(state: StepState) = when (state) {
    StepState.WAITING -> "Nunggu"
    StepState.RUNNING -> "Lagi jalan"
    StepState.DONE -> "Beres"
    StepState.FAILED -> "Gagal"
    StepState.SKIPPED -> "Dilewati"
}
