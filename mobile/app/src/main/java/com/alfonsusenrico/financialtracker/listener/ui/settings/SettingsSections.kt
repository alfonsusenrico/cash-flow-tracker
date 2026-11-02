package com.alfonsusenrico.financialtracker.listener.ui.settings

import androidx.compose.foundation.background
import androidx.compose.foundation.border
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.IntrinsicSize
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxHeight
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.selection.selectable
import androidx.compose.foundation.selection.selectableGroup
import androidx.compose.foundation.selection.toggleable
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.automirrored.outlined.OpenInNew
import androidx.compose.material.icons.outlined.ExpandLess
import androidx.compose.material.icons.outlined.ExpandMore
import androidx.compose.material.icons.outlined.WarningAmber
import androidx.compose.material.icons.rounded.Check
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.Icon
import androidx.compose.material3.Switch
import androidx.compose.material3.SwitchDefaults
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.semantics.Role
import androidx.compose.ui.semantics.heading
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.style.TextAlign
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import com.alfonsusenrico.financialtracker.listener.ui.components.BentoTile
import com.alfonsusenrico.financialtracker.listener.ui.components.CompanionCard
import com.alfonsusenrico.financialtracker.listener.ui.components.Formatting
import com.alfonsusenrico.financialtracker.listener.ui.components.Note
import com.alfonsusenrico.financialtracker.listener.ui.model.StatusTone
import com.alfonsusenrico.financialtracker.listener.ui.model.SyncHealth
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionTheme
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionType
import com.alfonsusenrico.financialtracker.listener.ui.theme.TABULAR_FIGURES
import com.alfonsusenrico.financialtracker.listener.ui.theme.ThemeMode

/** The four health tiles (`.f-hgrid`); a tile's icon and word carry its state, not its colour. */
@Composable
fun HealthTiles(health: SyncHealth) {
    val single = LocalDensity.current.fontScale >= 1.5f
    val tiles = listOf(
        Triple("Pendengar notifikasi", if (health.listenerConnected) "Aktif" else "Terputus", health.listenerConnected),
        Triple("Sinkron terakhir", if (health.lastSyncAt > 0) Formatting.dateTime(health.lastSyncAt) else "Belum pernah", null),
        Triple("Gagal disimpan", "${health.failedCaptures}", health.failedCaptures == 0),
        Triple("Dibuang di ponsel", "${health.dropped}", null)
    )
    Column(verticalArrangement = Arrangement.spacedBy(10.dp)) {
        (if (single) tiles.chunked(1) else tiles.chunked(2)).forEach { pair ->
            Row(Modifier.fillMaxWidth().height(IntrinsicSize.Min), horizontalArrangement = Arrangement.spacedBy(10.dp)) {
                pair.forEach { (label, value, ok) -> HealthTile(label, value, ok, Modifier.weight(1f).fillMaxHeight()) }
            }
        }
    }
}

@Composable
private fun HealthTile(label: String, value: String, ok: Boolean?, modifier: Modifier) {
    val colors = CompanionTheme.colors
    BentoTile(label, modifier, minHeight = 96.dp) {
        Row(verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(6.dp)) {
            if (ok != null) {
                Icon(if (ok) Icons.Rounded.Check else Icons.Outlined.WarningAmber, null, tint = colors.text, modifier = Modifier.size(18.dp))
            }
            Text(value, style = CompanionType.tileState, color = colors.text)
        }
    }
}

/** Every listener figure the companion keeps, behind one expandable row. */
@Composable
fun ListenerDetails(health: SyncHealth) {
    val colors = CompanionTheme.colors
    var open by rememberSaveable { mutableStateOf(false) }
    fun at(millis: Long) = if (millis > 0) Formatting.dateTime(millis) else "-"
    CompanionCard(spacing = 4.dp) {
        Row(
            Modifier.fillMaxWidth().heightIn(min = 48.dp).clip(RoundedCornerShape(12.dp))
                .clickable(role = Role.Button, onClickLabel = if (open) "Tutup" else "Buka") { open = !open }
                .semantics { heading() },
            verticalAlignment = Alignment.CenterVertically
        ) {
            Text("Rincian pendengar", style = CompanionType.cardTitle, color = colors.text, modifier = Modifier.weight(1f))
            Icon(if (open) Icons.Outlined.ExpandLess else Icons.Outlined.ExpandMore, null, tint = colors.text)
        }
        if (open) {
            listOf(
                "Sekarang" to if (health.listenerConnected) "Terhubung" else "Terputus",
                "Terhubung sejak" to at(health.connectedAt),
                "Terputus terakhir" to at(health.disconnectedAt),
                "Jumlah terputus" to health.disconnectCount.toString(),
                "Tangkapan terakhir" to at(health.lastCaptureAt),
                "Gagal disimpan" to "${health.failedCaptures} (terakhir ${at(health.lastCaptureFailureAt)})",
                "Dibuang di ponsel" to health.dropped.toString(),
                "Kirim langsung terakhir" to "${at(health.lastDirectSyncAt)} (${health.lastDirectSyncOutcome ?: "-"})"
            ).forEachIndexed { index, (label, value) ->
                if (index > 0) HorizontalDivider(color = colors.border)
                Row(
                    Modifier.fillMaxWidth().padding(vertical = 8.dp).semantics(mergeDescendants = true) {},
                    horizontalArrangement = Arrangement.spacedBy(12.dp)
                ) {
                    Text(label, style = CompanionType.small, color = colors.text2)
                    Text(
                        value, style = CompanionType.small.copy(fontWeight = FontWeight(600), fontFeatureSettings = TABULAR_FIGURES),
                        color = colors.text, modifier = Modifier.weight(1f), textAlign = TextAlign.End
                    )
                }
            }
        }
    }
}

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
fun LinkRow(label: String, state: String, onClick: () -> Unit) {
    val colors = CompanionTheme.colors
    Row(
        Modifier.fillMaxWidth().heightIn(min = 56.dp).clip(RoundedCornerShape(12.dp))
            .clickable(role = Role.Button, onClickLabel = "Buka pengaturan Android", onClick = onClick)
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
