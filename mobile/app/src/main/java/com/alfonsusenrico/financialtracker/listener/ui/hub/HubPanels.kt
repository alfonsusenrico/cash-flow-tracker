package com.alfonsusenrico.financialtracker.listener.ui.hub

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.style.TextAlign
import androidx.compose.ui.unit.dp
import com.alfonsusenrico.financialtracker.listener.data.health.SelfTestProgress
import com.alfonsusenrico.financialtracker.listener.data.report.WebLinks
import com.alfonsusenrico.financialtracker.listener.ui.components.CardTitle
import com.alfonsusenrico.financialtracker.listener.ui.components.CompanionCard
import com.alfonsusenrico.financialtracker.listener.ui.components.Formatting
import com.alfonsusenrico.financialtracker.listener.ui.model.SyncHealth
import com.alfonsusenrico.financialtracker.listener.ui.settings.LinkRow
import com.alfonsusenrico.financialtracker.listener.ui.settings.SelfTestRow
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionTheme
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionType
import com.alfonsusenrico.financialtracker.listener.ui.theme.TABULAR_FIGURES

/** A tracker page offered under "Buka di web". */
data class WebLink(val label: String, val detail: String, val path: String)

fun webLinks(reportPath: String?): List<WebLink> = listOf(
    WebLink("Laporan bulanan", "Ringkasan bulan dan Kakeibo", reportPath ?: WebLinks.REPORT),
    WebLink("Buku Kas", "Semua transaksi; ubah lengkap di sini", WebLinks.LEDGER),
    WebLink("Tinjau", "Yang perlu dicek dari notifikasi", WebLinks.REVIEW),
    WebLink("Pengaturan web", "Perangkat, kunci API, kategori", WebLinks.SETTINGS)
)

/** "Buka di web": each page is confirmed before the phone leaves for the browser. */
@Composable
fun WebSheet(links: List<WebLink>, onOpen: (WebLink) -> Unit) {
    val colors = CompanionTheme.colors
    var pending by rememberSaveable { mutableStateOf<String?>(null) }
    Column(
        Modifier.fillMaxWidth().verticalScroll(rememberScrollState()).padding(start = 12.dp, end = 12.dp, bottom = 24.dp),
        verticalArrangement = Arrangement.spacedBy(10.dp)
    ) {
        Text("Uangmu dibaca di web. Halaman kebuka di browser.", style = CompanionType.small, color = colors.text2, modifier = Modifier.padding(horizontal = 6.dp))
        CompanionCard(spacing = 0.dp) {
            links.forEachIndexed { index, link ->
                if (index > 0) HorizontalDivider(color = colors.border)
                LinkRow(link.label, link.detail, { pending = link.path }, actionLabel = "Buka di browser")
            }
        }
    }
    val chosen = links.firstOrNull { it.path == pending }
    if (chosen != null) {
        AlertDialog(
            onDismissRequest = { pending = null },
            title = { Text("Buka ${chosen.label} di browser?") },
            text = { Text("Kamu keluar dari aplikasi ini sebentar. Masuk pakai akun web-mu kalau diminta.") },
            confirmButton = { TextButton(onClick = { pending = null; onOpen(chosen) }) { Text("Buka", color = colors.text) } },
            dismissButton = { TextButton(onClick = { pending = null }) { Text("Batal", color = colors.text) } },
            containerColor = colors.surface, titleContentColor = colors.text, textContentColor = colors.text2
        )
    }
}

/** What "Rincian status" lists: the listener, permissions and sending, as this phone sees them. */
data class StatusFacts(
    val health: SyncHealth,
    val notificationAccess: Boolean,
    val batteryUnrestricted: Boolean,
    val postNotifications: Boolean,
    val selfTestLine: String
)

/** "Rincian status": the figures behind the status card, the Android pages that fix them, and "Tes HP-ku". */
@Composable
fun StatusSheet(
    facts: StatusFacts,
    selfTest: SelfTestProgress,
    onOpenAccess: () -> Unit,
    onOpenBattery: () -> Unit,
    onOpenNotifications: () -> Unit,
    onSelfTest: () -> Unit
) {
    val colors = CompanionTheme.colors
    val health = facts.health
    fun at(millis: Long) = if (millis > 0) Formatting.dateTime(millis) else "-"
    Column(
        Modifier.fillMaxWidth().verticalScroll(rememberScrollState()).padding(start = 12.dp, end = 12.dp, bottom = 24.dp),
        verticalArrangement = Arrangement.spacedBy(10.dp)
    ) {
        CompanionCard(spacing = 0.dp) {
            CardTitle("Sekarang")
            listOf(
                "Pendengar" to if (health.listenerConnected) "Aktif sejak ${at(health.connectedAt)}" else "Terputus sejak ${at(health.disconnectedAt)}",
                "Tangkapan terakhir" to at(health.lastCaptureAt),
                "Kirim terakhir" to at(health.lastSyncAt),
                "Jumlah terputus" to health.disconnectCount.toString(),
                "Gagal disimpan" to "${health.failedCaptures} (terakhir ${at(health.lastCaptureFailureAt)})",
                "Dibuang di ponsel" to health.dropped.toString(),
                "Tes terakhir" to facts.selfTestLine
            ).forEachIndexed { index, (label, value) ->
                if (index > 0) HorizontalDivider(color = colors.border)
                Row(
                    Modifier.fillMaxWidth().padding(vertical = 10.dp).semantics(mergeDescendants = true) {},
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
        CompanionCard(spacing = 6.dp) {
            CardTitle("Izin")
            LinkRow("Akses notifikasi", if (facts.notificationAccess) "Nyala" else "Mati", onOpenAccess)
            LinkRow("Jalan di latar", if (facts.batteryUnrestricted) "Boleh" else "Belum boleh", onOpenBattery)
            LinkRow("Notifikasi dariku", if (facts.postNotifications) "Nyala" else "Mati", onOpenNotifications)
            SelfTestRow(selfTest, onSelfTest)
        }
    }
}
