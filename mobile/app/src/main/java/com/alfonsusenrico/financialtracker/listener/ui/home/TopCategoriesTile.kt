package com.alfonsusenrico.financialtracker.listener.ui.home

import androidx.compose.foundation.background
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.semantics.clearAndSetSemantics
import androidx.compose.ui.semantics.contentDescription
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import com.alfonsusenrico.financialtracker.listener.data.network.model.LedgerCashFlowSummary
import com.alfonsusenrico.financialtracker.listener.data.network.model.TopCategory
import com.alfonsusenrico.financialtracker.listener.ui.components.BentoTile
import com.alfonsusenrico.financialtracker.listener.ui.components.Formatting
import com.alfonsusenrico.financialtracker.listener.ui.shell.LedgerSummaryState
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionTheme
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionType
import com.alfonsusenrico.financialtracker.listener.ui.theme.TABULAR_FIGURES
import java.time.ZoneId

/** The summary's period, as both tracker tiles show it: "Periode 29 Okt 2026, 00.00 – 29 Okt 2026, 10.00 (Asia/Jakarta)". */
internal fun ledgerPeriod(summary: LedgerCashFlowSummary): String {
    val zone = ZoneId.of(summary.timezone)
    return "Periode ${Formatting.dateTime(summary.start!!, zone)} – ${Formatting.dateTime(summary.end!!, zone)} (${summary.timezone})"
}

/** How fresh the shown summary is; both tracker tiles share it because they come from one response. */
internal fun ledgerFreshness(state: LedgerSummaryState, summary: LedgerCashFlowSummary): String {
    val generated = Formatting.dateTime(summary.generatedAt!!, ZoneId.of(summary.timezone))
    return when {
        state.stale -> "Belum diperbarui. Terakhir berhasil $generated"
        state.cached -> "Tersimpan di ponsel, terakhir diperbarui $generated"
        else -> "Diperbarui $generated"
    }
}

/** The tracker's Kakeibo pillar names, as the web shows them. */
internal fun pillarLabel(kakeiboType: String?): String? = when (kakeiboType) {
    "need" -> "Kebutuhan"
    "want" -> "Keinginan"
    "culture" -> "Budaya"
    "unexpected" -> "Tak Terduga"
    "saving" -> "Tabungan"
    else -> null
}

/**
 * "Kategori teratas" from the tracker's summary: its top three categories for the same period, with
 * the summary's freshness. Nothing here is computed on the phone; a tracker that does not send the
 * categories gets no tile at all.
 */
@Composable
internal fun TopCategoriesTile(state: LedgerSummaryState, modifier: Modifier = Modifier) {
    val summary = state.summary ?: return
    val categories = summary.shownTopCategories() ?: return
    val colors = CompanionTheme.colors
    BentoTile("Kategori teratas", modifier.fillMaxWidth()) {
        if (categories.isEmpty()) {
            Text("Belum ada pengeluaran pada periode ini", style = CompanionType.small, color = colors.text2)
        } else {
            // Sorted by outflow descending, so the first entry is the largest.
            val largest = categories.first().outflow!!.coerceAtLeast(1)
            Column(verticalArrangement = Arrangement.spacedBy(10.dp)) {
                categories.forEachIndexed { index, category -> CategoryRow(category, highlight = index == 0, largest = largest) }
            }
        }
        Text(ledgerPeriod(summary), style = CompanionType.meta, color = colors.text2)
        Text(ledgerFreshness(state, summary), style = CompanionType.meta, color = colors.text2)
        if (state.error != null) Text(state.error, style = CompanionType.meta, color = colors.text2)
    }
}

@Composable
private fun CategoryRow(category: TopCategory, highlight: Boolean, largest: Long) {
    val colors = CompanionTheme.colors
    val amount = Formatting.rupiah(category.outflow!!)
    val pillar = pillarLabel(category.kakeiboType)
    val spoken = listOfNotNull(category.name, amount, pillar).joinToString(", ")
    Column(Modifier.fillMaxWidth().clearAndSetSemantics { contentDescription = spoken }, verticalArrangement = Arrangement.spacedBy(4.dp)) {
        Row(Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.spacedBy(10.dp)) {
            Text(category.name!!, style = CompanionType.small.copy(fontSize = 14.sp), color = colors.text, modifier = Modifier.weight(1f))
            Text(
                amount, style = CompanionType.small.copy(fontSize = 14.sp, fontWeight = FontWeight(700), fontFeatureSettings = TABULAR_FIGURES),
                color = colors.text
            )
        }
        if (pillar != null) Text(pillar, style = CompanionType.meta.copy(fontSize = 12.5.sp), color = colors.text2)
        Box(Modifier.fillMaxWidth().height(8.dp).clip(RoundedCornerShape(8.dp)).background(colors.surface2)) {
            val share = category.outflow.toFloat() / largest
            if (share > 0f) {
                Box(
                    Modifier.fillMaxWidth(share).height(8.dp).clip(RoundedCornerShape(8.dp))
                        .background(if (highlight) colors.barHighlight else colors.bar)
                )
            }
        }
    }
}
