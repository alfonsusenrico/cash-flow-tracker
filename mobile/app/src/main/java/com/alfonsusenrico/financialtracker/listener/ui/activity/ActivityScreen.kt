package com.alfonsusenrico.financialtracker.listener.ui.activity

import androidx.compose.foundation.background
import androidx.compose.foundation.border
import androidx.compose.foundation.clickable
import androidx.compose.foundation.horizontalScroll
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.ExperimentalLayoutApi
import androidx.compose.foundation.layout.FlowRow
import androidx.compose.foundation.layout.PaddingValues
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.selection.selectable
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.outlined.Delete
import androidx.compose.material.icons.outlined.History
import androidx.compose.material.icons.outlined.Layers
import androidx.compose.material.icons.rounded.Check
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.Icon
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.semantics.Role
import androidx.compose.ui.semantics.clearAndSetSemantics
import androidx.compose.ui.semantics.CustomAccessibilityAction
import androidx.compose.ui.semantics.contentDescription
import androidx.compose.ui.semantics.customActions
import androidx.compose.ui.semantics.onClick
import androidx.compose.ui.semantics.heading
import androidx.compose.ui.semantics.role
import androidx.compose.ui.semantics.selected
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import com.alfonsusenrico.financialtracker.listener.ui.components.ButtonKind
import com.alfonsusenrico.financialtracker.listener.ui.components.CardTitle
import com.alfonsusenrico.financialtracker.listener.ui.components.CompanionButton
import com.alfonsusenrico.financialtracker.listener.ui.components.CompanionCard
import com.alfonsusenrico.financialtracker.listener.ui.components.DELETE_ACTION_LABEL
import com.alfonsusenrico.financialtracker.listener.ui.components.Formatting
import com.alfonsusenrico.financialtracker.listener.ui.components.MoneyText
import com.alfonsusenrico.financialtracker.listener.ui.components.PageHeader
import com.alfonsusenrico.financialtracker.listener.ui.components.SectionLabel
import com.alfonsusenrico.financialtracker.listener.ui.components.StatusBadge
import com.alfonsusenrico.financialtracker.listener.ui.components.SwipeToDeleteItem
import com.alfonsusenrico.financialtracker.listener.ui.components.moneyDescription
import com.alfonsusenrico.financialtracker.listener.ui.components.toneColors
import com.alfonsusenrico.financialtracker.listener.ui.capture.CaptureViewModel
import com.alfonsusenrico.financialtracker.listener.ui.model.ActivityFilter
import com.alfonsusenrico.financialtracker.listener.ui.model.CAPTURE_SOURCE_LABEL
import com.alfonsusenrico.financialtracker.listener.ui.model.CapturePresentation
import com.alfonsusenrico.financialtracker.listener.ui.model.HABIT_TAG
import com.alfonsusenrico.financialtracker.listener.ui.model.MoneyDirection
import com.alfonsusenrico.financialtracker.listener.ui.model.RowPresentation
import com.alfonsusenrico.financialtracker.listener.ui.model.StatusTone
import com.alfonsusenrico.financialtracker.listener.ui.shell.CompanionLists
import com.alfonsusenrico.financialtracker.listener.ui.shell.CompanionViewModel
import com.alfonsusenrico.financialtracker.listener.ui.shell.ShellActions
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionTheme
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionType
import com.alfonsusenrico.financialtracker.listener.ui.theme.TABULAR_FIGURES
import java.time.LocalDate

@Composable
fun ActivityRoute(
    viewModel: CompanionViewModel,
    captureViewModel: CaptureViewModel,
    actions: ShellActions,
    onOpenRow: (rowId: Long, toQuestions: Boolean) -> Unit,
    onOpenCapture: (CapturePresentation) -> Unit
) {
    val lists by viewModel.lists.collectAsState()
    val captureRows by captureViewModel.visible.collectAsState()
    val captures = remember(captureRows) { captureRows.orEmpty().map(CapturePresentation::of) }
    val filter by viewModel.selectedFilter.collectAsState()
    val total by viewModel.totalCount.collectAsState()
    val duplicates by viewModel.hiddenDuplicates.collectAsState()
    ActivityScreen(
        lists = lists,
        filter = filter,
        totalCount = total,
        hiddenDuplicates = duplicates,
        today = LocalDate.now(),
        onFilter = viewModel::setFilter,
        onOpenRow = onOpenRow,
        onDelete = actions.deleteWithUndo,
        onClearIgnored = {
            viewModel.clearIgnored { count -> actions.message("$count notifikasi dihapus dari daftar") }
        },
        captures = captures,
        onOpenCapture = onOpenCapture
    )
}

/** One feed entry: a stored notification or a typed capture, placed by its time. */
private sealed interface FeedEntry {
    val key: String
    val moment: Long
    val localDate: LocalDate

    data class Notification(val item: RowPresentation) : FeedEntry {
        override val key get() = "n-${item.row.id}"
        override val moment get() = item.row.postTime
        override val localDate: LocalDate get() = item.localDate
    }

    data class Capture(val item: CapturePresentation) : FeedEntry {
        override val key get() = "c-${item.id}"
        override val moment get() = item.moment
        override val localDate: LocalDate get() = item.localDate
    }
}

@Composable
fun ActivityScreen(
    lists: CompanionLists,
    filter: ActivityFilter,
    totalCount: Int,
    hiddenDuplicates: Int,
    today: LocalDate,
    onFilter: (ActivityFilter) -> Unit,
    onOpenRow: (rowId: Long, toQuestions: Boolean) -> Unit,
    onDelete: (Long) -> Unit,
    onClearIgnored: () -> Unit,
    captures: List<CapturePresentation> = emptyList(),
    onOpenCapture: (CapturePresentation) -> Unit = {}
) {
    val colors = CompanionTheme.colors
    val rows = remember(lists, filter) {
        filter.select(lists.visible, lists.ignored, lists.local, lists.questions).map(RowPresentation::of)
    }
    val filteredCaptures = remember(captures, filter) { filter.selectCaptures(captures) }
    val entries = remember(rows, filteredCaptures) {
        (rows.map(FeedEntry::Notification) + filteredCaptures.map(FeedEntry::Capture)).sortedByDescending { it.moment }
    }
    val openQuestionIds = remember(lists) { lists.questions.map { it.id }.toSet() }
    // Day totals count every recorded row and capture of the day, whatever the filter shows.
    val dayOut = remember(lists, captures) {
        val notified = lists.visible.map(RowPresentation::of).filter { it.recorded != null && it.direction == MoneyDirection.OUT }
            .map { it.localDate to it.recorded!!.amount!! }
        val typed = captures.mapNotNull { it.recordedFact }.filter { it.direction == MoneyDirection.OUT }.map { it.date to it.amount }
        (notified + typed).groupBy({ it.first }, { it.second }).mapValues { (_, amounts) -> amounts.sum() }
    }
    var confirmClear by rememberSaveable { mutableStateOf(false) }

    Column(Modifier.fillMaxSize()) {
        PageHeader("Aktivitas", "$totalCount notifikasi · $hiddenDuplicates kembar disembunyikan")
        FilterBar(lists, captures, filter, onFilter)
        LazyColumn(
            modifier = Modifier.fillMaxSize(),
            contentPadding = PaddingValues(start = 12.dp, end = 12.dp, top = 4.dp, bottom = 32.dp),
            verticalArrangement = Arrangement.spacedBy(6.dp)
        ) {
            if (filter == ActivityFilter.IGNORED && rows.isNotEmpty()) {
                item(key = "clear-ignored") {
                    CompanionCard(spacing = 10.dp) {
                        Text(
                            "Ditolak server karena bukan transaksi. ${rows.size} diabaikan · terhapus otomatis setelah 3 hari.",
                            style = CompanionType.small.copy(fontSize = 14.sp), color = colors.text2
                        )
                        CompanionButton("Hapus semua", { confirmClear = true }, kind = ButtonKind.GHOST, icon = Icons.Outlined.Delete)
                    }
                }
            }
            if (filter == ActivityFilter.LOCAL && filteredCaptures.isNotEmpty()) {
                item(key = "capture-queue-note") {
                    Text(
                        "${filteredCaptures.size} catatan ketik menunggu dikirim ke tracker",
                        style = CompanionType.meta, color = colors.text2, modifier = Modifier.padding(horizontal = 4.dp, vertical = 6.dp)
                    )
                }
            }
            if (filter == ActivityFilter.LOCAL && rows.isNotEmpty()) {
                item(key = "local-note") {
                    Text(
                        "${rows.size} disimpan di ponsel · tidak dikirim otomatis · terhapus setelah 7 hari",
                        style = CompanionType.meta, color = colors.text2, modifier = Modifier.padding(horizontal = 4.dp, vertical = 6.dp)
                    )
                }
            }
            if (entries.isEmpty()) {
                item(key = "empty") {
                    CompanionCard {
                        CardTitle("Kosong untuk saringan ini", Icons.Rounded.Check)
                        if (filter == ActivityFilter.ALL) {
                            Text(
                                "Notifikasi dari aplikasi bank dan dompet digital yang dipantau akan muncul di sini saat transaksi terjadi.",
                                style = CompanionType.small, color = colors.text2
                            )
                        }
                    }
                }
            }
            entries.groupBy { it.localDate }.forEach { (date, dayEntries) ->
                item(key = "day-$date") { DayHeader(date, today, dayOut[date] ?: 0L) }
                items(dayEntries, key = { it.key }) { entry ->
                    when (entry) {
                        is FeedEntry.Notification -> {
                            val item = entry.item
                            SwipeToDeleteItem(onDelete = { onDelete(item.row.id) }) {
                                FeedItem(
                                    item,
                                    onClick = { onOpenRow(item.row.id, item.row.id in openQuestionIds) },
                                    onDelete = { onDelete(item.row.id) }
                                )
                            }
                        }
                        is FeedEntry.Capture -> CaptureFeedItem(entry.item, onClick = { onOpenCapture(entry.item) })
                    }
                }
            }
            if (filter == ActivityFilter.ALL && hiddenDuplicates > 0) {
                item(key = "duplicates") {
                    Row(Modifier.padding(horizontal = 4.dp, vertical = 8.dp), horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                        Icon(Icons.Outlined.Layers, null, tint = colors.text2, modifier = Modifier.size(18.dp))
                        Text(
                            "$hiddenDuplicates notifikasi kembar disembunyikan (bank sama, dua aplikasi, dalam 2 menit).",
                            style = CompanionType.meta.copy(fontSize = 13.sp), color = colors.text2
                        )
                    }
                }
            }
        }
    }

    if (confirmClear) {
        AlertDialog(
            onDismissRequest = { confirmClear = false },
            title = { Text("Hapus semua yang diabaikan?") },
            text = { Text("Daftar ini dikosongkan di ponsel. Tidak ada yang berubah di tracker.") },
            confirmButton = {
                TextButton(onClick = {
                    confirmClear = false
                    onClearIgnored()
                }) { Text("Hapus Semua", color = colors.danger) }
            },
            dismissButton = { TextButton(onClick = { confirmClear = false }) { Text("Batal", color = colors.text) } },
            containerColor = colors.surface, titleContentColor = colors.text, textContentColor = colors.text2
        )
    }
}

@Composable
private fun FilterBar(lists: CompanionLists, captures: List<CapturePresentation>, current: ActivityFilter, onFilter: (ActivityFilter) -> Unit) {
    val colors = CompanionTheme.colors
    Row(
        Modifier.fillMaxWidth().horizontalScroll(rememberScrollState()).padding(start = 12.dp, end = 12.dp, top = 2.dp, bottom = 6.dp)
            .semantics { contentDescription = "Saring aktivitas" },
        horizontalArrangement = Arrangement.spacedBy(6.dp)
    ) {
        ActivityFilter.entries.forEach { filter ->
            val count = when (filter) {
                ActivityFilter.NEEDS_ANSWER -> lists.questions.size
                ActivityFilter.PENDING -> lists.visible.count(ActivityFilter::needsCheck) + filter.selectCaptures(captures).size
                ActivityFilter.LOCAL -> lists.local.size + filter.selectCaptures(captures).size
                ActivityFilter.IGNORED -> lists.ignored.size
                else -> null
            }
            val selected = filter == current
            val shape = RoundedCornerShape(14.dp)
            Row(
                Modifier.heightIn(min = 48.dp).clip(shape)
                    .background(if (selected) colors.text else colors.surface)
                    .border(1.dp, if (selected) colors.text else colors.border, shape)
                    .selectable(selected = selected, role = Role.Tab, onClick = { onFilter(filter) })
                    .clearAndSetSemantics {
                        contentDescription = if (count == null) filter.label else "${filter.label}, $count"
                        this.selected = selected
                        role = Role.Tab
                    }
                    .padding(horizontal = 14.dp),
                verticalAlignment = Alignment.CenterVertically,
                horizontalArrangement = Arrangement.spacedBy(6.dp)
            ) {
                val fg = if (selected) colors.bg else colors.text
                Text(filter.label, style = CompanionType.small.copy(fontSize = 14.sp, fontWeight = FontWeight(650)), color = fg)
                if (count != null) {
                    Text("$count", style = CompanionType.small.copy(fontSize = 14.sp, fontWeight = FontWeight(650), fontFeatureSettings = TABULAR_FIGURES), color = fg)
                }
            }
        }
    }
}

@OptIn(ExperimentalLayoutApi::class)
@Composable
private fun DayHeader(date: LocalDate, today: LocalDate, out: Long) {
    val colors = CompanionTheme.colors
    // No-break spaces keep "Kamis, 29 Okt" together; at large text the line breaks after "Hari ini ·" instead.
    val dayMonth = Formatting.dayMonth(date).replace(' ', '\u00A0')
    val label = if (date.isBefore(today.minusDays(1))) dayMonth else "${Formatting.relativeDay(date, today)} · $dayMonth"
    // At large text the money out moves below the date instead of squeezing it into a narrow column.
    FlowRow(
        Modifier.fillMaxWidth().padding(start = 4.dp, end = 4.dp, top = 12.dp, bottom = 2.dp).semantics(mergeDescendants = true) { heading() },
        horizontalArrangement = Arrangement.SpaceBetween,
        verticalArrangement = Arrangement.spacedBy(2.dp)
    ) {
        SectionLabel(label, modifier = Modifier.padding(end = 12.dp))
        SectionLabel("Keluar ${Formatting.rupiah(out)}", color = colors.text2, softWrap = false)
    }
}

@OptIn(ExperimentalLayoutApi::class)
@Composable
fun FeedItem(item: RowPresentation, onClick: () -> Unit, onDelete: () -> Unit) {
    val colors = CompanionTheme.colors
    val shape = RoundedCornerShape(18.dp)
    val description = listOfNotNull(
        item.title, moneyDescription(item.amount, item.direction), item.status.label, item.habit?.let { HABIT_TAG },
        item.account, Formatting.time(item.row.postTime)
    ).joinToString(", ")
    Column(
        Modifier.fillMaxWidth().heightIn(min = 64.dp).clip(shape).background(colors.surface).border(1.dp, colors.border, shape)
            .clickable(role = Role.Button, onClickLabel = "Buka rincian", onClick = onClick)
            .clearAndSetSemantics {
                contentDescription = description
                role = Role.Button
                onClick(label = "Buka rincian") { onClick(); true }
                customActions = listOf(CustomAccessibilityAction(DELETE_ACTION_LABEL) { onDelete(); true })
            }
            .padding(horizontal = 14.dp, vertical = 12.dp),
        verticalArrangement = Arrangement.spacedBy(4.dp)
    ) {
        Row(horizontalArrangement = Arrangement.spacedBy(10.dp)) {
            Text(item.title, style = CompanionType.bodyStrong, color = colors.text, modifier = Modifier.weight(1f))
            MoneyText(item.amount, item.direction, CompanionType.money)
        }
        FlowRow(horizontalArrangement = Arrangement.spacedBy(8.dp), verticalArrangement = Arrangement.spacedBy(4.dp)) {
            StatusBadge(item.status, Modifier.align(Alignment.CenterVertically))
            if (item.habit != null) HabitTag(Modifier.align(Alignment.CenterVertically))
            Text(
                "${item.account} · ${Formatting.time(item.row.postTime)}",
                style = CompanionType.meta.copy(fontFeatureSettings = TABULAR_FIGURES), color = colors.text2,
                modifier = Modifier.align(Alignment.CenterVertically)
            )
        }
    }
}

/** A quiet tag beside the status: the tracker recorded this row from the owner's habits instead of asking. */
@Composable
private fun HabitTag(modifier: Modifier = Modifier) {
    val (bg, fg) = toneColors(StatusTone.MUTED)
    Row(
        modifier = modifier.clip(RoundedCornerShape(999.dp)).background(bg).padding(start = 7.dp, end = 9.dp, top = 3.dp, bottom = 3.dp),
        verticalAlignment = Alignment.CenterVertically,
        horizontalArrangement = Arrangement.spacedBy(5.dp)
    ) {
        Icon(Icons.Outlined.History, contentDescription = null, tint = fg, modifier = Modifier.size(14.dp))
        Text(HABIT_TAG, color = fg, style = CompanionType.meta.copy(fontWeight = FontWeight(600), fontSize = 12.5.sp))
    }
}

/** A typed capture in the feed: like a notification row, labelled as typed on this phone. */
@OptIn(ExperimentalLayoutApi::class)
@Composable
fun CaptureFeedItem(item: CapturePresentation, onClick: () -> Unit) {
    val colors = CompanionTheme.colors
    val shape = RoundedCornerShape(18.dp)
    val meta = listOfNotNull(CAPTURE_SOURCE_LABEL, item.account, Formatting.time(item.moment)).joinToString(" · ")
    val description = listOf(item.title, moneyDescription(item.amount, item.direction), item.status.label, meta).joinToString(", ")
    val action = if (item.question != null) "Jawab pertanyaan" else "Buka rincian"
    Column(
        Modifier.fillMaxWidth().heightIn(min = 64.dp).clip(shape).background(colors.surface).border(1.dp, colors.border, shape)
            .clickable(role = Role.Button, onClickLabel = action, onClick = onClick)
            .clearAndSetSemantics {
                contentDescription = description
                role = Role.Button
                onClick(label = action) { onClick(); true }
            }
            .padding(horizontal = 14.dp, vertical = 12.dp),
        verticalArrangement = Arrangement.spacedBy(4.dp)
    ) {
        Row(horizontalArrangement = Arrangement.spacedBy(10.dp)) {
            Text(item.title, style = CompanionType.bodyStrong, color = colors.text, modifier = Modifier.weight(1f))
            MoneyText(item.amount, item.direction, CompanionType.money)
        }
        FlowRow(horizontalArrangement = Arrangement.spacedBy(8.dp), verticalArrangement = Arrangement.spacedBy(4.dp)) {
            StatusBadge(item.status, Modifier.align(Alignment.CenterVertically))
            Text(
                meta, style = CompanionType.meta.copy(fontFeatureSettings = TABULAR_FIGURES), color = colors.text2,
                modifier = Modifier.align(Alignment.CenterVertically)
            )
        }
    }
}
