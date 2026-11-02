package com.alfonsusenrico.financialtracker.listener.ui.hub

import androidx.activity.compose.BackHandler
import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.interaction.MutableInteractionSource
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.ColumnScope
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.statusBarsPadding
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.automirrored.rounded.ArrowBack
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.mutableStateListOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.saveable.Saver
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.snapshots.SnapshotStateList
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.semantics.clearAndSetSemantics
import androidx.compose.ui.semantics.heading
import androidx.compose.ui.semantics.isTraversalGroup
import androidx.compose.ui.semantics.paneTitle
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.unit.dp
import com.alfonsusenrico.financialtracker.listener.ui.components.SquareIconButton
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionTheme
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionType

/** The screens that open over the hub. A short sheet sizes to its content; the others take nearly the full height. */
enum class SheetKind(val title: String, val tall: Boolean = true, val ownHeader: Boolean = false) {
    QUESTIONS("Pertanyaan"),
    LOG("Riwayat tangkapan"),
    DETAIL("Rincian", ownHeader = true),
    CAPTURE_DETAIL("Rincian", ownHeader = true),
    SETTINGS("Pengaturan"),
    STATUS("Rincian status", tall = false),
    WEB("Buka di web", tall = false)
}

/** One open sheet; [id] is the row or capture a Rincian or a question shows, -1 when none. */
data class SheetEntry(val kind: SheetKind, val id: Long = -1)

/**
 * The sheets over the hub. Only Riwayat nests (Rincian opens on top of it with a way back); opening any
 * other sheet replaces what was open.
 */
class SheetStack(initial: List<SheetEntry> = emptyList()) {
    val entries: SnapshotStateList<SheetEntry> = mutableStateListOf<SheetEntry>().apply { addAll(initial) }
    val top: SheetEntry? get() = entries.lastOrNull()
    val isOpen: Boolean get() = entries.isNotEmpty()

    fun open(kind: SheetKind, id: Long = -1) {
        entries.clear()
        entries.add(SheetEntry(kind, id))
    }

    /** A Rincian from Riwayat keeps Riwayat beneath it; from anywhere else it opens alone. */
    fun openDetail(kind: SheetKind, id: Long) {
        if (top?.kind != SheetKind.LOG) entries.clear()
        entries.add(SheetEntry(kind, id))
    }

    fun back() {
        if (entries.isNotEmpty()) entries.removeAt(entries.lastIndex)
    }

    fun close() = entries.clear()

    companion object {
        val Saver: Saver<SheetStack, Any> = Saver(
            save = { stack -> stack.entries.flatMap { listOf(it.kind.name, it.id) } },
            restore = { saved ->
                @Suppress("UNCHECKED_CAST")
                val flat = saved as List<Any>
                SheetStack(flat.chunked(2).map { (kind, id) -> SheetEntry(SheetKind.valueOf(kind as String), id as Long) })
            }
        )
    }
}

@Composable
fun rememberSheetStack(): SheetStack = rememberSaveable(saver = SheetStack.Saver) { SheetStack() }

/**
 * A modal sheet over the hub: a scrim that closes it, a grab line, the title as a heading, "Tutup", and a back
 * arrow when it sits on another sheet. Android's back steps back the same way.
 */
@Composable
fun SheetFrame(
    entry: SheetEntry,
    previous: SheetEntry?,
    onBack: () -> Unit,
    onClose: () -> Unit,
    content: @Composable ColumnScope.() -> Unit
) {
    val colors = CompanionTheme.colors
    BackHandler { if (previous != null) onBack() else onClose() }
    Box(Modifier.fillMaxSize()) {
        Box(Modifier.fillMaxSize().background(colors.text.copy(alpha = 0.42f)).clearAndSetSemantics {})
        Column(Modifier.fillMaxSize()) {
            // Only the strip of hub left visible above the sheet closes it, so no tap on the sheet can reach it.
            val closeTap = Modifier.fillMaxWidth()
                .clickable(interactionSource = remember { MutableInteractionSource() }, indication = null, onClick = onClose)
                .clearAndSetSemantics {}
            Box(closeTap.statusBarsPadding().height(if (entry.kind.tall) 12.dp else 48.dp))
            if (!entry.kind.tall) Box(closeTap.weight(1f))
            // A shaped background rather than a clip, as the Catat sheet: the content keeps clear of the corners.
            val shape = RoundedCornerShape(topStart = 28.dp, topEnd = 28.dp)
            Column(
                Modifier.fillMaxWidth().then(if (entry.kind.tall) Modifier.weight(1f) else Modifier)
                    .background(colors.bg, shape)
                    .semantics { paneTitle = entry.kind.title; isTraversalGroup = true }
            ) {
                Box(Modifier.fillMaxWidth().padding(top = 8.dp), contentAlignment = Alignment.Center) {
                    Box(Modifier.size(width = 40.dp, height = 4.dp).clip(RoundedCornerShape(2.dp)).background(colors.borderStrong).clearAndSetSemantics {})
                }
                if (!entry.kind.ownHeader) {
                    Row(
                        Modifier.fillMaxWidth().heightIn(min = 56.dp).padding(horizontal = 12.dp),
                        verticalAlignment = Alignment.CenterVertically,
                        horizontalArrangement = Arrangement.spacedBy(8.dp)
                    ) {
                        if (previous != null) {
                            SquareIconButton(Icons.AutoMirrored.Rounded.ArrowBack, "Balik ke ${previous.kind.title}", onBack)
                        }
                        Text(
                            entry.kind.title, style = CompanionType.pageTitle, color = colors.text,
                            modifier = Modifier.weight(1f).semantics { heading() }
                        )
                        TextButton(onClick = onClose, modifier = Modifier.heightIn(min = 48.dp)) {
                            Text("Tutup", style = CompanionType.button, color = colors.text)
                        }
                    }
                }
                // The sheet's own page takes the room left under its header, so its scrolling stays inside the sheet.
                Column(Modifier.weight(1f, fill = entry.kind.tall).fillMaxWidth(), content = content)
            }
        }
    }
}
