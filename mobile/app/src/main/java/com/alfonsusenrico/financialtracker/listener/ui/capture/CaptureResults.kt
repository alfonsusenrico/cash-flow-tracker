package com.alfonsusenrico.financialtracker.listener.ui.capture

import androidx.compose.animation.core.LinearEasing
import androidx.compose.animation.core.RepeatMode
import androidx.compose.animation.core.animateFloat
import androidx.compose.animation.core.infiniteRepeatable
import androidx.compose.animation.core.rememberInfiniteTransition
import androidx.compose.animation.core.tween
import androidx.compose.foundation.background
import androidx.compose.foundation.border
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.automirrored.rounded.Undo
import androidx.compose.material.icons.outlined.AutoAwesome
import androidx.compose.material.icons.outlined.DeleteOutline
import androidx.compose.material.icons.outlined.Edit
import androidx.compose.material.icons.outlined.WarningAmber
import androidx.compose.material.icons.outlined.WifiOff
import androidx.compose.material.icons.rounded.Check
import androidx.compose.material.icons.rounded.Refresh
import androidx.compose.material3.Icon
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.graphics.Brush
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.semantics.LiveRegionMode
import androidx.compose.ui.semantics.Role
import androidx.compose.ui.semantics.clearAndSetSemantics
import androidx.compose.ui.semantics.contentDescription
import androidx.compose.ui.semantics.liveRegion
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.text.style.TextAlign
import androidx.compose.ui.unit.dp
import com.alfonsusenrico.financialtracker.listener.data.textcapture.TextCaptures
import com.alfonsusenrico.financialtracker.listener.ui.components.ButtonKind
import com.alfonsusenrico.financialtracker.listener.ui.components.CompanionButton
import com.alfonsusenrico.financialtracker.listener.ui.components.MoneyText
import com.alfonsusenrico.financialtracker.listener.ui.components.StateBox
import com.alfonsusenrico.financialtracker.listener.ui.components.StatusBadge
import com.alfonsusenrico.financialtracker.listener.ui.detail.FormMode
import com.alfonsusenrico.financialtracker.listener.ui.detail.FormSeed
import com.alfonsusenrico.financialtracker.listener.ui.detail.TransactionForm
import com.alfonsusenrico.financialtracker.listener.ui.model.CapturePresentation
import com.alfonsusenrico.financialtracker.listener.ui.model.MoneyDirection
import com.alfonsusenrico.financialtracker.listener.ui.model.RowStatus
import com.alfonsusenrico.financialtracker.listener.ui.model.StatusIcon
import com.alfonsusenrico.financialtracker.listener.ui.model.StatusTone
import com.alfonsusenrico.financialtracker.listener.ui.model.kakeiboLabel
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionTheme
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionType
import com.alfonsusenrico.financialtracker.listener.ui.theme.TABULAR_FIGURES

const val UNDO_BAR_TAG = "undo-bar"

/** The three steps the tracker runs for one sentence; listed, not timed, because the phone cannot see them. */
private val PROCESSING_STEPS = listOf("Membaca kalimat", "Mencari nominal", "Mencocokkan rekening & kategori")

@Composable
fun CaptureProcessing(text: String, reducedMotion: Boolean) {
    val colors = CompanionTheme.colors
    TypedQuote(text)
    ReceiptCard(Modifier.semantics { contentDescription = "AI memproses: ${PROCESSING_STEPS.joinToString(", ")}" }) {
        Row(
            Modifier.semantics { liveRegion = LiveRegionMode.Polite },
            horizontalArrangement = Arrangement.spacedBy(8.dp), verticalAlignment = Alignment.CenterVertically
        ) {
            Icon(Icons.Outlined.AutoAwesome, contentDescription = null, tint = colors.text, modifier = Modifier.size(18.dp))
            Text("AI memproses…", style = CompanionType.bodyStrong, color = colors.text)
        }
        PROCESSING_STEPS.forEach { step ->
            Row(horizontalArrangement = Arrangement.spacedBy(10.dp), verticalAlignment = Alignment.CenterVertically) {
                Box(Modifier.size(8.dp).clip(CircleShape).background(colors.accentLine))
                Text(step, style = CompanionType.small, color = colors.text2)
            }
        }
        Skeleton(0.6f, reducedMotion)
        Skeleton(0.85f, reducedMotion)
    }
}

/** A placeholder line (`.f-skel`); its shimmer is left out when Android's animations are off. */
@Composable
private fun Skeleton(width: Float, reducedMotion: Boolean) {
    val colors = CompanionTheme.colors
    val shine = if (colors.isDark) Color.White.copy(alpha = 0.08f) else Color.White.copy(alpha = 0.4f)
    val offset = if (reducedMotion) null else {
        val transition = rememberInfiniteTransition(label = "skeleton")
        transition.animateFloat(-1f, 1f, infiniteRepeatable(tween(1200, easing = LinearEasing), RepeatMode.Restart), label = "shine").value
    }
    Box(
        Modifier.fillMaxWidth(width).height(12.dp).clip(RoundedCornerShape(8.dp)).background(colors.surface2)
            .then(
                if (offset == null) Modifier else Modifier.background(
                    Brush.horizontalGradient(
                        0f to Color.Transparent, ((offset + 1f) / 2f).coerceIn(0.01f, 0.99f) to shine, 1f to Color.Transparent
                    )
                )
            )
    )
}

/**
 * Recorded: the receipt with Ubah and the 10-second "Batalkan", or once the window has passed, "Tersimpan"
 * pointing to Rincian.
 */
@Composable
fun CaptureRecorded(state: CaptureSheetState, actions: CaptureSheetActions) {
    val item = state.capture ?: return
    val saved = state.phase == SheetPhase.SAVED
    var editing by rememberSaveable(item.id) { mutableStateOf(false) }
    if (editing && state.canEdit) {
        ReceiptCard {
            Text("Ubah transaksi", style = CompanionType.cardTitle, color = CompanionTheme.colors.text)
            TransactionForm(
                captureFormSeed(item), FormMode.EDIT_RECORDED, canSubmit = state.canEdit,
                accounts = state.accounts, categories = state.categories, busy = state.busy,
                onCancel = { editing = false }, onInvalid = actions.onInvalid,
                onSubmit = { draft -> actions.onEdit(item.id, draft) { ok -> if (ok) editing = false } }
            )
        }
        NoticeLine(state.notice)
        return
    }
    ReceiptCard(Modifier.semantics { liveRegion = LiveRegionMode.Polite }) {
        StatusBadge(
            if (item.isMovement) RowStatus("Pindah saldo tercatat", StatusTone.MOVE, StatusIcon.SWAP)
            else RowStatus("Tercatat di tracker", StatusTone.OK, StatusIcon.CHECK)
        )
        MoneyText(item.amount, item.direction, CompanionType.heroAmount)
        FactTags(recordedTags(item))
        val notes = item.view?.transaction?.notes
        if (!notes.isNullOrBlank() && notes != item.title) WhyLine("Catatan: $notes")
        ActionPair(
            first = { modifier ->
                if (state.canEdit) CompanionButton("Ubah", { editing = true }, modifier, ButtonKind.GHOST, Icons.Outlined.Edit, enabled = !state.busy)
                else WhyLine(if (item.isMovement) "Pindah saldo diubah di Tracker Web." else "Diubah di Tracker Web.")
            },
            second = { modifier ->
                if (saved) {
                    Row(modifier.heightIn(min = 48.dp), horizontalArrangement = Arrangement.spacedBy(6.dp, Alignment.CenterHorizontally), verticalAlignment = Alignment.CenterVertically) {
                        Icon(Icons.Rounded.Check, contentDescription = null, tint = CompanionTheme.colors.okFg, modifier = Modifier.size(18.dp))
                        Text("Tersimpan", style = CompanionType.bodyStrong, color = CompanionTheme.colors.text)
                    }
                } else {
                    UndoButton(state.secondsLeft, state.undoFraction, state.reducedMotion, !state.busy, modifier) { actions.onUndo(item.id) }
                }
            }
        )
        if (saved) WhyLine("Untuk membatalkan sekarang, buka Rincian lalu Hapus dari tracker.")
    }
    NoticeLine(state.notice)
    if (saved) CompanionButton("Buka rincian", { actions.onOpenDetail(item.id) }, Modifier.fillMaxWidth(), ButtonKind.GHOST)
    ActionPair(
        first = { CompanionButton("Catat lagi", actions.onAnother, it, ButtonKind.PRIMARY) },
        second = { CompanionButton("Selesai", actions.onClose, it, ButtonKind.GHOST) }
    )
}

/** "Batalkan (n)": the seconds as text plus a draining bar; the bar goes when animations are off. */
@Composable
private fun UndoButton(seconds: Int, fraction: Float, reducedMotion: Boolean, enabled: Boolean, modifier: Modifier, onClick: () -> Unit) {
    val colors = CompanionTheme.colors
    val shape = RoundedCornerShape(14.dp)
    Box(
        modifier.heightIn(min = 48.dp).clip(shape).border(1.dp, colors.borderStrong, shape)
            .clickable(enabled = enabled, role = Role.Button, onClick = onClick)
            .clearAndSetSemantics { contentDescription = "Batalkan, sisa $seconds detik" },
        contentAlignment = Alignment.Center
    ) {
        Row(Modifier.padding(horizontal = 12.dp, vertical = 10.dp), horizontalArrangement = Arrangement.spacedBy(6.dp), verticalAlignment = Alignment.CenterVertically) {
            Icon(Icons.AutoMirrored.Rounded.Undo, contentDescription = null, tint = colors.text, modifier = Modifier.size(18.dp))
            Text("Batalkan ($seconds)", style = CompanionType.button.copy(fontFeatureSettings = TABULAR_FIGURES), color = colors.text, textAlign = TextAlign.Center)
        }
        if (!reducedMotion) {
            Box(Modifier.align(Alignment.BottomStart).fillMaxWidth(fraction).height(3.dp).background(colors.accentLine).testTag(UNDO_BAR_TAG))
        }
    }
}

/** Amount aside, what the tracker recorded: description, category, pillar and account, or the move. */
fun recordedTags(item: CapturePresentation): List<FactTag> = if (item.isMovement) {
    listOfNotNull(FactTag("Pindah saldo"), item.account?.let(::FactTag))
} else {
    listOfNotNull(item.title, item.category, kakeiboLabel(item.kakeibo), item.account).distinct().map(::FactTag)
}

@Composable
fun CaptureQueued(state: CaptureSheetState, actions: CaptureSheetActions) {
    val item = state.capture ?: return
    val reason = CaptureMessages.waiting(item.capture.errorCode)
    val body = buildString {
        append("“${item.capture.text}” ")
        append(
            when {
                item.capture.errorCode == TextCaptures.NOT_PAIRED -> "menunggu di ponsel sampai tracker terhubung."
                state.autoSync -> "dikirim otomatis saat ponsel online."
                else -> "menunggu di ponsel karena Kirim otomatis mati. Tekan Kirim saat online."
            }
        )
        append(" Lihat di Aktivitas → Di ponsel.")
        if (reason != null && item.capture.errorCode != TextCaptures.NOT_PAIRED) append(" ").append(reason)
    }
    StateBox(StatusTone.PHONE, Icons.Outlined.WifiOff, "Disimpan di antrean", body, Modifier.semantics { liveRegion = LiveRegionMode.Polite })
    NoticeLine(state.notice)
    CompanionButton("Kirim", { actions.onSend(item.id) }, Modifier.fillMaxWidth(), ButtonKind.GHOST, Icons.Rounded.Refresh, enabled = !state.busy)
    ActionPair(
        first = { CompanionButton("Catat lagi", actions.onAnother, it, ButtonKind.PRIMARY) },
        second = { CompanionButton("Lihat antrean", actions.onOpenQueue, it, ButtonKind.GHOST) }
    )
}

@Composable
fun CaptureError(state: CaptureSheetState, actions: CaptureSheetActions) {
    val item = state.capture ?: return
    StateBox(
        StatusTone.WARN, Icons.Outlined.WarningAmber, "AI belum bisa memproses",
        CaptureMessages.waiting(item.capture.errorCode), Modifier.semantics { liveRegion = LiveRegionMode.Assertive }
    )
    TypedQuote(item.capture.text)
    NoticeLine(state.notice)
    ActionPair(
        first = { CompanionButton("Coba lagi", { actions.onSend(item.id) }, it, ButtonKind.PRIMARY, Icons.Rounded.Refresh, enabled = !state.busy) },
        second = { CompanionButton("Tutup", actions.onClose, it, ButtonKind.GHOST) }
    )
}

@Composable
fun CaptureFailed(state: CaptureSheetState, actions: CaptureSheetActions) {
    val item = state.capture ?: return
    StateBox(
        StatusTone.WARN, Icons.Outlined.WarningAmber, "Tracker menolak catatan ini",
        CaptureMessages.refused(item.capture.errorCode) + " Tidak ada yang tercatat.",
        Modifier.semantics { liveRegion = LiveRegionMode.Assertive }
    )
    TypedQuote(item.capture.text)
    NoticeLine(state.notice)
    ActionPair(
        first = { CompanionButton("Tulis ulang", { actions.onRewrite(item.id, item.capture.text) }, it, ButtonKind.PRIMARY, enabled = !state.busy) },
        second = { CompanionButton("Coba lagi", { actions.onSend(item.id) }, it, ButtonKind.GHOST, Icons.Rounded.Refresh, enabled = !state.busy) }
    )
}

/** Undone, or deleted from the tracker: nothing of this capture is left in the ledger. */
@Composable
fun CaptureClosed(state: CaptureSheetState, actions: CaptureSheetActions) {
    val item = state.capture ?: return
    val undone = state.phase == SheetPhase.UNDONE
    StateBox(
        StatusTone.MUTED, if (undone) Icons.AutoMirrored.Rounded.Undo else Icons.Outlined.DeleteOutline,
        if (undone) "Dibatalkan" else "Dihapus dari tracker",
        "“${item.capture.text}” tidak tercatat di tracker.", Modifier.semantics { liveRegion = LiveRegionMode.Polite }
    )
    NoticeLine(state.notice)
    ActionPair(
        first = { CompanionButton("Catat lagi", actions.onAnother, it, ButtonKind.PRIMARY) },
        second = { CompanionButton("Selesai", actions.onClose, it, ButtonKind.GHOST) }
    )
}

/** The edit form's starting values for a recorded capture. */
fun captureFormSeed(item: CapturePresentation) = FormSeed(
    key = "capture-${item.id}",
    amount = item.amount,
    type = if (item.direction == MoneyDirection.IN) "income" else "expense",
    notes = item.view?.transaction?.notes ?: item.title
)
