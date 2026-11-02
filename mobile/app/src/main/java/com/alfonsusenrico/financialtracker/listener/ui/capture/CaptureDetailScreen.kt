package com.alfonsusenrico.financialtracker.listener.ui.capture

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.automirrored.outlined.HelpOutline
import androidx.compose.material.icons.automirrored.outlined.Send
import androidx.compose.material.icons.outlined.Delete
import androidx.compose.material.icons.outlined.Edit
import androidx.compose.material.icons.outlined.PhoneAndroid
import androidx.compose.material.icons.outlined.Schedule
import androidx.compose.material.icons.outlined.WarningAmber
import androidx.compose.material.icons.rounded.Refresh
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.HorizontalDivider
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
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.style.TextAlign
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import com.alfonsusenrico.financialtracker.listener.data.model.TextCaptureState
import com.alfonsusenrico.financialtracker.listener.data.network.model.AccountItemResponse
import com.alfonsusenrico.financialtracker.listener.data.network.model.CategoryItemResponse
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.data.textcapture.CaptureAbilities
import com.alfonsusenrico.financialtracker.listener.ui.components.ButtonKind
import com.alfonsusenrico.financialtracker.listener.ui.components.CardTitle
import com.alfonsusenrico.financialtracker.listener.ui.components.CompanionButton
import com.alfonsusenrico.financialtracker.listener.ui.components.CompanionCard
import com.alfonsusenrico.financialtracker.listener.ui.components.Formatting
import com.alfonsusenrico.financialtracker.listener.ui.components.InnerTopBar
import com.alfonsusenrico.financialtracker.listener.ui.components.MoneyText
import com.alfonsusenrico.financialtracker.listener.ui.components.StateBox
import com.alfonsusenrico.financialtracker.listener.ui.components.StatusBadge
import com.alfonsusenrico.financialtracker.listener.ui.detail.FormMode
import com.alfonsusenrico.financialtracker.listener.ui.detail.TransactionDraft
import com.alfonsusenrico.financialtracker.listener.ui.detail.TransactionForm
import com.alfonsusenrico.financialtracker.listener.ui.model.CAPTURE_SOURCE_LABEL
import com.alfonsusenrico.financialtracker.listener.ui.model.CapturePresentation
import com.alfonsusenrico.financialtracker.listener.ui.model.MoneyDirection
import com.alfonsusenrico.financialtracker.listener.ui.model.StatusTone
import com.alfonsusenrico.financialtracker.listener.ui.model.kakeiboLabel
import com.alfonsusenrico.financialtracker.listener.ui.shell.ShellActions
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionTheme
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionType
import com.alfonsusenrico.financialtracker.listener.ui.theme.TABULAR_FIGURES

/** Callbacks of a capture's Rincian; each names the capture it acts on. */
class CaptureDetailActions(
    val onBack: () -> Unit,
    val onAnswer: (Long) -> Unit,
    val onSend: (Long) -> Unit,
    val onDiscard: (Long) -> Unit,
    val onDeleteFromTracker: (Long) -> Unit,
    val onEdit: (Long, TransactionDraft, (Boolean) -> Unit) -> Unit,
    val onInvalid: (String) -> Unit
)

@Composable
fun CaptureDetailRoute(
    viewModel: CaptureViewModel,
    accounts: List<AccountItemResponse>,
    categories: List<CategoryItemResponse>,
    actions: ShellActions,
    captureId: Long,
    onBack: () -> Unit
) {
    val context = LocalContext.current
    val capture by remember(captureId) { viewModel.observe(captureId) }.collectAsState(initial = null)
    val busy by viewModel.busy.collectAsState()
    val notice by viewModel.notice.collectAsState()
    val pairing = remember { AppPreferences(context).pairingContext }
    val item = remember(capture) { capture?.let(CapturePresentation::of) }
    CaptureDetailScreen(
        item = item,
        currentPairing = pairing,
        accounts = accounts,
        categories = categories,
        busy = busy,
        notice = notice?.takeIf { it.captureId == captureId }?.text,
        actions = CaptureDetailActions(
            onBack = onBack,
            onAnswer = viewModel::open,
            onSend = viewModel::send,
            onDiscard = { id ->
                viewModel.discard(id) { removed ->
                    actions.message(if (removed) "Dihapus dari aplikasi. Tidak ada yang dikirim ke tracker." else "Catatan ini sudah dikirim, jadi tidak dihapus.")
                    if (removed) onBack()
                }
            },
            onDeleteFromTracker = { id -> viewModel.deleteFromTracker(id, actions.message) },
            onEdit = viewModel::edit,
            onInvalid = actions.message
        )
    )
}

/** Rincian for a typed capture, in the structure of a notification's detail page. */
@Composable
fun CaptureDetailScreen(
    item: CapturePresentation?,
    currentPairing: String,
    accounts: List<AccountItemResponse>,
    categories: List<CategoryItemResponse>,
    busy: Boolean,
    notice: String?,
    actions: CaptureDetailActions
) {
    val colors = CompanionTheme.colors
    var editing by rememberSaveable(item?.id) { mutableStateOf(false) }
    var confirm by rememberSaveable(item?.id) { mutableStateOf<String?>(null) }
    if (item == null) {
        Column(Modifier.fillMaxSize()) {
            InnerTopBar("Rincian", actions.onBack)
            Column(Modifier.padding(horizontal = 12.dp)) {
                CompanionCard {
                    CardTitle("Catatan tidak ditemukan")
                    Text("Mungkin sudah dihapus dari aplikasi.", style = CompanionType.small, color = colors.text2)
                }
            }
        }
        return
    }
    val capture = item.capture
    val canEdit = CaptureAbilities.canEdit(capture, item.view, currentPairing)
    val canDelete = CaptureAbilities.canDeleteFromTracker(capture, item.view, currentPairing)
    Column(Modifier.fillMaxSize()) {
        InnerTopBar(if (editing) "Ubah transaksi" else "Rincian", onBack = { if (editing) editing = false else actions.onBack() })
        Column(
            Modifier.fillMaxSize().verticalScroll(rememberScrollState()).padding(start = 12.dp, end = 12.dp, top = 4.dp, bottom = 32.dp),
            verticalArrangement = Arrangement.spacedBy(10.dp)
        ) {
            Hero(item)
            if (editing && canEdit) {
                CompanionCard {
                    CardTitle("Ubah data di tracker")
                    TransactionForm(
                        captureFormSeed(item), FormMode.EDIT_RECORDED, canSubmit = true, accounts = accounts, categories = categories,
                        busy = busy, onCancel = { editing = false }, onInvalid = actions.onInvalid,
                        onSubmit = { draft -> actions.onEdit(item.id, draft) { ok -> if (ok) editing = false } }
                    )
                }
            } else {
                StatePanel(item, busy, actions) { confirm = it }
                if (capture.state == TextCaptureState.RECORDED) RecordedCard(item, canEdit, busy) { editing = true }
            }
            NoticeLine(notice)
            CompanionCard {
                CardTitle("Teks yang diketik")
                Text("“${capture.text}”", style = CompanionType.body.copy(fontSize = 15.5.sp), color = colors.text)
                Text(
                    "Diketik ${Formatting.dateTime(capture.createdAt)} · $CAPTURE_SOURCE_LABEL",
                    style = CompanionType.meta.copy(fontFeatureSettings = TABULAR_FIGURES), color = colors.text2
                )
            }
            if (!editing && canDelete) {
                CompanionButton(
                    "Hapus dari tracker", { confirm = CONFIRM_TRACKER }, Modifier.fillMaxWidth(), ButtonKind.DANGER,
                    icon = Icons.Outlined.Delete, enabled = !busy
                )
            }
        }
    }

    when (confirm) {
        CONFIRM_TRACKER -> ConfirmDialog(
            title = "Hapus dari tracker?",
            body = "Transaksi ${Formatting.rupiah(item.amount ?: 0)} “${item.title}” dihapus dari tracker dan saldonya dihitung ulang. " +
                "Ini tidak bisa diurungkan dari aplikasi.",
            confirmLabel = "Hapus dari tracker",
            onConfirm = { actions.onDeleteFromTracker(item.id) },
            onDismiss = { confirm = null }
        )
        CONFIRM_PHONE -> ConfirmDialog(
            title = "Hapus dari aplikasi?",
            body = "Catatan ini belum ada di tracker. Ia hilang dari ponsel dan tidak dikirim.",
            confirmLabel = "Hapus",
            onConfirm = { actions.onDiscard(item.id) },
            onDismiss = { confirm = null }
        )
    }
}

private const val CONFIRM_TRACKER = "tracker"
private const val CONFIRM_PHONE = "phone"

@Composable
private fun ConfirmDialog(title: String, body: String, confirmLabel: String, onConfirm: () -> Unit, onDismiss: () -> Unit) {
    val colors = CompanionTheme.colors
    AlertDialog(
        onDismissRequest = onDismiss,
        title = { Text(title) },
        text = { Text(body) },
        confirmButton = {
            TextButton(onClick = {
                onDismiss()
                onConfirm()
            }) { Text(confirmLabel, color = colors.danger) }
        },
        dismissButton = { TextButton(onClick = onDismiss) { Text("Batal", color = colors.text) } },
        containerColor = colors.surface, titleContentColor = colors.text, textContentColor = colors.text2
    )
}

@Composable
private fun Hero(item: CapturePresentation) {
    val colors = CompanionTheme.colors
    Column(
        Modifier.fillMaxWidth().padding(top = 6.dp, bottom = 2.dp).semantics(mergeDescendants = true) {},
        horizontalAlignment = Alignment.CenterHorizontally,
        verticalArrangement = Arrangement.spacedBy(6.dp)
    ) {
        StatusBadge(item.status)
        MoneyText(item.amount, item.direction, CompanionType.heroAmount, textAlign = TextAlign.Center)
        Text(item.title, style = CompanionType.bodyStrong.copy(fontSize = 16.sp), color = colors.text, textAlign = TextAlign.Center)
        Text(
            "${Formatting.dateTime(item.moment)} · $CAPTURE_SOURCE_LABEL",
            style = CompanionType.meta.copy(fontSize = 13.sp, fontFeatureSettings = TABULAR_FIGURES), color = colors.text2,
            textAlign = TextAlign.Center
        )
    }
}

/** The state box and its actions for a capture that is not simply recorded. */
@Composable
private fun StatePanel(item: CapturePresentation, busy: Boolean, actions: CaptureDetailActions, confirm: (String) -> Unit) {
    val capture = item.capture
    when (capture.state) {
        TextCaptureState.NEEDS_DETAIL -> {
            StateBox(StatusTone.ASK, Icons.AutoMirrored.Outlined.HelpOutline, "Perlu jawaban", item.question?.prompt)
            CompanionButton("Jawab sekarang", { actions.onAnswer(item.id) }, Modifier.fillMaxWidth())
        }
        TextCaptureState.QUEUED -> {
            StateBox(
                StatusTone.PHONE, Icons.Outlined.PhoneAndroid, "Disimpan di antrean",
                CaptureMessages.waiting(capture.errorCode) ?: "Belum dikirim ke tracker."
            )
            CompanionButton("Kirim", { actions.onSend(item.id) }, Modifier.fillMaxWidth(), icon = Icons.AutoMirrored.Outlined.Send, enabled = !busy)
            if (CaptureAbilities.canDiscard(capture)) {
                CompanionButton("Hapus dari aplikasi", { confirm(CONFIRM_PHONE) }, Modifier.fillMaxWidth(), ButtonKind.DANGER, Icons.Outlined.Delete, enabled = !busy)
            }
        }
        TextCaptureState.SENDING -> StateBox(StatusTone.MUTED, Icons.Outlined.Schedule, "Dikirim", "Menunggu jawaban tracker.")
        TextCaptureState.FAILED -> {
            StateBox(StatusTone.WARN, Icons.Outlined.WarningAmber, "Tracker menolak catatan ini", CaptureMessages.refused(capture.errorCode))
            CompanionButton("Coba lagi", { actions.onSend(item.id) }, Modifier.fillMaxWidth(), icon = Icons.Rounded.Refresh, enabled = !busy)
            CompanionButton("Hapus dari aplikasi", { confirm(CONFIRM_PHONE) }, Modifier.fillMaxWidth(), ButtonKind.DANGER, Icons.Outlined.Delete, enabled = !busy)
        }
        TextCaptureState.UNDONE -> StateBox(StatusTone.MUTED, Icons.Rounded.Refresh, "Dibatalkan", "Tidak ada yang tercatat di tracker.")
        TextCaptureState.DELETED -> StateBox(
            StatusTone.MUTED, Icons.Outlined.Delete, "Dihapus dari tracker",
            if (capture.errorCode == "not_found") "Tracker tidak lagi menyimpan transaksi ini." else "Transaksinya sudah dihapus dari tracker."
        )
    }
}

@Composable
private fun RecordedCard(item: CapturePresentation, canEdit: Boolean, busy: Boolean, onEdit: () -> Unit) {
    val colors = CompanionTheme.colors
    val transaction = item.view?.transaction
    CompanionCard {
        CardTitle("Yang dicatat tracker")
        val facts = listOfNotNull(
            "Keterangan" to item.title,
            "Nominal" to (item.amount?.let(Formatting::rupiah) ?: "Rp —"),
            "Jenis" to when (item.direction) {
                MoneyDirection.IN -> "Masuk"
                MoneyDirection.OUT -> "Keluar"
                else -> "Pindah saldo (tidak dihitung pengeluaran)"
            },
            "Rekening" to (item.account ?: "Belum diketahui"),
            item.category?.let { "Kategori" to it },
            kakeiboLabel(item.kakeibo)?.let { "Pilar Kakeibo" to it },
            transaction?.notes?.takeIf { it.isNotBlank() && it != item.title }?.let { "Catatan" to it }
        )
        Column {
            facts.forEachIndexed { index, (label, value) ->
                if (index > 0) HorizontalDivider(color = colors.border)
                Row(
                    Modifier.fillMaxWidth().padding(vertical = 11.dp).semantics(mergeDescendants = true) {},
                    horizontalArrangement = Arrangement.spacedBy(16.dp)
                ) {
                    Text(label, style = CompanionType.small.copy(fontSize = 14.5.sp), color = colors.text2)
                    Text(
                        value, style = CompanionType.small.copy(fontSize = 14.5.sp, fontWeight = FontWeight(600), fontFeatureSettings = TABULAR_FIGURES),
                        color = colors.text, textAlign = TextAlign.End, modifier = Modifier.weight(1f)
                    )
                }
            }
        }
        if (canEdit) {
            CompanionButton("Ubah transaksi", onEdit, Modifier.fillMaxWidth(), ButtonKind.GHOST, Icons.Outlined.Edit, enabled = !busy)
        } else if (item.isMovement) {
            Text("Pindah saldo diubah dan dihapus di Tracker Web.", style = CompanionType.meta, color = colors.text2)
        } else {
            Text("Transaksi ini hanya bisa diubah di Tracker Web dari akun yang mencatatnya.", style = CompanionType.meta, color = colors.text2)
        }
    }
}
