package com.alfonsusenrico.financialtracker.listener.ui.detail

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
import androidx.compose.material.icons.outlined.History
import androidx.compose.material.icons.outlined.PhoneAndroid
import androidx.compose.material.icons.outlined.Schedule
import androidx.compose.material.icons.outlined.VisibilityOff
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
import com.alfonsusenrico.financialtracker.listener.data.model.RawNotificationEntity
import com.alfonsusenrico.financialtracker.listener.data.model.SenderReplyEntity
import com.alfonsusenrico.financialtracker.listener.data.network.model.AccountItemResponse
import com.alfonsusenrico.financialtracker.listener.data.network.model.CategoryItemResponse
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.data.sync.purposeReplyMessage
import com.alfonsusenrico.financialtracker.listener.ui.components.ButtonKind
import com.alfonsusenrico.financialtracker.listener.ui.components.CaptureReasons
import com.alfonsusenrico.financialtracker.listener.ui.components.CardTitle
import com.alfonsusenrico.financialtracker.listener.ui.components.CompanionButton
import com.alfonsusenrico.financialtracker.listener.ui.components.CompanionCard
import com.alfonsusenrico.financialtracker.listener.ui.components.Formatting
import com.alfonsusenrico.financialtracker.listener.ui.components.InnerTopBar
import com.alfonsusenrico.financialtracker.listener.ui.components.MoneyText
import com.alfonsusenrico.financialtracker.listener.ui.components.Note
import com.alfonsusenrico.financialtracker.listener.ui.components.OriginalNotification
import com.alfonsusenrico.financialtracker.listener.ui.components.SquareIconButton
import com.alfonsusenrico.financialtracker.listener.ui.components.StateBox
import com.alfonsusenrico.financialtracker.listener.ui.components.StatusBadge
import com.alfonsusenrico.financialtracker.listener.ui.model.MoneyDirection
import com.alfonsusenrico.financialtracker.listener.ui.model.OpenQuestions
import com.alfonsusenrico.financialtracker.listener.ui.model.RowAbilities
import com.alfonsusenrico.financialtracker.listener.ui.model.RowPresentation
import com.alfonsusenrico.financialtracker.listener.ui.model.StatusTone
import com.alfonsusenrico.financialtracker.listener.ui.model.detailLine
import com.alfonsusenrico.financialtracker.listener.ui.questions.questionContext
import com.alfonsusenrico.financialtracker.listener.ui.shell.CompanionViewModel
import com.alfonsusenrico.financialtracker.listener.ui.shell.ShellActions
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionTheme
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionType
import com.alfonsusenrico.financialtracker.listener.ui.theme.TABULAR_FIGURES
import java.time.LocalDate

/** Callbacks of the detail page; each write reports success so the page can close its form. */
class DetailActions(
    val onBack: () -> Unit,
    val onDelete: (Long) -> Unit,
    val onOpenQuestion: (Long) -> Unit,
    val onRetry: (Long) -> Unit,
    val onSendLocal: (Long) -> Unit,
    val onResend: (Long) -> Unit,
    val onInvalid: (String) -> Unit,
    val onSubmit: (row: RawNotificationEntity, mode: FormMode, editableId: String?, draft: TransactionDraft, done: (Boolean) -> Unit) -> Unit
)

@Composable
fun DetailRoute(
    viewModel: CompanionViewModel,
    actions: ShellActions,
    rowId: Long,
    onBack: () -> Unit,
    onOpenQuestion: (Long) -> Unit
) {
    val context = LocalContext.current
    val row by remember(rowId) { viewModel.row(rowId) }.collectAsState(initial = null)
    val lists by viewModel.lists.collectAsState()
    val accounts by viewModel.accounts.collectAsState()
    val categories by viewModel.categories.collectAsState()
    val busy by viewModel.isUpdating.collectAsState()
    val pairing = remember { AppPreferences(context).pairingContext }
    val say = actions.message
    DetailScreen(
        row = row,
        replies = lists.replies,
        currentPairing = pairing,
        accounts = accounts,
        categories = categories,
        busy = busy,
        today = LocalDate.now(),
        actions = DetailActions(
            onBack = onBack,
            onDelete = { id ->
                onBack()
                actions.deleteWithUndo(id)
            },
            onOpenQuestion = onOpenQuestion,
            onRetry = { id ->
                viewModel.retryNotification(id) { ok, error ->
                    say(if (ok) "Notifikasi dikirim ulang untuk diproses" else "Gagal mengulang: ${error ?: "periksa koneksi"}")
                }
            },
            onSendLocal = { id ->
                viewModel.sendLocal(id) { ok -> say(if (ok) "Dikirim ke tracker; menunggu hasil server" else "Notifikasi ini tidak bisa dikirim") }
            },
            onResend = { id ->
                viewModel.resendIgnored(id) { ok -> say(if (ok) "Dikirim ulang; menunggu hasil server" else "Notifikasi ini tidak bisa dikirim ulang") }
            },
            onInvalid = say,
            onSubmit = { target, mode, editableId, draft, done ->
                when (mode) {
                    FormMode.EDIT_RECORDED -> viewModel.updateSyncedTransaction(
                        target.id, editableId.orEmpty(), draft.amount, draft.type, draft.accountId, draft.categoryId, draft.notes
                    ) { ok, error ->
                        say(if (ok) "Transaksi berhasil diperbarui di server" else "Gagal memperbarui: ${error ?: "periksa koneksi"}")
                        done(ok)
                    }
                    FormMode.MANUAL_RECORD -> viewModel.resolveNotification(
                        target.id, draft.amount, draft.type, draft.accountId!!, draft.categoryId!!, draft.notes
                    ) { ok, error ->
                        say(if (ok) "Transaksi dicatat manual di tracker" else "Gagal mencatat: ${error ?: "periksa koneksi"}")
                        done(ok)
                    }
                    FormMode.SEND_UNSENT -> viewModel.saveAndSyncUnsynced(target.id, draft.amount, draft.type, draft.notes) { ok, error ->
                        say(if (ok) "Notifikasi disimpan dan sedang dikirim ke tracker" else "Gagal menyimpan: ${error ?: "periksa koneksi"}")
                        done(ok)
                    }
                }
            }
        )
    )
}

/** Rincian: option A's detail page in the E visual system. */
@Composable
fun DetailScreen(
    row: RawNotificationEntity?,
    replies: List<SenderReplyEntity>,
    currentPairing: String,
    accounts: List<AccountItemResponse>,
    categories: List<CategoryItemResponse>,
    busy: Boolean,
    today: LocalDate,
    actions: DetailActions
) {
    val colors = CompanionTheme.colors
    var mode by rememberSaveable(row?.id) { mutableStateOf<FormMode?>(null) }
    var confirmDelete by rememberSaveable(row?.id) { mutableStateOf(false) }
    if (row == null) {
        Column(Modifier.fillMaxSize()) {
            InnerTopBar("Rincian", actions.onBack)
            Column(Modifier.padding(horizontal = 12.dp)) {
                CompanionCard {
                    CardTitle("Item tidak ditemukan")
                    Text("Mungkin sudah dihapus.", style = CompanionType.small, color = colors.text2)
                }
            }
        }
        return
    }
    val item = remember(row) { RowPresentation.of(row) }
    val abilities = remember(item, currentPairing) { RowAbilities.of(item, currentPairing) }
    val purposeReply = remember(row, replies) { OpenQuestions.purposeReply(row, replies) }
    val seed = remember(item) { FormSeed.of(item) }
    val editing = mode

    Column(Modifier.fillMaxSize()) {
        InnerTopBar(editing?.pageTitle ?: "Rincian", onBack = { if (editing != null) mode = null else actions.onBack() }) {
            if (editing == null) SquareIconButton(Icons.Outlined.Delete, "Hapus dari aplikasi", { confirmDelete = true }, enabled = !busy)
        }
        Column(
            Modifier.fillMaxSize().verticalScroll(rememberScrollState()).padding(start = 12.dp, end = 12.dp, top = 4.dp, bottom = 32.dp),
            verticalArrangement = Arrangement.spacedBy(10.dp)
        ) {
            Hero(item)
            if (editing == null) StatePanel(item, abilities, purposeReply, busy, actions) { mode = it }
            if (editing != null) {
                CompanionCard {
                    CardTitle(editing.cardTitle)
                    TransactionForm(
                        seed, editing,
                        canSubmit = when (editing) {
                            FormMode.EDIT_RECORDED -> abilities.canEditRecorded
                            FormMode.MANUAL_RECORD -> abilities.canResolve
                            FormMode.SEND_UNSENT -> abilities.canSubmit
                        },
                        accounts = accounts, categories = categories, busy = busy,
                        onCancel = { mode = null },
                        onInvalid = actions.onInvalid,
                        onSubmit = { draft -> actions.onSubmit(row, editing, abilities.editableTransactionId, draft) { ok -> if (ok) mode = null } }
                    )
                }
            } else {
                RecordedCard(item, abilities) { mode = FormMode.EDIT_RECORDED }
            }
            CompanionCard {
                CardTitle("Notifikasi asli")
                OriginalNotification(row, today)
            }
            if (editing == null) {
                CompanionButton(
                    "Hapus dari aplikasi", { confirmDelete = true }, Modifier.fillMaxWidth(), ButtonKind.DANGER,
                    icon = Icons.Outlined.Delete, enabled = !busy
                )
            }
        }
    }

    if (confirmDelete) {
        AlertDialog(
            onDismissRequest = { confirmDelete = false },
            title = { Text("Hapus dari aplikasi?") },
            text = {
                Text(
                    "Notifikasi ini hilang dari daftar di ponsel dan tidak dikirim ke server. " +
                        "Transaksi yang sudah tercatat di tracker tidak ikut terhapus."
                )
            },
            confirmButton = {
                TextButton(onClick = {
                    confirmDelete = false
                    actions.onDelete(row.id)
                }) { Text("Hapus", color = colors.danger) }
            },
            dismissButton = { TextButton(onClick = { confirmDelete = false }) { Text("Batal", color = colors.text) } },
            containerColor = colors.surface, titleContentColor = colors.text, textContentColor = colors.text2
        )
    }
}

@Composable
private fun Hero(item: RowPresentation) {
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
            "${Formatting.dateTime(item.row.postTime)} · dari ${item.row.appLabel}",
            style = CompanionType.meta.copy(fontSize = 13.sp, fontFeatureSettings = TABULAR_FIGURES), color = colors.text2,
            textAlign = TextAlign.Center
        )
    }
}

/** The state box and its action: question, phone, ignored, review, unsent or processing. */
@Composable
private fun StatePanel(
    item: RowPresentation,
    abilities: RowAbilities,
    purposeReply: SenderReplyEntity?,
    busy: Boolean,
    actions: DetailActions,
    openForm: (FormMode) -> Unit
) {
    val row = item.row
    val question = item.question
    when {
        question != null -> {
            StateBox(StatusTone.ASK, Icons.AutoMirrored.Outlined.HelpOutline, question.title, questionContext(item))
            CompanionButton("Jawab sekarang", { actions.onOpenQuestion(row.id) }, Modifier.fillMaxWidth())
        }
        row.captureOutcome == "keep_local" -> {
            StateBox(
                StatusTone.PHONE, Icons.Outlined.PhoneAndroid, "Disimpan di ponsel",
                "${CaptureReasons.describeKept(row.captureReason)} Notifikasi ini tidak dikirim otomatis " +
                    "dan terhapus setelah 7 hari. Jika ini transaksi, kirim ke tracker."
            )
            CompanionButton("Kirim ke Tracker", { actions.onSendLocal(row.id) }, Modifier.fillMaxWidth(), icon = Icons.AutoMirrored.Outlined.Send, enabled = !busy)
        }
        row.processingStatus == "ignored" -> {
            StateBox(
                StatusTone.MUTED, Icons.Outlined.VisibilityOff, "Diabaikan server",
                "Server tidak mengenali ini sebagai transaksi. Jika ini transaksi dan server sudah diperbarui, kirim ulang untuk diproses lagi."
            )
            CompanionButton("Kirim Ulang", { actions.onResend(row.id) }, Modifier.fillMaxWidth(), ButtonKind.GHOST, Icons.AutoMirrored.Outlined.Send, enabled = !busy)
        }
        abilities.reason != null -> {
            StateBox(
                if (abilities.serverRetrying) StatusTone.MUTED else StatusTone.WARN,
                if (abilities.serverRetrying) Icons.Rounded.Refresh else Icons.Outlined.WarningAmber,
                if (abilities.serverRetrying) "Diulang otomatis" else "Perlu dicek",
                abilities.reason
            )
            if (abilities.canRetry) {
                CompanionButton("Coba Proses Ulang", { actions.onRetry(row.id) }, Modifier.fillMaxWidth(), icon = Icons.Rounded.Refresh, enabled = !busy)
            }
            if (abilities.canResolve) {
                CompanionButton("Catat manual", { openForm(FormMode.MANUAL_RECORD) }, Modifier.fillMaxWidth(), ButtonKind.GHOST, enabled = !busy)
            }
        }
        abilities.canSubmit -> {
            StateBox(
                StatusTone.MUTED, Icons.AutoMirrored.Outlined.Send, "Belum dikirim",
                "Notifikasi ini belum sampai di tracker. Periksa nominal dan jenisnya bila perlu, lalu kirim."
            )
            CompanionButton("Periksa lalu kirim", { openForm(FormMode.SEND_UNSENT) }, Modifier.fillMaxWidth(), ButtonKind.GHOST, enabled = !busy)
        }
        row.processingStatus == "queued" || row.processingStatus == "processing" -> {
            StateBox(StatusTone.MUTED, Icons.Outlined.Schedule, "Diproses", "Tracker sedang memproses notifikasi ini. Hasilnya muncul di sini.")
        }
    }
    if (question == null && purposeReply?.state == "closed") {
        StateBox(StatusTone.MUTED, Icons.AutoMirrored.Outlined.HelpOutline, "Apa ini?", purposeReplyMessage(purposeReply.state, purposeReply.errorCode))
    }
}

@Composable
private fun RecordedCard(item: RowPresentation, abilities: RowAbilities, onEdit: () -> Unit) {
    val colors = CompanionTheme.colors
    val recorded = item.recorded
    CompanionCard {
        CardTitle("Yang dicatat tracker")
        if (recorded == null) {
            Text("Belum ada yang dicatat tracker untuk notifikasi ini.", style = CompanionType.small, color = colors.text2)
        } else {
            RecordedFacts(item)
            item.habit?.let { Note(it.detailLine(), StatusTone.MUTED, Icons.Outlined.History) }
        }
        if (abilities.canEditRecorded) {
            CompanionButton("Ubah transaksi", onEdit, Modifier.fillMaxWidth(), ButtonKind.GHOST, Icons.Outlined.Edit)
        } else if (item.row.processingStatus == "recorded") {
            Text("Transaksi ini hanya bisa diubah di Tracker Web.", style = CompanionType.meta, color = colors.text2)
            CompanionButton("Edit di Tracker Web", {}, Modifier.fillMaxWidth(), ButtonKind.GHOST, enabled = false)
        }
    }
}

@Composable
private fun RecordedFacts(item: RowPresentation) {
    val colors = CompanionTheme.colors
    val recorded = item.recorded ?: return
    val facts = listOfNotNull(
        "Keterangan" to recorded.description.orEmpty(),
        "Nominal" to Formatting.rupiah(recorded.amount!!),
        "Jenis" to when (item.direction) {
            MoneyDirection.IN -> "Masuk"
            MoneyDirection.OUT -> "Keluar"
            else -> "Pindah saldo (tidak dihitung pengeluaran)"
        },
        "Rekening" to item.account,
        recorded.category?.takeIf { it.isNotBlank() }?.let { "Kategori" to it }
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
}
