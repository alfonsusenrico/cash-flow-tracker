package com.alfonsusenrico.financialtracker.listener.ui.questions

import androidx.compose.foundation.background
import androidx.compose.foundation.border
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.verticalScroll
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.automirrored.rounded.ArrowBack
import androidx.compose.material.icons.automirrored.rounded.ArrowForward
import androidx.compose.material.icons.rounded.Check
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.DisposableEffect
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.semantics.heading
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.text.style.TextAlign
import androidx.compose.ui.unit.dp
import com.alfonsusenrico.financialtracker.listener.data.model.AnswerAttemptEntity
import com.alfonsusenrico.financialtracker.listener.data.model.RawNotificationEntity
import com.alfonsusenrico.financialtracker.listener.data.model.SenderReplyEntity
import com.alfonsusenrico.financialtracker.listener.data.network.model.AccountItemResponse
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.data.sync.AnswerAttempts
import com.alfonsusenrico.financialtracker.listener.data.sync.PurposeQuestions
import com.alfonsusenrico.financialtracker.listener.data.sync.SenderQuestions
import com.alfonsusenrico.financialtracker.listener.data.sync.SourcePocketQuestions
import com.alfonsusenrico.financialtracker.listener.data.sync.purposeReplyMessage
import com.alfonsusenrico.financialtracker.listener.data.sync.validUuid
import com.alfonsusenrico.financialtracker.listener.ui.components.ButtonKind
import com.alfonsusenrico.financialtracker.listener.ui.components.CardTitle
import com.alfonsusenrico.financialtracker.listener.ui.components.CompanionButton
import com.alfonsusenrico.financialtracker.listener.ui.components.CompanionCard
import com.alfonsusenrico.financialtracker.listener.ui.components.Formatting
import com.alfonsusenrico.financialtracker.listener.ui.components.MoneyText
import com.alfonsusenrico.financialtracker.listener.ui.components.OriginalNotification
import com.alfonsusenrico.financialtracker.listener.ui.components.toneColors
import com.alfonsusenrico.financialtracker.listener.data.reminder.QuestionUrgency
import androidx.compose.material.icons.outlined.Bedtime
import androidx.compose.material.icons.outlined.Bolt
import androidx.compose.material3.Icon
import androidx.compose.foundation.layout.size
import androidx.compose.ui.text.font.FontWeight
import com.alfonsusenrico.financialtracker.listener.ui.components.SquareIconButton
import com.alfonsusenrico.financialtracker.listener.ui.components.StateBox
import com.alfonsusenrico.financialtracker.listener.ui.components.StatusBadge
import com.alfonsusenrico.financialtracker.listener.ui.model.OpenQuestions
import com.alfonsusenrico.financialtracker.listener.ui.model.QuestionKind
import com.alfonsusenrico.financialtracker.listener.ui.model.RowPresentation
import com.alfonsusenrico.financialtracker.listener.ui.model.StatusTone
import com.alfonsusenrico.financialtracker.listener.ui.shell.CompanionViewModel
import com.alfonsusenrico.financialtracker.listener.ui.shell.ShellActions
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionTheme
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionType
import kotlinx.coroutines.flow.flowOf
import java.time.LocalDate
import kotlin.math.roundToInt

/** Answers from Tanya; each reports through the shell's messages, as the detail sheet's did. */
class QuestionAnswers(
    val purpose: (row: Long, questionId: String, suggestionId: String?, text: String?) -> Unit,
    val sender: (row: Long, questionId: String, name: String?) -> Unit,
    val sourcePocket: (row: Long, questionId: String, accountId: String) -> Unit,
    val refreshPockets: (row: Long) -> Unit,
    val mapping: (row: Long, accountId: String) -> Unit
)

/** The answer callbacks the question sheet and the hub's top question share; results go to [say]. */
fun questionAnswers(viewModel: CompanionViewModel, say: (String) -> Unit) = QuestionAnswers(
    purpose = { id, question, suggestion, text ->
        viewModel.answerPurpose(id, question, suggestion, text) { ok, error ->
            say(if (ok) "Jawaban tersimpan; mengirim ke server" else error ?: "Jawaban belum tersimpan")
        }
    },
    sender = { id, question, name ->
        viewModel.confirmSender(id, question, name) { ok, error ->
            say(if (ok) "Balasan tersimpan; menunggu server" else error ?: "Balasan belum tersimpan")
        }
    },
    sourcePocket = { id, question, account ->
        viewModel.confirmSourcePocket(id, question, account) { ok, error ->
            say(if (ok) "Pilihan tersimpan; menunggu pencatatan" else error ?: "Pilihan belum tersimpan")
        }
    },
    refreshPockets = viewModel::loadSourcePocketOptions,
    mapping = { id, account ->
        viewModel.confirmMapping(id, account) { ok, error ->
            say(if (ok) "Rekening dikonfirmasi; notifikasi diproses ulang" else "Gagal konfirmasi: ${error ?: "periksa koneksi"}")
        }
    }
)

/** Where a question sits: urgent ones are asked now, light ones can wait for the evening reminder. */
fun urgencyLabel(row: RawNotificationEntity, reminderLabel: String?): String =
    if (reminderLabel != null && QuestionUrgency.isLight(OpenQuestions.kindOf(row))) "Nanti malam · $reminderLabel" else "Perlu sekarang"

@Composable
fun QuestionsRoute(
    viewModel: CompanionViewModel,
    actions: ShellActions,
    requestedRowId: Long?,
    reminderLabel: String?,
    onOpenDetail: (Long) -> Unit,
    onHome: () -> Unit
) {
    val context = LocalContext.current
    val lists by viewModel.lists.collectAsState()
    val accounts by viewModel.accounts.collectAsState()
    val busy by viewModel.isUpdating.collectAsState()
    val pocketOptions by viewModel.sourcePocketOptions.collectAsState()
    val attempts by viewModel.answerAttempts.collectAsState()
    val ordered = remember(lists.questions) { QuestionUrgency.ordered(lists.questions, OpenQuestions::kindOf) }
    var currentId by rememberSaveable(requestedRowId) { mutableStateOf(requestedRowId) }
    val shownId = currentId ?: ordered.firstOrNull()?.id
    val row by remember(shownId) { shownId?.let(viewModel::row) ?: flowOf(null) }.collectAsState(initial = null)
    val pairing = remember { AppPreferences(context).pairingContext }

    LaunchedEffect(shownId) {
        // Pin the shown question, so answering it does not jump to the next one before the owner sees the result.
        if (currentId == null && shownId != null) currentId = shownId
        viewModel.focusRow(shownId)
    }
    DisposableEffect(Unit) { onDispose { viewModel.focusRow(null) } }

    QuestionsScreen(
        row = row?.takeIf { it.id == shownId },
        questions = ordered,
        replies = lists.replies,
        currentPairing = pairing,
        accounts = accounts,
        pocketOptions = pocketOptions,
        busy = busy,
        today = LocalDate.now(),
        onShow = { currentId = it },
        onOpenDetail = onOpenDetail,
        onHome = onHome,
        answers = remember(viewModel) { questionAnswers(viewModel, actions.message) },
        attempts = attempts,
        reminderLabel = reminderLabel
    )
}

/**
 * Tanya: one question per page. The original notification comes first, then the question card, then the
 * pager. A question answered here stays on screen with its state until the owner moves on.
 */
@Composable
fun QuestionsScreen(
    row: RawNotificationEntity?,
    questions: List<RawNotificationEntity>,
    replies: List<SenderReplyEntity>,
    currentPairing: String,
    accounts: List<AccountItemResponse>,
    pocketOptions: SourcePocketOptionState,
    busy: Boolean,
    today: LocalDate,
    onShow: (Long) -> Unit,
    onOpenDetail: (Long) -> Unit,
    onHome: () -> Unit,
    answers: QuestionAnswers,
    attempts: List<AnswerAttemptEntity> = emptyList(),
    reminderLabel: String? = null
) {
    val index = row?.let { r -> questions.indexOfFirst { it.id == r.id } } ?: -1
    val subtitle = when {
        index >= 0 -> "${index + 1} dari ${questions.size} menunggu"
        questions.isNotEmpty() -> "${questions.size} menunggu"
        else -> "Tidak ada yang menunggu"
    }
    Column(Modifier.fillMaxSize()) {
        Text(subtitle, style = CompanionType.small, color = CompanionTheme.colors.text2, modifier = Modifier.padding(horizontal = 18.dp))
        Column(
            Modifier.fillMaxSize().verticalScroll(rememberScrollState()).padding(start = 12.dp, end = 12.dp, top = 4.dp, bottom = 32.dp),
            verticalArrangement = Arrangement.spacedBy(10.dp)
        ) {
            if (row == null) {
                CompanionCard {
                    CardTitle("Semua terjawab", Icons.Rounded.Check)
                    Text(
                        "Aku kabarin kalau ada yang baru.",
                        style = CompanionType.small, color = CompanionTheme.colors.text2
                    )
                    CompanionButton("Kembali ke beranda", onHome, Modifier.fillMaxWidth(), ButtonKind.GHOST)
                }
                return@Column
            }
            if (index >= 0) UrgencyTag(urgencyLabel(row, reminderLabel))
            OriginalNotification(row, today)
            QuestionCard(row, index >= 0, questions, replies, attempts, currentPairing, accounts, pocketOptions, busy, onShow, onOpenDetail, onHome, answers)
            if (index >= 0) Pager(index, questions, onShow)
        }
    }
}

/** "Perlu sekarang" or "Nanti malam · 20.00", with an icon so the meaning never rests on colour. */
@Composable
fun UrgencyTag(label: String) {
    val (bg, fg) = toneColors(if (label == "Perlu sekarang") StatusTone.ASK else StatusTone.MUTED)
    Row(
        Modifier.clip(RoundedCornerShape(999.dp)).background(bg).padding(start = 8.dp, end = 10.dp, top = 4.dp, bottom = 4.dp),
        verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(5.dp)
    ) {
        Icon(if (label == "Perlu sekarang") Icons.Outlined.Bolt else Icons.Outlined.Bedtime, null, tint = fg, modifier = Modifier.size(14.dp))
        Text(label, style = CompanionType.meta.copy(fontWeight = FontWeight(650)), color = fg)
    }
}

/**
 * The hub's top question: its tag, the original notification and the same controls as the question sheet,
 * so it is answered in place. The shell focuses its row while no sheet is open, for source-pocket options.
 */
@Composable
fun HubQuestion(viewModel: CompanionViewModel, actions: ShellActions, row: RawNotificationEntity, reminderLabel: String?, onOpenDetail: (Long) -> Unit) {
    val context = LocalContext.current
    val lists by viewModel.lists.collectAsState()
    val accounts by viewModel.accounts.collectAsState()
    val busy by viewModel.isUpdating.collectAsState()
    val pocketOptions by viewModel.sourcePocketOptions.collectAsState()
    val attempts by viewModel.answerAttempts.collectAsState()
    val pairing = remember { AppPreferences(context).pairingContext }
    val answers = remember(viewModel) { questionAnswers(viewModel, actions.message) }
    Column(verticalArrangement = Arrangement.spacedBy(10.dp)) {
        UrgencyTag(urgencyLabel(row, reminderLabel))
        OriginalNotification(row, LocalDate.now())
        QuestionCard(
            row, open = true, questions = lists.questions, replies = lists.replies, attempts = attempts, currentPairing = pairing,
            accounts = accounts, pocketOptions = pocketOptions, busy = busy, onShow = {}, onOpenDetail = onOpenDetail,
            onHome = {}, answers = answers
        )
    }
}

@Composable
private fun QuestionCard(
    row: RawNotificationEntity,
    open: Boolean,
    questions: List<RawNotificationEntity>,
    replies: List<SenderReplyEntity>,
    attempts: List<AnswerAttemptEntity>,
    currentPairing: String,
    accounts: List<AccountItemResponse>,
    pocketOptions: SourcePocketOptionState,
    busy: Boolean,
    onShow: (Long) -> Unit,
    onOpenDetail: (Long) -> Unit,
    onHome: () -> Unit,
    answers: QuestionAnswers
) {
    val colors = CompanionTheme.colors
    val item = remember(row) { RowPresentation.of(row) }
    val kind = item.question
    val shape = RoundedCornerShape(22.dp)
    val canAnswer = row.isSynced && row.pairingContext == currentPairing &&
        (kind == QuestionKind.PURPOSE || validUuid(row.backendEventId))
    Column(
        Modifier.fillMaxWidth().clip(shape).background(colors.surface).border(1.dp, colors.border, shape)
            .padding(horizontal = 16.dp, vertical = 18.dp),
        verticalArrangement = Arrangement.spacedBy(12.dp)
    ) {
        Column(verticalArrangement = Arrangement.spacedBy(6.dp)) {
            StatusBadge(item.status)
            MoneyText(item.amount, item.direction, CompanionType.questionAmount)
        }
        if (kind == null) {
            ClosedQuestion(row, replies, onOpenDetail)
            NextAction(questions, row.id, onShow, onHome)
            return@Column
        }
        Text(kind.title, style = CompanionType.questionTitle, color = colors.text, modifier = Modifier.semantics { heading() })
        Text(questionContext(item), style = CompanionType.small.copy(fontSize = CompanionType.body.fontSize), color = colors.text2)
        when (kind) {
            QuestionKind.PURPOSE -> {
                val question = PurposeQuestions.parse(row.resultJson)?.second ?: return@Column
                val reply = OpenQuestions.purposeReply(row, replies)
                val unsaved = AnswerAttempts.noticeFor(attempts, row.backendEventId, question.questionId)
                PurposeQuestionControls(question, reply?.state, reply?.errorCode, busy, canAnswer, unsaved) { suggestion, text ->
                    answers.purpose(row.id, question.questionId!!, suggestion, text)
                }
            }
            QuestionKind.SENDER -> {
                val question = SenderQuestions.parse(row.resultJson)?.confirmation ?: return@Column
                val state = replies.firstOrNull { it.rowId == row.id && it.questionId == question.questionId }?.state
                val unsaved = AnswerAttempts.noticeFor(attempts, row.backendEventId, question.questionId)
                SenderQuestionControls(question, state, busy, canAnswer, unsaved) { name -> answers.sender(row.id, question.questionId!!, name) }
            }
            QuestionKind.SOURCE_POCKET -> {
                val question = SourcePocketQuestions.parse(row.resultJson)?.confirmation ?: return@Column
                val reply = replies.firstOrNull {
                    it.rowId == row.id && it.answerKind == "source_pocket" && it.questionId == question.questionId
                }
                SourcePocketQuestionControls(
                    question, pocketOptions.takeIf { it.rowId == row.id } ?: SourcePocketOptionState(),
                    reply?.state, reply?.accountId, busy, canAnswer,
                    onRefresh = { answers.refreshPockets(row.id) },
                    onAnswer = { answers.sourcePocket(row.id, question.questionId!!, it) }
                )
            }
            QuestionKind.MAPPING -> {
                val proposal = OpenQuestions.mappingProposal(row) ?: return@Column
                val unsaved = AnswerAttempts.noticeFor(attempts, row.backendEventId, OpenQuestions.mappingQuestionId(row))
                MappingQuestionControls(proposal, accounts, busy, canAnswer, unsaved) { answers.mapping(row.id, it) }
            }
        }
        if (!open) NextAction(questions, row.id, onShow, onHome)
    }
}

/** A question the server closed or that was answered elsewhere: nothing to answer here. */
@Composable
private fun ClosedQuestion(row: RawNotificationEntity, replies: List<SenderReplyEntity>, onOpenDetail: (Long) -> Unit) {
    val reply = OpenQuestions.purposeReply(row, replies)
    val note = reply?.takeIf { it.state == "closed" }?.let { purposeReplyMessage(it.state, it.errorCode) }
        ?: "Pertanyaan ini sudah selesai di tracker."
    StateBox(StatusTone.MUTED, Icons.Rounded.Check, "Ditutup", "$note Tidak perlu dijawab di sini.")
    CompanionButton("Lihat transaksi", { onOpenDetail(row.id) }, Modifier.fillMaxWidth(), ButtonKind.GHOST)
}

@Composable
private fun NextAction(questions: List<RawNotificationEntity>, shownId: Long, onShow: (Long) -> Unit, onHome: () -> Unit) {
    val next = questions.firstOrNull { it.id != shownId }
    if (next != null) {
        val left = questions.count { it.id != shownId }
        CompanionButton("Berikutnya · $left lagi", { onShow(next.id) }, Modifier.fillMaxWidth(), icon = Icons.AutoMirrored.Rounded.ArrowForward)
    } else {
        CompanionButton("Kembali ke beranda", onHome, Modifier.fillMaxWidth())
    }
}

@Composable
private fun Pager(index: Int, questions: List<RawNotificationEntity>, onShow: (Long) -> Unit) {
    val previous = questions.getOrNull(index - 1)
    val next = questions.getOrNull(index + 1)
    Row(Modifier.fillMaxWidth(), verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(8.dp)) {
        SquareIconButton(Icons.AutoMirrored.Rounded.ArrowBack, "Pertanyaan sebelumnya", { previous?.let { onShow(it.id) } }, enabled = previous != null)
        Text(
            "${index + 1} dari ${questions.size}", style = CompanionType.button, color = CompanionTheme.colors.text,
            textAlign = TextAlign.Center, modifier = Modifier.weight(1f)
        )
        SquareIconButton(Icons.AutoMirrored.Rounded.ArrowForward, "Pertanyaan berikutnya", { next?.let { onShow(it.id) } }, enabled = next != null)
    }
}

/** One line of context under the question title, from the question the tracker sent. */
fun questionContext(item: RowPresentation): String {
    val row = item.row
    return when (item.question) {
        QuestionKind.PURPOSE -> PurposeQuestions.parse(row.resultJson)?.second?.prompt.orEmpty()
        QuestionKind.SENDER -> SenderQuestions.parse(row.resultJson)?.confirmation?.let {
            "${Formatting.rupiah(it.amount ?: 0)} masuk ke ${it.receivingAccount?.label} dari ${it.maskedSender}"
        }.orEmpty()
        QuestionKind.SOURCE_POCKET -> SourcePocketQuestions.parse(row.resultJson)?.confirmation?.let {
            Formatting.rupiah(it.amount ?: 0) + it.destinationLabel?.let { label -> " ke $label" }.orEmpty() + " · ${it.jagoAccount?.label}"
        }.orEmpty()
        QuestionKind.MAPPING -> OpenQuestions.mappingProposal(row)?.let {
            val percent = ((it.confidence ?: 0.0) * 100).roundToInt()
            "“${it.name}” dari notifikasi belum dikenal. Usulan: ${it.proposed?.label} (yakin $percent%)."
        }.orEmpty()
        null -> ""
    }
}
