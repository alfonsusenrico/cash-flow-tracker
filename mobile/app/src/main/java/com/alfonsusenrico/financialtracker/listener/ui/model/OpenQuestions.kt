package com.alfonsusenrico.financialtracker.listener.ui.model

import com.alfonsusenrico.financialtracker.listener.data.model.RawNotificationEntity
import com.alfonsusenrico.financialtracker.listener.data.model.SenderReplyEntity
import com.alfonsusenrico.financialtracker.listener.data.network.model.IngestResultItem
import com.alfonsusenrico.financialtracker.listener.data.sync.PurposeAnswers
import com.alfonsusenrico.financialtracker.listener.data.sync.PurposeQuestions
import com.alfonsusenrico.financialtracker.listener.data.sync.SenderQuestions
import com.alfonsusenrico.financialtracker.listener.data.sync.SourcePocketQuestions
import com.alfonsusenrico.financialtracker.listener.service.MappingConfirmationNotifier
import com.google.gson.Gson

/** The four questions the tracker can ask about a notification; [title] is the Tanya heading. */
enum class QuestionKind(val title: String) {
    PURPOSE("Apa ini?"),
    SENDER("Siapa pengirimnya?"),
    SOURCE_POCKET("Dari rekening atau kantong mana?"),
    MAPPING("Rekening yang mana?")
}

object OpenQuestions {
    /** Kept in step with SourcePocketQuestionControls: these reply states let the owner answer again. */
    private val POCKET_ANSWERABLE_STATES = setOf("invalid", "stale")

    /** The question a row carries, answered or not; null when the row asks nothing. */
    fun kindOf(row: RawNotificationEntity): QuestionKind? = when (row.processingStatus) {
        "recorded" -> QuestionKind.PURPOSE.takeIf {
            row.purposeQuestionId != null &&
                PurposeQuestions.parse(row.resultJson)?.second?.questionId == row.purposeQuestionId
        }
        "needs_confirmation" -> when {
            SenderQuestions.parse(row.resultJson) != null -> QuestionKind.SENDER
            SourcePocketQuestions.parse(row.resultJson) != null -> QuestionKind.SOURCE_POCKET
            mappingProposal(row) != null -> QuestionKind.MAPPING
            else -> null
        }
        else -> null
    }

    /**
     * Whether the row's question still waits for the owner. Purpose rows come from the DAO's
     * needs-answer query ([needsAnswerIds]); sender and pocket rows wait until a reply is stored that the
     * owner cannot replace; mapping confirmations are sent at once and wait until the server settles them.
     */
    fun awaitsOwner(
        row: RawNotificationEntity,
        kind: QuestionKind,
        pendingReplies: List<SenderReplyEntity>,
        needsAnswerIds: Set<Long>
    ): Boolean = when (kind) {
        QuestionKind.PURPOSE -> row.id in needsAnswerIds
        QuestionKind.SENDER -> {
            val questionId = SenderQuestions.parse(row.resultJson)?.confirmation?.questionId
            pendingReplies.none { it.rowId == row.id && it.questionId == questionId && it.state != "invalid" }
        }
        QuestionKind.SOURCE_POCKET -> {
            val questionId = SourcePocketQuestions.parse(row.resultJson)?.confirmation?.questionId
            pendingReplies.none {
                it.rowId == row.id && it.answerKind == "source_pocket" && it.questionId == questionId &&
                    it.state !in POCKET_ANSWERABLE_STATES
            }
        }
        QuestionKind.MAPPING -> true
    }

    /** Open questions, newest first: what Tanya pages through and what "Perlu dijawab" lists. */
    fun open(
        rows: List<RawNotificationEntity>,
        pendingReplies: List<SenderReplyEntity>,
        needsAnswer: List<RawNotificationEntity>
    ): List<RawNotificationEntity> {
        val needsAnswerIds = needsAnswer.map { it.id }.toSet()
        return (rows + needsAnswer).distinctBy { it.id }
            .filter { row -> kindOf(row)?.let { awaitsOwner(row, it, pendingReplies, needsAnswerIds) } == true }
            .sortedByDescending { it.postTime }
    }

    /** The purpose reply shown for a row: the one for its open question, else a closed one. */
    fun purposeReply(row: RawNotificationEntity, replies: List<SenderReplyEntity>): SenderReplyEntity? {
        val own = replies.filter { it.rowId == row.id && it.answerKind == PurposeAnswers.KIND }
        return own.firstOrNull { it.questionId == row.purposeQuestionId }
            ?: own.firstOrNull { row.purposeQuestionId == null && it.state == "closed" }
    }

    /** The question id of the row's account proposal; null for a proposal that predates question ids. */
    fun mappingQuestionId(row: RawNotificationEntity): String? = try {
        Gson().fromJson(row.resultJson, IngestResultItem::class.java)?.confirmation?.questionId
    } catch (_: RuntimeException) {
        null
    }

    fun mappingProposal(row: RawNotificationEntity) = if (row.processingStatus != "needs_confirmation") null else try {
        Gson().fromJson(row.resultJson, IngestResultItem::class.java)
            ?.takeIf(MappingConfirmationNotifier::validProposal)?.mappingProposal
    } catch (_: RuntimeException) {
        null
    }
}
