package com.alfonsusenrico.financialtracker.listener

import com.alfonsusenrico.financialtracker.listener.PurposeFixtures.EVENT
import com.alfonsusenrico.financialtracker.listener.PurposeFixtures.QUESTION
import com.alfonsusenrico.financialtracker.listener.PurposeFixtures.questionJson
import com.alfonsusenrico.financialtracker.listener.PurposeFixtures.result
import com.alfonsusenrico.financialtracker.listener.PurposeFixtures.resultJson
import com.alfonsusenrico.financialtracker.listener.data.network.model.PurposeAnswerRequest
import com.alfonsusenrico.financialtracker.listener.data.network.model.PurposeSuggestion
import com.alfonsusenrico.financialtracker.listener.data.sync.PurposeQuestions
import com.alfonsusenrico.financialtracker.listener.data.sync.PurposeQuestions.Inspection
import com.google.gson.Gson
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class PurposeQuestionsTest {

    @Test fun backendQuestionParsesWithItsSuggestions() {
        val item = result()
        val question = PurposeQuestions.valid(item)!!
        assertEquals(QUESTION, question.questionId)
        assertEquals(listOf("s1", "s2"), question.suggestions!!.map { it.id })
        assertEquals("makan kafe senja 1", question.suggestions!!.first().label)
        assertEquals("Makanan & Minuman", question.suggestions!!.first().categoryName)
        assertEquals("needs_review", item.reviewStatus)
        assertEquals(item to question, PurposeQuestions.parse(resultJson()))
    }

    @Test fun absentQuestionIsNotAnError() {
        assertEquals(Inspection.Absent, PurposeQuestions.inspect(result(question = null)))
        assertNull(PurposeQuestions.parse(resultJson(question = null)))
    }

    @Test fun textOnlyAndSuggestionOnlyQuestionsAreAnswerable() {
        assertTrue(PurposeQuestions.inspect(result(questionJson(suggestions = 0))) is Inspection.Valid)
        assertTrue(PurposeQuestions.inspect(result(questionJson(allowsText = false))) is Inspection.Valid)
    }

    @Test fun malformedQuestionsAreIgnoredWithAReasonCode() {
        val item = result()
        val question = item.question!!
        val suggestion = question.suggestions!!.first()
        fun reason(changed: com.alfonsusenrico.financialtracker.listener.data.network.model.PurposeQuestion) =
            (PurposeQuestions.inspect(item.copy(question = changed)) as Inspection.Malformed).reason

        assertEquals("purpose_question_unsupported", reason(question.copy(type = "future")))
        listOf(
            question.copy(transactionId = EVENT), question.copy(direction = "income"),
            question.copy(amount = 187501), question.copy(currency = "USD")
        ).forEach { assertEquals("purpose_question_mismatch", reason(it)) }
        listOf(
            question.copy(questionId = "not-a-uuid"), question.copy(questionId = null),
            question.copy(prompt = " "), question.copy(prompt = "x".repeat(501)),
            question.copy(allowsText = null), question.copy(textMaxLength = 0), question.copy(textMaxLength = 201),
            question.copy(suggestions = null),
            question.copy(suggestions = List(6) { suggestion.copy(id = "s$it") }),
            question.copy(suggestions = listOf(suggestion, suggestion)),
            question.copy(suggestions = listOf(suggestion.copy(id = "s 1"))),
            question.copy(suggestions = listOf(suggestion.copy(id = "s".repeat(17)))),
            question.copy(suggestions = listOf(suggestion.copy(label = ""))),
            question.copy(suggestions = listOf(suggestion.copy(label = "x".repeat(161)))),
            question.copy(suggestions = emptyList(), allowsText = false)
        ).forEach { assertEquals("purpose_question_invalid", reason(it)) }

        assertEquals(Inspection.Malformed("purpose_question_mismatch"), PurposeQuestions.inspect(item.copy(status = "needs_review")))
        assertEquals(Inspection.Malformed("purpose_question_mismatch"), PurposeQuestions.inspect(item.copy(type = "internal_movement", target = "Jago")))
        assertEquals(Inspection.Malformed("purpose_question_mismatch"), PurposeQuestions.inspect(item.copy(transactionId = null)))
        assertNull(PurposeQuestions.parse("{broken"))
        assertNull(PurposeQuestions.parse(resultJson(questionJson(questionId = "bad"))))
    }

    @Test fun suggestionWithoutCategoryIsStillAnAnswer() {
        val item = result()
        val plain = item.question!!.copy(suggestions = listOf(PurposeSuggestion(id = "s1", kind = "current", label = "Pengeluaran Groceries")))
        assertTrue(PurposeQuestions.inspect(item.copy(question = plain)) is Inspection.Valid)
    }

    @Test fun textAnswersFollowTheBackendRule() {
        assertEquals("sewa lapangan futsal", PurposeQuestions.normalizeText("  sewa lapangan futsal \n"))
        assertEquals("A", PurposeQuestions.normalizeText("Ａ"))
        assertEquals("x".repeat(200), PurposeQuestions.normalizeText("x".repeat(200)))
        assertEquals("x".repeat(10), PurposeQuestions.normalizeText("x".repeat(10), limit = 10))
        listOf("", "   ", "x".repeat(201), "a\u0000b", "a​b", "ab", "a\nb").forEach {
            assertNull(it, PurposeQuestions.normalizeText(it))
        }
        assertNull(PurposeQuestions.normalizeText("x".repeat(11), limit = 10))
    }

    @Test fun amountsAndAccountsUseTheBackendNotation() {
        assertEquals("Rp187.500", PurposeQuestions.rupiah(187500))
        assertEquals("Rp1.250.000", PurposeQuestions.rupiah(1_250_000))
        assertEquals("BCA Harian", PurposeQuestions.accountLabel(result()))
        assertEquals("GoPay", PurposeQuestions.accountLabel(result().copy(type = "income", source = null, target = "GoPay")))
    }

    @Test fun answerBodyCarriesExactlyOneAnswer() {
        val gson = Gson()
        val suggestion = gson.toJsonTree(PurposeAnswerRequest(QUESTION, EVENT, suggestionId = "s1")).asJsonObject
        assertEquals(setOf("question_id", "reply_id", "suggestion_id"), suggestion.keySet())
        val text = gson.toJsonTree(PurposeAnswerRequest(QUESTION, EVENT, text = "sewa lapangan")).asJsonObject
        assertEquals(setOf("question_id", "reply_id", "text"), text.keySet())
    }
}
