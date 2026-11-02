package com.alfonsusenrico.financialtracker.listener

import com.alfonsusenrico.financialtracker.listener.data.network.model.*
import com.alfonsusenrico.financialtracker.listener.data.sync.SourcePocketQuestions
import com.google.gson.Gson
import org.junit.Assert.*
import org.junit.Test
import java.util.UUID

class SourcePocketQuestionsTest {
    private fun uuid() = UUID.randomUUID().toString()
    private val question = ConfirmationQuestion(type = "source_pocket", questionId = uuid(), institution = "jago",
        jagoAccount = AccountChoice(uuid(), "Bank Jago"), amount = 125000, currency = "IDR")
    private val item = IngestResultItem(eventId = uuid(), status = "needs_confirmation", confirmation = question)

    @Test fun validatesQuestionWithoutMakingASelection() {
        assertTrue(SourcePocketQuestions.valid(item))
        for (bad in listOf(question.copy(type = "sender"), question.copy(institution = "bca"),
            question.copy(questionId = "bad"), question.copy(jagoAccount = null), question.copy(amount = 0),
            question.copy(currency = "USD"))) {
            assertFalse(SourcePocketQuestions.valid(item.copy(confirmation = bad)))
        }
        assertFalse(SourcePocketQuestions.valid(item.copy(status = "recorded")))
    }

    @Test fun completeCachedOptionsAreBoundToEventAndQuestion() {
        val choices = (1..5).map { AccountChoice(uuid(), "Bank Jago · Kantong $it") }
        val options = SourcePocketOptions(item.eventId, question.questionId, choices)
        assertTrue(SourcePocketQuestions.validOptions(options, item))
        assertEquals(5, SourcePocketQuestions.cached(Gson().toJson(options), item)!!.options!!.size)
        assertFalse(SourcePocketQuestions.validOptions(options.copy(eventId = uuid()), item))
        assertFalse(SourcePocketQuestions.validOptions(options.copy(questionId = uuid()), item))
        assertFalse(SourcePocketQuestions.validOptions(options.copy(options = choices + choices[0]), item))
        assertFalse(SourcePocketQuestions.validOptions(options.copy(options = emptyList()), item))
        assertNull(SourcePocketQuestions.parse("not-json"))
        assertNull(SourcePocketQuestions.cached("not-json", item))
    }
}
