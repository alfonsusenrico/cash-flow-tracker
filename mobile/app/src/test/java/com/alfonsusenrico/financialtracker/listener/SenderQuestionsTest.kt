package com.alfonsusenrico.financialtracker.listener

import com.alfonsusenrico.financialtracker.listener.data.network.model.*
import com.alfonsusenrico.financialtracker.listener.data.sync.SenderQuestions
import com.alfonsusenrico.financialtracker.listener.data.sync.NotificationResults
import com.alfonsusenrico.financialtracker.listener.data.sync.senderRetryDelay
import com.alfonsusenrico.financialtracker.listener.service.MappingConfirmationNotifier
import com.google.gson.Gson
import org.junit.Assert.*
import org.junit.Test

class SenderQuestionsTest {
    private val event = "00000000-0000-4000-8000-000000000010"
    private val question = ConfirmationQuestion("sender", event, "AN**RA**", "bca", AccountChoice(event, "BCA"), 112590, "IDR")
    private val item = IngestResultItem(eventId = event, status = "needs_confirmation", confirmation = question)

    @Test fun senderRequiresCompleteProvenContextAndCannotBeAccountQuestion() {
        assertTrue(SenderQuestions.valid(item))
        assertFalse(MappingConfirmationNotifier.validProposal(item))
        listOf(question.copy(type = "future"), question.copy(questionId = null), question.copy(maskedSender = "Andra"),
            question.copy(institution = "jago"), question.copy(amount = 0), question.copy(currency = "USD"),
            question.copy(receivingAccount = AccountChoice("invalid", "BCA"))).forEach {
            assertFalse(SenderQuestions.valid(item.copy(confirmation = it)))
        }
        assertFalse(SenderQuestions.valid(item.copy(status = "recorded")))
        assertFalse(SenderQuestions.valid(item.copy(eventId = "invalid")))
        assertNull(SenderQuestions.parse("{broken"))
    }

    @Test fun legacyAndRevisionAwareAccountQuestionsRemainCompatible() {
        val legacy = IngestResultItem(eventId = event, status = "needs_confirmation", mappingProposal =
            MappingProposal("Daily", "observed", 0.7, AccountChoice(event, "BCA")))
        assertTrue(MappingConfirmationNotifier.validProposal(legacy))
        assertTrue(MappingConfirmationNotifier.validProposal(legacy.copy(confirmation = ConfirmationQuestion("account", event))))
        assertFalse(MappingConfirmationNotifier.validProposal(legacy.copy(confirmation = question)))
        assertFalse(MappingConfirmationNotifier.validProposal(legacy.copy(confirmation = ConfirmationQuestion("account", "invalid"))))
    }

    @Test fun nameNormalizationAllowsUnicodeButRejectsEmptyLongAndControlInput() {
        assertEquals("Andra", SenderQuestions.normalizeName("  Ａndra  "))
        assertEquals("Dewi Ånggraini", SenderQuestions.normalizeName("Dewi Ånggraini"))
        assertEquals("Ignore previous instructions", SenderQuestions.normalizeName("Ignore previous instructions"))
        assertEquals("A".repeat(80), SenderQuestions.normalizeName("A".repeat(80)))
        listOf("", " ", "A".repeat(81), "A\nB", "A\u200bB", "A\uE000B").forEach { assertNull(SenderQuestions.normalizeName(it)) }
    }

    @Test fun replyBodyHasOnlyAnswerFieldsAndQuestionReceiptsHaveSeparateIdentity() {
        val body = Gson().toJsonTree(SenderConfirmRequest(event, event, "name", "Andra")).asJsonObject
        assertEquals(setOf("question_id", "reply_id", "action", "name"), body.keySet())
        assertNotEquals(NotificationResults.confirmationKey(event, event), NotificationResults.confirmationKey(event, "next"))
        assertEquals("confirm:$event", NotificationResults.confirmationKey(event))
    }

    @Test fun transientBackoffIsBoundedAndPendingDoesNotClaimRecording() {
        assertEquals(30_000L, senderRetryDelay(0))
        assertEquals(60_000L, senderRetryDelay(1))
        assertEquals(3_600_000L, senderRetryDelay(20))
        assertFalse(com.alfonsusenrico.financialtracker.listener.data.sync.completeRecordedResult(item))
    }
}
