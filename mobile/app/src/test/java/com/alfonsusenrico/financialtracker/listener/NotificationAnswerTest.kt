package com.alfonsusenrico.financialtracker.listener

import android.app.KeyguardManager
import android.content.Context
import android.content.Intent
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import com.alfonsusenrico.financialtracker.listener.data.model.AnswerAttemptEntity
import com.alfonsusenrico.financialtracker.listener.data.sync.AnswerAttempts
import com.alfonsusenrico.financialtracker.listener.data.sync.NotificationAnswer
import com.alfonsusenrico.financialtracker.listener.data.sync.NotificationResults
import com.alfonsusenrico.financialtracker.listener.data.sync.SenderReplies
import com.alfonsusenrico.financialtracker.listener.receiver.PurposeAnswerReceiver
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.Shadows.shadowOf
import org.robolectric.annotation.Config

/** The shared steps of a notification answer, and the trace each answer kind leaves. */
@RunWith(AndroidJUnit4::class)
@Config(sdk = [34], application = TestApplication::class)
class NotificationAnswerTest {
    private val context = ApplicationProvider.getApplicationContext<Context>()
    private lateinit var server: MockWebServer
    private lateinit var harness: AnswerPathHarness
    private val event = "00000000-0000-4000-8000-000000000901"
    private val question = "00000000-0000-4000-8000-000000000902"
    private val messages = NotificationAnswer.Messages(notSaved = "Belum tersimpan.", locked = "Terkunci.", missingInput = "Kosong.")
    private val offered = mutableListOf<String>()

    @Before fun open() {
        server = MockWebServer().apply { start() }
        harness = AnswerPathHarness(context, server.url("/").toString())
    }

    @After fun close() {
        harness.close()
        server.shutdown()
    }

    private fun answer(timeoutMillis: Long = 8_000) =
        NotificationAnswer(harness.db, AnswerAttempts.PURPOSE, 1, event, question, messages, timeoutMillis)

    private fun run(
        answer: NotificationAnswer = answer(), locked: Boolean = false, missingInput: Boolean = false,
        store: suspend () -> SenderReplies.Outcome
    ) = runBlocking { answer.run(locked, missingInput, store) { offered += it } }

    private fun steps(event: String = this.event, question: String? = this.question) =
        runBlocking { harness.db.answerAttemptDao().forQuestion(event, question) }.map { it.outcome }

    @Test fun storedAnswerIsTracedAndNotOfferedAgain() {
        run { SenderReplies.Outcome(true, null, AnswerAttempts.QUEUED) }
        assertEquals(listOf(AnswerAttempts.RECEIVED, AnswerAttempts.QUEUED), steps())
        assertTrue(offered.isEmpty())
    }

    @Test fun refusalFromTheStoreKeepsItsReasonAndMessage() {
        run { SenderReplies.Outcome(false, "Pertanyaan sudah berubah.", AnswerAttempts.QUESTION_CHANGED) }
        assertEquals(listOf(AnswerAttempts.RECEIVED, AnswerAttempts.QUESTION_CHANGED), steps())
        assertEquals(listOf("Pertanyaan sudah berubah."), offered)
    }

    @Test fun lockedAndMissingInputAreRefusedBeforeStoring() {
        run(locked = true) { error("A locked answer must not be stored") }
        run(missingInput = true) { error("A missing answer must not be stored") }
        assertEquals(
            listOf(AnswerAttempts.RECEIVED, AnswerAttempts.LOCKED, AnswerAttempts.RECEIVED, AnswerAttempts.MISSING_INPUT),
            steps()
        )
        assertEquals(listOf("Terkunci.", "Kosong."), offered)
    }

    @Test fun storeThatOutlastsTheBudgetIsRecordedAsATimeout() {
        run(answer(timeoutMillis = 50)) {
            delay(5_000)
            SenderReplies.Outcome(true, null, AnswerAttempts.QUEUED)
        }
        assertEquals(listOf(AnswerAttempts.RECEIVED, AnswerAttempts.TIMEOUT), steps())
        assertEquals(listOf("Belum tersimpan."), offered)
    }

    @Test fun failingStoreIsRecordedAsAnError() {
        run { throw IllegalStateException("fictional storage failure") }
        assertEquals(listOf(AnswerAttempts.RECEIVED, AnswerAttempts.ERROR), steps())
        assertEquals(listOf("Belum tersimpan."), offered)
    }

    @Test fun failingReofferDoesNotEscapeTheReceiver() = runBlocking {
        answer().run(false, false, { SenderReplies.Outcome(false, "x", AnswerAttempts.INVALID_ANSWER) }) {
            throw IllegalStateException("fictional notification failure")
        }
        assertEquals(listOf(AnswerAttempts.RECEIVED, AnswerAttempts.INVALID_ANSWER), steps())
    }

    @Test fun purposePromptRefusalIsTracedWithItsKind() = runBlocking {
        val rowId = harness.insertRow(PurposeFixtures.HASH)
        val results = NotificationResults(context, harness.db, scheduleFollowUp = { _, _ -> })
        results.consume(harness.db.notificationDao().getById(rowId)!!, PurposeFixtures.result(), harness.pairing)
        results.deliverPendingAlerts(harness.pairing)
        shadowOf(context.getSystemService(KeyguardManager::class.java)).setIsDeviceLocked(true)
        harness.shown("notification:${PurposeFixtures.EVENT}")!!.actions[0].actionIntent.send()
        harness.settle { steps(PurposeFixtures.EVENT, PurposeFixtures.QUESTION).size == 2 }
        val recorded = harness.db.answerAttemptDao().forQuestion(PurposeFixtures.EVENT, PurposeFixtures.QUESTION)
        assertEquals(listOf(AnswerAttempts.RECEIVED, AnswerAttempts.LOCKED), recorded.map { it.outcome })
        assertTrue(recorded.all { it.answerKind == AnswerAttempts.PURPOSE && it.channel == AnswerAttempts.NOTIFICATION && it.rowId == rowId })
        assertEquals("Buka kunci lalu jawab lagi.", harness.text(harness.shown("notification:${PurposeFixtures.EVENT}")))
    }

    @Test fun accountConfirmationRefusalIsTracedWithItsKind() = runBlocking {
        val rowId = harness.insertRow(MappingFixtures.HASH)
        NotificationResults(context, harness.db, scheduleFollowUp = { _, _ -> })
            .consume(harness.db.notificationDao().getById(rowId)!!, MappingFixtures.proposal(), harness.pairing)
        server.enqueue(MockResponse().setResponseCode(503))
        harness.shown(MappingFixtures.TAG)!!.actions[0].actionIntent.send()
        harness.settle { steps(MappingFixtures.EVENT, MappingFixtures.QUESTION).size == 2 }
        val recorded = harness.db.answerAttemptDao().forQuestion(MappingFixtures.EVENT, MappingFixtures.QUESTION)
        assertEquals(listOf(AnswerAttempts.RECEIVED, AnswerAttempts.SERVER_REFUSED), recorded.map { it.outcome })
        assertTrue(recorded.all { it.answerKind == AnswerAttempts.ACCOUNT && it.rowId == rowId })
        assertEquals(2, harness.shown(MappingFixtures.TAG)?.actions?.size)
    }

    @Test fun malformedPurposeActionIsTracedWithoutIdentifiers() {
        context.sendBroadcast(Intent(context, PurposeAnswerReceiver::class.java).setAction(PurposeAnswerReceiver.ACTION_ANSWER))
        var recorded = emptyList<AnswerAttemptEntity>()
        harness.settle {
            recorded = runBlocking { harness.db.answerAttemptDao().recentOutcomesFlow().first() }
            recorded.isNotEmpty()
        }
        assertEquals(AnswerAttempts.MALFORMED, recorded.single().outcome)
        assertEquals(AnswerAttempts.PURPOSE, recorded.single().answerKind)
        assertEquals(null, recorded.single().eventId)
    }
}
