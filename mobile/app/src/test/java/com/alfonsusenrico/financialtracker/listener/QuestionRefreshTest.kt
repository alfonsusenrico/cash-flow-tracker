package com.alfonsusenrico.financialtracker.listener

import android.content.Context
import androidx.room.Room
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import com.alfonsusenrico.financialtracker.listener.PurposeFixtures.EVENT
import com.alfonsusenrico.financialtracker.listener.PurposeFixtures.HASH
import com.alfonsusenrico.financialtracker.listener.PurposeFixtures.QUESTION
import com.alfonsusenrico.financialtracker.listener.PurposeFixtures.SOURCE_QUESTION
import com.alfonsusenrico.financialtracker.listener.PurposeFixtures.ignoredJson
import com.alfonsusenrico.financialtracker.listener.PurposeFixtures.questionJson
import com.alfonsusenrico.financialtracker.listener.PurposeFixtures.sourcePocketJson
import com.alfonsusenrico.financialtracker.listener.PurposeFixtures.resultJson
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.model.RawNotificationEntity
import com.alfonsusenrico.financialtracker.listener.data.network.model.IngestResultItem
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.data.sync.NotificationResults
import com.alfonsusenrico.financialtracker.listener.data.sync.PurposeAnswers
import com.alfonsusenrico.financialtracker.listener.data.sync.QuestionRefresh
import com.alfonsusenrico.financialtracker.listener.data.sync.purposeReplyMessage
import com.google.gson.Gson
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.annotation.Config
import java.util.concurrent.TimeUnit

/** Follow-up checks of open questions ("Apa ini?" and confirmations) and what a refreshed result does to the row. */
@RunWith(AndroidJUnit4::class)
@Config(sdk = [34], application = TestApplication::class)
class QuestionRefreshTest {
    private val context = ApplicationProvider.getApplicationContext<Context>()
    private lateinit var db: AppDatabase
    private lateinit var server: MockWebServer
    private lateinit var pairing: String
    private val capturedAt = 1_800_000_000_000L
    private var clock = capturedAt + 5_000L
    private val followUps = mutableListOf<Pair<String, Long>>()
    private val purposeAlerts = mutableListOf<Boolean>()
    private val recordAlerts = mutableListOf<Pair<IngestResultItem, Boolean>>()
    private val confirmations = mutableListOf<String?>()
    private val clearedConfirmations = mutableListOf<String>()
    private var rowId = 0L

    private val results get() = NotificationResults(
        context, db,
        postConfirmation = { _, item -> confirmations.add(item.confirmation?.questionId); true },
        clearConfirmation = { clearedConfirmations.add(it) },
        postPurpose = { _, _, activeOnly -> purposeAlerts.add(activeOnly); true },
        // These tests cover the immediate prompt, used when the evening reminder is off.
        batchLightQuestions = { false },
        scheduleFollowUp = { event, delay -> followUps.add(event to delay) },
        now = { clock }
    ) { item, activeOnly -> recordAlerts.add(item to activeOnly); true }

    private val answers get() = PurposeAnswers(
        context, db, schedule = { _, _, _ -> }, refresh = {}, showState = { _, _ -> }, results = results, now = { clock }
    )

    @Before fun open() = runBlocking {
        server = MockWebServer().apply { start() }
        AppPreferences(context).apply {
            TestEndpoint.set(context, server.url("/").toString())
            apiKey = "fictional-refresh-key"
        }
        pairing = AppPreferences(context).pairingContext
        db = Room.inMemoryDatabaseBuilder(context, AppDatabase::class.java).build()
        rowId = db.notificationDao().insertOrIgnore(RawNotificationEntity(
            deviceId = "fictional", packageName = "com.bca.mybca.omni.android", appLabel = "myBCA", postTime = 1,
            capturedAt = capturedAt, title = "Financial Diary", payloadHash = HASH
        ))
    }

    @After fun close() {
        db.close()
        server.shutdown()
    }

    private suspend fun row() = db.notificationDao().getById(rowId)!!
    private fun item(json: String): IngestResultItem = Gson().fromJson(json, IngestResultItem::class.java)
    private fun openResult(question: String = questionJson()) = item(resultJson(question))
    private fun closed() = item(resultJson(question = null, description = "makan kafe senja", reviewStatus = "reviewed"))
    private suspend fun consume(result: IngestResultItem) = assertTrue(results.consume(row(), result, pairing))
    private suspend fun needsAnswer() = db.notificationDao().getNeedsAnswerFlow().first().map { it.id }

    @Test fun recordedQuestionSchedulesAFollowUpCheck() = runBlocking {
        consume(openResult())
        assertEquals(QUESTION, row().purposeQuestionId)
        assertEquals(0, row().checkAttempt)
        assertEquals(clock + TimeUnit.MINUTES.toMillis(2), row().nextCheckAt)
        assertEquals(listOf(EVENT to TimeUnit.MINUTES.toMillis(2)), followUps)
    }

    @Test fun recordWithoutQuestionSchedulesNothing() = runBlocking {
        consume(item(resultJson(question = null)))
        assertNull(row().nextCheckAt)
        assertTrue(followUps.isEmpty())
    }

    @Test fun questionStillOpenTakesTheNextBackoffStep() = runBlocking {
        repeat(8) {
            consume(openResult())
            clock = row().nextCheckAt!!
        }
        val minutes = followUps.map { TimeUnit.MILLISECONDS.toMinutes(it.second) }
        assertEquals(listOf(2L, 10L, 30L, 120L, 360L, 720L, 720L, 720L), minutes)
        assertEquals(7, row().checkAttempt)
        assertEquals(listOf(rowId), needsAnswer())
    }

    @Test fun closedQuestionLeavesTheListAndQuietlyReplacesThePrompt() = runBlocking {
        consume(openResult())
        results.deliverPendingAlerts(pairing)
        assertEquals(listOf(false), purposeAlerts)
        assertEquals(listOf(rowId), needsAnswer())

        clock = row().nextCheckAt!!
        consume(closed())

        assertNull(row().purposeQuestionId)
        assertNull(row().nextCheckAt)
        assertEquals("recorded", row().processingStatus)
        assertFalse(row().resultJson!!.contains("\"question\""))
        assertTrue(needsAnswer().isEmpty())
        // activeOnly: the prompt still shown is replaced without sound; a dismissed one stays gone.
        assertEquals(listOf(true), recordAlerts.map { it.second })
        assertEquals("makan kafe senja", recordAlerts.single().first.description)
        assertEquals(1, followUps.size)
    }

    @Test fun reviewedTransactionClosesAQuestionTheResultStillCarries() = runBlocking {
        consume(openResult())
        consume(item(resultJson(reviewStatus = "reviewed")))
        assertNull(row().purposeQuestionId)
        assertNull(row().purposeQuestionError)
        assertNull(row().nextCheckAt)
        assertFalse(row().resultJson!!.contains("\"question\""))
        assertTrue(needsAnswer().isEmpty())
    }

    @Test fun differentQuestionStartsTheScheduleOver() = runBlocking {
        consume(openResult())
        clock = row().nextCheckAt!!
        consume(openResult())
        assertEquals(1, row().checkAttempt)
        val next = "00000000-0000-4000-8000-000000000105"
        consume(openResult(questionJson(questionId = next)))
        assertEquals(next, row().purposeQuestionId)
        assertEquals(0, row().checkAttempt)
        assertEquals(TimeUnit.MINUTES.toMillis(2), followUps.last().second)
        assertEquals(listOf(rowId), needsAnswer())
    }

    @Test fun checksStopSevenDaysAfterCapture() = runBlocking {
        clock = capturedAt + QuestionRefresh.WINDOW_MILLIS - TimeUnit.MINUTES.toMillis(3)
        consume(openResult())
        assertEquals(capturedAt + QuestionRefresh.WINDOW_MILLIS - TimeUnit.MINUTES.toMillis(1), row().nextCheckAt)

        clock = row().nextCheckAt!!
        consume(openResult())
        assertNull(row().nextCheckAt)
        assertEquals(1, followUps.size)
        assertEquals(QUESTION, row().purposeQuestionId)
        assertEquals(listOf(rowId), needsAnswer())
    }

    @Test fun failedCheckKeepsTheRecordAndItsQuestion() = runBlocking {
        consume(openResult())
        results.deferFollowUp(row(), "server_unavailable", terminal = false)
        assertEquals("recorded", row().processingStatus)
        assertNull(row().processingError)
        assertEquals(QUESTION, row().purposeQuestionId)
        assertEquals(1, row().checkAttempt)
        assertEquals(clock + TimeUnit.MINUTES.toMillis(10), row().nextCheckAt)

        results.deferFollowUp(row(), "auth_rejected", terminal = true)
        assertEquals("recorded", row().processingStatus)
        assertNull(row().processingError)
        assertNull(row().nextCheckAt)
        assertEquals(listOf(rowId), needsAnswer())

        clock = capturedAt + QuestionRefresh.WINDOW_MILLIS
        results.deferFollowUp(row(), "network_unavailable", terminal = false)
        assertNull(row().nextCheckAt)
    }

    @Test fun unsentAnswerToAClosedQuestionResolvesClosedWithoutAPost() = runBlocking {
        consume(openResult())
        assertTrue(answers.enqueue(rowId, QUESTION, "s1", null).queued)
        val replyId = db.senderReplyDao().forQuestion(pairing, EVENT, QUESTION)!!.replyId

        consume(openResult())
        assertEquals("queued", db.senderReplyDao().get(replyId)!!.state)

        consume(closed())
        val reply = db.senderReplyDao().get(replyId)!!
        assertEquals("closed", reply.state)
        assertEquals("question_closed", reply.errorCode)
        assertEquals("Pertanyaan sudah selesai di server; jawaban ini tidak dikirim lagi.",
            purposeReplyMessage(reply.state, reply.errorCode))

        // Were the answer posted, the server would refuse it: the question is closed there.
        server.enqueue(MockResponse().setResponseCode(409).setBody("""{"ok": false, "detail": {"code": "stale_question"}}"""))
        answers.deliver(replyId)
        assertEquals(0, server.requestCount)
        assertEquals("closed", db.senderReplyDao().get(replyId)!!.state)
        assertFalse(answers.enqueue(rowId, QUESTION, "s2", null).queued)
    }

    @Test fun confirmationQuestionIsCheckedOnTheSameSchedule() = runBlocking {
        consume(item(sourcePocketJson()))
        assertEquals("needs_confirmation", row().processingStatus)
        assertEquals(listOf(SOURCE_QUESTION), confirmations)
        assertEquals(0, row().checkAttempt)
        clock = row().nextCheckAt!!
        consume(item(sourcePocketJson()))
        assertEquals(1, row().checkAttempt)
        // The delivered confirmation is not posted again for the same question.
        assertEquals(listOf(SOURCE_QUESTION), confirmations)

        clock = row().nextCheckAt!!
        consume(item(sourcePocketJson(questionId = "00000000-0000-4000-8000-000000000108")))
        assertEquals(0, row().checkAttempt)
        val minutes = followUps.map { TimeUnit.MILLISECONDS.toMinutes(it.second) }
        assertEquals(listOf(2L, 10L, 2L), minutes)
    }

    @Test fun recordedAfterConfirmationClearsTheQuestionAndItsNotification() = runBlocking {
        consume(item(sourcePocketJson()))
        val options = """{"event_id": "$EVENT", "question_id": "$SOURCE_QUESTION", "options": []}"""
        assertEquals(1, db.notificationDao().saveSourcePocketOptions(rowId, pairing, EVENT, options))
        clock = row().nextCheckAt!!

        consume(item(resultJson(question = null, description = "Pindah ke Kantong Harian")))

        assertEquals("recorded", row().processingStatus)
        assertNull(row().nextCheckAt)
        assertNull(row().sourcePocketOptionsJson)
        assertFalse(row().resultJson!!.contains("source_pocket"))
        assertEquals(listOf(EVENT), clearedConfirmations)
        // The recorded alert follows through the pending-alert delivery, as after a phone answer.
        assertTrue(recordAlerts.isEmpty())
        results.deliverPendingAlerts(pairing)
        assertEquals(listOf(false), recordAlerts.map { it.second })
        assertEquals(1, followUps.size)
    }

    @Test fun ignoredAfterConfirmationClearsTheQuestion() = runBlocking {
        consume(item(sourcePocketJson()))
        consume(item(ignoredJson()))
        assertEquals("ignored", row().processingStatus)
        assertNull(row().nextCheckAt)
        assertEquals(listOf(EVENT), clearedConfirmations)
    }

    @Test fun failedCheckKeepsTheConfirmationPending() = runBlocking {
        consume(item(sourcePocketJson()))
        results.deferFollowUp(row(), "auth_rejected", terminal = true)
        assertEquals("needs_confirmation", row().processingStatus)
        assertEquals("source_pocket_required", row().processingError)
        assertNull(row().nextCheckAt)
        assertTrue(row().resultJson!!.contains(SOURCE_QUESTION))
    }

    @Test fun foregroundRefreshPicksOnlyOpenQuestionRows() = runBlocking {
        val dao = db.notificationDao()
        consume(openResult())
        fun other(n: Int) = RawNotificationEntity(
            deviceId = "fictional", packageName = "com.bca", appLabel = "BCA", postTime = 10L + n, payloadHash = "fictional-$n"
        )
        val eventOf = { n: Int -> "00000000-0000-4000-8000-00000000030$n" }
        suspend fun store(n: Int, status: String, question: String?, pairingContext: String = pairing) {
            val id = dao.insertOrIgnore(other(n))
            dao.saveCompletion(
                id = id, accepted = true, syncedAt = 1, eventId = eventOf(n), status = status, errorCode = null,
                recordKey = "notification:${eventOf(n)}", transactionId = null, pairingContext = pairingContext,
                nextCheckAt = null, checkAttempt = 0, resultJson = null, purposeQuestionId = question
            )
        }
        store(1, "recorded", null)
        store(2, "queued", null)
        store(3, "needs_review", null)
        store(4, "recorded", "00000000-0000-4000-8000-000000000401", pairingContext = "00000000-0000-4000-8000-000000000998")
        store(5, "recorded", "00000000-0000-4000-8000-000000000501")
        store(6, "needs_confirmation", null)
        answers.enqueue(rowId, QUESTION, "s1", null)

        val refreshed = mutableListOf<String>()
        assertEquals(3, QuestionRefresh.refreshOpen(context, dao) { refreshed.add(it) })
        assertEquals(setOf(EVENT, eventOf(5), eventOf(6)), refreshed.toSet())

        AppPreferences(context).apiKey = ""
        refreshed.clear()
        assertEquals(0, QuestionRefresh.refreshOpen(context, dao) { refreshed.add(it) })
        assertTrue(refreshed.isEmpty())
    }
}
