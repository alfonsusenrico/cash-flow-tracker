package com.alfonsusenrico.financialtracker.listener

import android.app.Application
import android.content.Context
import androidx.room.Room
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import com.alfonsusenrico.financialtracker.listener.PurposeFixtures.EVENT
import com.alfonsusenrico.financialtracker.listener.PurposeFixtures.HASH
import com.alfonsusenrico.financialtracker.listener.PurposeFixtures.QUESTION
import com.alfonsusenrico.financialtracker.listener.PurposeFixtures.questionJson
import com.alfonsusenrico.financialtracker.listener.PurposeFixtures.result
import com.alfonsusenrico.financialtracker.listener.PurposeFixtures.resultJson
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.model.RawNotificationEntity
import com.alfonsusenrico.financialtracker.listener.data.model.SenderReplyEntity
import com.alfonsusenrico.financialtracker.listener.data.network.model.IngestResultItem
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.data.sync.NotificationResults
import com.alfonsusenrico.financialtracker.listener.data.sync.PurposeAnswers
import com.alfonsusenrico.financialtracker.listener.data.sync.senderRetryDelay
import com.google.gson.Gson
import com.google.gson.JsonParser
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.SocketPolicy
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.annotation.Config

/** The answer outbox end to end: Room, the real API client and a fictional backend. */
@RunWith(AndroidJUnit4::class)
@Config(sdk = [34], application = TestApplication::class)
class PurposeAnswersTest {
    private val context = ApplicationProvider.getApplicationContext<Context>()
    private lateinit var db: AppDatabase
    private lateinit var server: MockWebServer
    private lateinit var pairing: String
    private val scheduled = mutableListOf<Triple<String, Long, Boolean>>()
    private val refreshed = mutableListOf<String>()
    private val states = mutableListOf<String>()
    private val purposeAlerts = mutableListOf<Boolean>()
    private val recordAlerts = mutableListOf<Pair<IngestResultItem, Boolean>>()
    private var rowId = 0L

    private val results get() = NotificationResults(
        context, db, postConfirmation = { _, _ -> true }, clearConfirmation = {},
        postPurpose = { _, _, activeOnly -> purposeAlerts.add(activeOnly); true },
        scheduleFollowUp = { _, _ -> }
    ) { item, activeOnly -> recordAlerts.add(item to activeOnly); true }

    private val answers get() = PurposeAnswers(
        context, db,
        schedule = { id, delay, continuation -> scheduled.add(Triple(id, delay, continuation)) },
        refresh = { refreshed.add(it) },
        showState = { _, message -> states.add(message) },
        results = results,
        now = { 1_000L }
    )

    @Before fun open() = runBlocking {
        server = MockWebServer().apply { start() }
        AppPreferences(context).apply {
            TestEndpoint.set(context, server.url("/").toString())
            apiKey = "fictional-purpose-key"
        }
        pairing = AppPreferences(context).pairingContext
        db = Room.inMemoryDatabaseBuilder(context, AppDatabase::class.java).build()
        rowId = db.notificationDao().insertOrIgnore(RawNotificationEntity(
            deviceId = "fictional", packageName = "com.bca.mybca.omni.android", appLabel = "myBCA", postTime = 1,
            title = "Financial Diary", payloadHash = HASH
        ))
        assertTrue(results.consume(db.notificationDao().getById(rowId)!!, result(), pairing))
        results.deliverPendingAlerts(pairing)
    }

    @After fun close() {
        db.close()
        server.shutdown()
    }

    private suspend fun row() = db.notificationDao().getById(rowId)!!
    private suspend fun reply(): SenderReplyEntity = db.senderReplyDao().forQuestion(pairing, EVENT, QUESTION)!!
    private fun answered(description: String = "makan kafe senja 1") =
        MockResponse().setBody("""{"ok": true, "result": ${resultJson(question = null, description = description, reviewStatus = "reviewed")},
            "answer": {"status": "applied", "transaction_id": "${PurposeFixtures.TRANSACTION}", "review_status": "reviewed"}}""")
    private fun error(code: Int, detail: String) = MockResponse().setResponseCode(code).setBody("""{"ok": false, "detail": {"code": "$detail"}}""")

    @Test fun recordedQuestionIsStoredAndAlertsAsThePrompt() = runBlocking {
        assertEquals(QUESTION, row().purposeQuestionId)
        assertNull(row().purposeQuestionError)
        assertEquals(listOf(false), purposeAlerts)
        assertTrue(recordAlerts.isEmpty())
        assertEquals(listOf(rowId), db.notificationDao().getNeedsAnswerFlow().first().map { it.id })
    }

    @Test fun malformedQuestionIsDroppedWithItsReasonAndAlertsAsARecord() = runBlocking {
        val other = db.notificationDao().insertOrIgnore(RawNotificationEntity(
            deviceId = "fictional", packageName = "com.bca", appLabel = "BCA", postTime = 2, payloadHash = "fictional-other"
        ))
        val malformed = Gson().fromJson(resultJson(questionJson(questionId = "bad")), IngestResultItem::class.java)
            .copy(payloadHash = "fictional-other", eventId = "00000000-0000-4000-8000-000000000201", recordKey = "notification:other")
        assertTrue(results.consume(db.notificationDao().getById(other)!!, malformed, pairing))
        results.deliverPendingAlerts(pairing)
        val stored = db.notificationDao().getById(other)!!
        assertNull(stored.purposeQuestionId)
        assertEquals("purpose_question_invalid", stored.purposeQuestionError)
        assertFalse(stored.resultJson!!.contains("\"question\""))
        assertEquals(listOf(false), recordAlerts.map { it.second })
    }

    @Test fun answerIsStoredBeforeAnyNetworkCallAndDeliveredOnce() = runBlocking {
        val outcome = answers.enqueue(rowId, QUESTION, "s1", null)
        assertTrue(outcome.queued)
        assertEquals(0, server.requestCount)
        val saved = reply()
        assertEquals("queued", saved.state)
        assertEquals("purpose", saved.answerKind)
        assertEquals("s1", saved.suggestionId)
        assertEquals(listOf(Triple(saved.replyId, 0L, false)), scheduled)
        assertEquals(listOf("Jawaban tersimpan; mengirim ke server."), states)
        assertTrue(db.notificationDao().getNeedsAnswerFlow().first().isEmpty())

        assertTrue(answers.enqueue(rowId, QUESTION, "s1", null).queued)
        assertEquals(saved.replyId, reply().replyId)
        assertFalse(answers.enqueue(rowId, QUESTION, "s2", null).queued)
        assertEquals("s1", reply().suggestionId)
    }

    @Test fun successSendsTheVersionAndConsumesTheAnsweredResult() = runBlocking {
        answers.enqueue(rowId, QUESTION, null, "  sewa lapangan futsal ")
        server.enqueue(answered("sewa lapangan futsal"))
        answers.deliver(reply().replyId)

        val request = server.takeRequest()
        assertEquals("/api/ingest/notifications/$EVENT/answer-purpose", request.path)
        assertEquals(BuildConfig.VERSION_NAME, request.getHeader("X-Companion-Version"))
        val body = JsonParser.parseString(request.body.readUtf8()).asJsonObject
        assertEquals(setOf("question_id", "reply_id", "text"), body.keySet())
        assertEquals(QUESTION, body["question_id"].asString)
        assertEquals(reply().replyId, body["reply_id"].asString)
        assertEquals("sewa lapangan futsal", body["text"].asString)

        assertEquals("accepted", reply().state)
        assertNull(reply().answerText)
        assertNull(row().purposeQuestionId)
        assertTrue(row().resultJson!!.contains("sewa lapangan futsal"))
        assertEquals(listOf(true), recordAlerts.map { it.second })
        assertEquals("sewa lapangan futsal", recordAlerts.single().first.description)
        assertTrue(refreshed.isEmpty())
    }

    @Test fun networkLossRetriesWithBackoffAndReplaysTheSameReplyId() = runBlocking {
        answers.enqueue(rowId, QUESTION, "s2", null)
        val replyId = reply().replyId
        server.enqueue(MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_AFTER_REQUEST))
        answers.deliver(replyId)
        assertEquals("queued", reply().state)
        assertEquals(1, reply().attempt)
        assertEquals("network_unavailable", reply().errorCode)
        assertEquals(1_000L + senderRetryDelay(0), reply().nextAttemptAt)
        assertEquals(Triple(replyId, senderRetryDelay(0), true), scheduled.last())

        server.enqueue(MockResponse().setResponseCode(503))
        answers.deliver(replyId)
        assertEquals(2, reply().attempt)
        assertEquals("server_unavailable", reply().errorCode)
        assertEquals(Triple(replyId, senderRetryDelay(1), true), scheduled.last())

        server.enqueue(answered())
        answers.deliver(replyId)
        val sent = List(3) { JsonParser.parseString(server.takeRequest().body.readUtf8()).asJsonObject["reply_id"].asString }
        assertEquals(listOf(replyId, replyId, replyId), sent)
        assertEquals("accepted", reply().state)

        answers.deliver(replyId)
        assertEquals(3, server.requestCount)
    }

    @Test fun conflictClosesTheQuestionAndRefreshesTheResult() = runBlocking {
        for (code in listOf("stale_question", "purpose_answer_conflict")) {
            db.openHelper.writableDatabase.execSQL("DELETE FROM sender_replies")
            answers.enqueue(rowId, QUESTION, "s1", null)
            server.enqueue(error(409, code))
            answers.deliver(reply().replyId)
            assertEquals("closed", reply().state)
            assertEquals(code, reply().errorCode)
            assertEquals("Pertanyaan sudah ditutup di server; jawaban ini tidak dipakai.", states.last())
            assertEquals(EVENT, refreshed.last())
            assertTrue(db.notificationDao().getNeedsAnswerFlow().first().isEmpty())
            assertFalse(answers.enqueue(rowId, QUESTION, "s2", null).queued)
        }
    }

    @Test fun rejectionFailsVisiblyAndAllowsANewAnswer() = runBlocking {
        answers.enqueue(rowId, QUESTION, "s1", null)
        val first = reply().replyId
        server.enqueue(error(422, "unknown_suggestion"))
        answers.deliver(first)
        assertEquals("failed", reply().state)
        assertEquals("unknown_suggestion", reply().errorCode)
        assertEquals("Saran ini tidak berlaku lagi. Pilih saran lain atau tulis jawaban.", states.last())
        assertEquals(listOf(rowId), db.notificationDao().getNeedsAnswerFlow().first().map { it.id })
        assertTrue(refreshed.isEmpty())

        assertTrue(answers.enqueue(rowId, QUESTION, null, "sewa lapangan").queued)
        assertNotEquals(first, reply().replyId)
        server.enqueue(MockResponse().setResponseCode(404).setBody("""{"ok": false, "detail": "Notification event not found"}"""))
        answers.deliver(reply().replyId)
        assertEquals("failed", reply().state)
        assertEquals("event_not_found", reply().errorCode)
    }

    @Test fun rejectedCredentialsWaitWithoutRetrying() = runBlocking {
        answers.enqueue(rowId, QUESTION, "s1", null)
        server.enqueue(MockResponse().setResponseCode(401))
        answers.deliver(reply().replyId)
        assertEquals("waiting_auth", reply().state)
        assertEquals(1, scheduled.size)
    }

    @Test fun invalidAnswersAreRefusedBeforeTheyAreStored() = runBlocking {
        assertFalse(answers.enqueue(rowId, QUESTION, null, " ").queued)
        assertFalse(answers.enqueue(rowId, QUESTION, null, "x".repeat(201)).queued)
        assertFalse(answers.enqueue(rowId, QUESTION, "s9", null).queued)
        assertFalse(answers.enqueue(rowId, QUESTION, "s1", "both").queued)
        assertFalse(answers.enqueue(rowId, "00000000-0000-4000-8000-000000000999", "s1", null).queued)
        assertFalse(answers.enqueue(rowId, QUESTION, "s1", null, expectedPairing = "00000000-0000-4000-8000-000000000998").queued)
        assertNull(db.senderReplyDao().forQuestion(pairing, EVENT, QUESTION))
        assertTrue(scheduled.isEmpty())
    }

    @Test fun refreshOfAnAnsweredQuestionLeavesTheAnswerStateShown() = runBlocking {
        answers.enqueue(rowId, QUESTION, "s1", null)
        db.notificationDao().markAlertDelivered(pairing, "notification:$EVENT")
        purposeAlerts.clear()
        assertTrue(results.consume(row(), result(), pairing))
        assertTrue(purposeAlerts.isEmpty())
        assertTrue(recordAlerts.isEmpty())
        assertEquals(QUESTION, row().purposeQuestionId)
    }
}
