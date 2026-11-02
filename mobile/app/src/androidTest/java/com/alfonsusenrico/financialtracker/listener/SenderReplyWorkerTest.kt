package com.alfonsusenrico.financialtracker.listener

import androidx.test.platform.app.InstrumentationRegistry
import androidx.work.Data
import androidx.work.WorkManager
import androidx.work.testing.TestListenableWorkerBuilder
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.model.*
import com.alfonsusenrico.financialtracker.listener.data.network.model.*
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.data.sync.*
import com.google.gson.Gson
import kotlinx.coroutines.runBlocking
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import java.util.UUID
import java.util.concurrent.TimeUnit

@RunWith(Parameterized::class)
class SenderReplyWorkerTest(private val answerKind: String) {
    companion object {
        @JvmStatic @Parameterized.Parameters(name = "{0}")
        fun kinds() = listOf(arrayOf("sender"), arrayOf("source_pocket"))
    }
    private val context = InstrumentationRegistry.getInstrumentation().targetContext
    private val db = AppDatabase.getDatabase(context)
    private val prefs = AppPreferences(context)
    private val server = MockWebServer()
    private val rows = mutableListOf<Long>()
    private fun uuid() = UUID.randomUUID().toString()

    @Before fun setup() {
        check(context.packageName.endsWith(".verification"))
        server.start()
        TestEndpoint.set(context, server.url("/").toString())
        prefs.apiKey = "fictional-sender-key"
        prefs.isAutoSyncEnabled = false
    }

    @After fun cleanup() = runBlocking {
        WorkManager.getInstance(context).cancelAllWork().result.get(10, TimeUnit.SECONDS)
        rows.forEach { db.notificationDao().deleteById(it) }
        prefs.apiKey = ""
        server.shutdown()
    }

    private suspend fun reply(action: String = "name"): SenderReplyEntity {
        val event = uuid()
        val question = uuid()
        val source = uuid()
        val pair = prefs.pairingContext
        val id = db.notificationDao().insertOrIgnore(RawNotificationEntity(deviceId = "fictional", packageName = "com.bca", appLabel = "BCA", postTime = 1000,
            title = "Fictional transfer", payloadHash = event, backendEventId = event, processingStatus = "needs_confirmation",
            isSynced = true, pairingContext = pair, resultJson = Gson().toJson(IngestResultItem(payloadHash = event, eventId = event,
                status = "needs_confirmation", confirmation = if (answerKind == "sender") {
                    ConfirmationQuestion("sender", question, "AN**RA**", "bca", AccountChoice(uuid(), "BCA"), 112590, "IDR")
                } else {
                    ConfirmationQuestion("source_pocket", question, institution = "jago", amount = 112590, currency = "IDR", jagoAccount = AccountChoice(uuid(), "Bank Jago"))
                }))))
        rows.add(id)
        val reply = SenderReplyEntity(uuid(), id, pair, event, question,
            if (answerKind == "sender") action else "select",
            if (answerKind == "sender" && action == "name") "Andra" else null,
            answerKind = answerKind, accountId = source.takeIf { answerKind == "source_pocket" })
        db.senderReplyDao().insert(reply)
        return reply
    }

    private suspend fun run(reply: SenderReplyEntity) = TestListenableWorkerBuilder<SenderReplyWorker>(context,
        inputData = Data.Builder().putString(SenderReplyWorker.REPLY_ID, reply.replyId).build()).build().doWork()

    private fun accepted(reply: SenderReplyEntity, status: String = "needs_review") = MockResponse().setBody(
        Gson().toJson(NotificationResultResponse(true, IngestResultItem(payloadHash = reply.eventId, eventId = reply.eventId,
            status = status, errorCode = "uncertain_mapping"))))

    @Test fun transientLostAcknowledgementReplaysSameAnswerWithoutReingestion() = runBlocking {
        val reply = reply()
        server.enqueue(MockResponse().setResponseCode(503).setBody("fictional-private-body"))
        run(reply)
        val first = server.takeRequest(5, TimeUnit.SECONDS)!!
        val stored = db.senderReplyDao().get(reply.replyId)!!
        assertEquals("queued", stored.state)
        assertEquals("server_unavailable", stored.errorCode)
        assertTrue(stored.nextAttemptAt > System.currentTimeMillis())
        server.enqueue(accepted(reply))
        run(reply)
        val replay = server.takeRequest(5, TimeUnit.SECONDS)!!
        assertEquals("/api/ingest/notifications/${reply.eventId}/confirm-${answerKind.replace('_', '-')}", first.path)
        assertEquals(first.path, replay.path)
        assertEquals(first.body.readUtf8(), replay.body.readUtf8())
        assertEquals("Bearer fictional-sender-key", replay.getHeader("Authorization"))
        assertEquals("accepted", db.senderReplyDao().get(reply.replyId)!!.state)
        assertNull(db.senderReplyDao().get(reply.replyId)!!.name)
        assertEquals("needs_review", db.notificationDao().getById(reply.rowId)!!.processingStatus)
        assertTrue(db.notificationDao().getUnsyncedNotifications(reply.pairingContext).isEmpty())
        run(reply)
        assertNull(server.takeRequest(100, TimeUnit.MILLISECONDS))
    }

    @Test fun permanentErrorsStopAutomaticDeliveryAndAuthWaitsForRecovery() = runBlocking {
        for ((code, state) in listOf(401 to "waiting_auth", 403 to "waiting_auth", 409 to "stale", 422 to "invalid")) {
            val reply = reply("unknown")
            server.enqueue(MockResponse().setResponseCode(code))
            run(reply)
            server.takeRequest(5, TimeUnit.SECONDS)!!
            assertEquals(state, db.senderReplyDao().get(reply.replyId)!!.state)
            assertEquals("needs_confirmation", db.notificationDao().getById(reply.rowId)!!.processingStatus)
            if (state != "waiting_auth") {
                // Cancel the explicit conflict refresh before checking that the answer itself stays stopped.
                WorkManager.getInstance(context).cancelAllWork().result.get(10, TimeUnit.SECONDS)
                run(reply)
                server.takeRequest(100, TimeUnit.MILLISECONDS)?.let { assertTrue(it.path!!.endsWith("/result")) }
            }
        }
    }

    @Test fun rateLimitedAnswerRetainsIdentityForRetry() = runBlocking {
        val reply = reply()
        server.enqueue(MockResponse().setResponseCode(429))
        run(reply)
        server.takeRequest(5, TimeUnit.SECONDS)!!
        val stored = db.senderReplyDao().get(reply.replyId)!!
        assertEquals("queued", stored.state)
        assertEquals("server_unavailable", stored.errorCode)
        assertEquals(reply.accountId, stored.accountId)
        assertEquals(reply.questionId, stored.questionId)
        assertTrue(stored.nextAttemptAt > System.currentTimeMillis())
    }

    @Test fun foreignPairingCannotSendReplyOrConsumeAnotherEvent() = runBlocking {
        val reply = reply()
        prefs.apiKey = "fictional-different-key"
        run(reply)
        assertEquals("waiting_pairing", db.senderReplyDao().get(reply.replyId)!!.state)
        assertNull(server.takeRequest(100, TimeUnit.MILLISECONDS))
        prefs.apiKey = "fictional-sender-key"
        assertEquals(reply.pairingContext, prefs.pairingContext)
        assertTrue(db.senderReplyDao().recoverable(prefs.pairingContext).any { it.replyId == reply.replyId })
        prefs.apiKey = "fictional-different-key"
        val current = reply()
        server.enqueue(MockResponse().setBody(Gson().toJson(NotificationResultResponse(true,
            IngestResultItem(payloadHash = uuid(), eventId = uuid(), status = "recorded")))))
        run(current)
        server.takeRequest(5, TimeUnit.SECONDS)
        assertEquals("invalid_result", db.senderReplyDao().get(current.replyId)!!.errorCode)
        assertEquals("needs_confirmation", db.notificationDao().getById(current.rowId)!!.processingStatus)
    }
}
