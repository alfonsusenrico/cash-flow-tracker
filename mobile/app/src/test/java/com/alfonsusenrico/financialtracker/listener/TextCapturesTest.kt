package com.alfonsusenrico.financialtracker.listener

import android.content.Context
import androidx.room.Room
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.model.TextCaptureState
import com.alfonsusenrico.financialtracker.listener.data.network.model.TransactionUpdateRequest
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.data.textcapture.CaptureActionResult
import com.alfonsusenrico.financialtracker.listener.data.textcapture.CaptureAnswer
import com.alfonsusenrico.financialtracker.listener.data.textcapture.TextCaptures
import com.google.gson.JsonParser
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import okhttp3.mockwebserver.SocketPolicy
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.annotation.Config
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.Collections
import java.util.concurrent.atomic.AtomicInteger

/** The capture queue end to end: Room, the real API client and a fictional tracker with synthetic captures. */
@RunWith(AndroidJUnit4::class)
@Config(sdk = [34], application = TestApplication::class)
class TextCapturesTest {
    private val context = ApplicationProvider.getApplicationContext<Context>()
    private val prefs get() = AppPreferences(context)
    private lateinit var db: AppDatabase
    private lateinit var server: MockWebServer
    private var now = 1_800_000_000_000L
    private val scheduled = AtomicInteger()
    private val ids = AtomicInteger()
    private val requests = Collections.synchronizedList(mutableListOf<RecordedRequest>())
    @Volatile private var respond: (RecordedRequest) -> MockResponse = { MockResponse().setResponseCode(404) }

    private val captures get() = TextCaptures(
        context, db, clock = { now }, zone = ZoneId.of("Asia/Jakarta"),
        scheduleResend = { scheduled.incrementAndGet() },
        newId = { "00000000-0000-4000-8000-%012d".format(ids.incrementAndGet()) }
    )
    private val dao get() = db.textCaptureDao()

    @Before fun open() {
        db = Room.inMemoryDatabaseBuilder(context, AppDatabase::class.java).build()
        server = MockWebServer()
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse {
                requests += request
                return respond(request)
            }
        }
        server.start()
        TestEndpoint.set(context, server.url("/").toString())
        prefs.apiKey = "fictional-capture-key"
        prefs.isAutoSyncEnabled = true
    }

    @After fun close() {
        db.close()
        server.shutdown()
        prefs.apiKey = ""
        prefs.isAutoSyncEnabled = true
    }

    private fun capturePosts() = requests.filter { it.path == "/api/capture/text" }

    @Test fun captureIsStoredBeforeAnyRequestWithItsDeviceTime() = runBlocking {
        respond = { MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_AFTER_REQUEST) }
        val id = captures.create("  bayar\nparkir   ptc 10rb ")!!
        val stored = dao.getById(id)!!
        assertEquals("bayar parkir ptc 10rb", stored.text)
        assertEquals(TextCaptureState.QUEUED, stored.state)
        assertEquals("2027-01-15T15:00:00+07:00", stored.deviceTime)
        assertTrue(requests.isEmpty())
        assertNull(captures.create("   "))
        assertNull(captures.create("x".repeat(201)))
    }

    @Test fun confidentCaptureIsRecordedWithItsUndoDeadline() = runBlocking {
        respond = { recorded(it, clientId = clientIdOf(it), undoInSeconds = 10) }
        val id = captures.create("bayar parkir ptc 10rb")!!
        val sent = captures.send(id, ownerAsked = false)!!
        assertEquals(TextCaptureState.RECORDED, sent.state)
        assertEquals(SERVER_ID, sent.serverId)
        assertEquals(TRANSACTION_ID, sent.transactionId)
        assertEquals(prefs.pairingContext, sent.pairingContext)
        assertEquals(now + 10_000, sent.undoDeadline)
        val body = JsonParser.parseString(capturePosts().single().body.readUtf8()).asJsonObject
        assertEquals(sent.clientCaptureId, body["client_capture_id"].asString)
        assertEquals("bayar parkir ptc 10rb", body["text"].asString)
        assertEquals("Bearer fictional-capture-key", capturePosts().single().getHeader("Authorization"))
    }

    @Test fun unsureCaptureStoresItsQuestionAndAnswerIsIdempotent() = runBlocking {
        val answerBodies = mutableListOf<String>()
        var answerFails = true
        respond = { request ->
            when (request.path) {
                "/api/capture/text" -> asking(clientIdOf(request))
                "/api/capture/$SERVER_ID/answer" -> {
                    answerBodies += request.body.readUtf8()
                    if (answerFails) MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_AFTER_REQUEST)
                    else recorded(request, clientId = CLIENT_OF_ASKING, undoInSeconds = 10)
                }
                else -> MockResponse().setResponseCode(404)
            }
        }
        val id = captures.create("beli pulsa 50rb")!!
        assertEquals(TextCaptureState.NEEDS_DETAIL, captures.send(id, ownerAsked = false)!!.state)
        assertEquals("account", TextCaptures.question(dao.getById(id)!!)!!.type)

        val answer = CaptureAnswer(QUESTION_ID, optionId = ACCOUNT_ID)
        assertEquals(CaptureActionResult.OFFLINE, captures.answer(id, answer))
        assertNotNull(dao.getById(id)!!.pendingAnswerJson)
        // A second tap and an explicit resend reuse the stored reply id.
        assertEquals(CaptureActionResult.OFFLINE, captures.answer(id, answer))
        answerFails = false
        assertEquals(CaptureActionResult.DONE, captures.resendAnswer(id))
        val replyIds = answerBodies.map { JsonParser.parseString(it).asJsonObject["reply_id"].asString }.toSet()
        assertEquals(1, replyIds.size)
        assertEquals(3, answerBodies.size)
        val done = dao.getById(id)!!
        assertEquals(TextCaptureState.RECORDED, done.state)
        assertNull(done.pendingAnswerJson)
        assertNull(done.questionJson)
    }

    @Test fun answerOutsideTheQuestionIsRefusedLocallyAndTrackerRefusalClearsIt() = runBlocking {
        respond = { request ->
            when (request.path) {
                "/api/capture/text" -> asking(clientIdOf(request))
                "/api/capture/$SERVER_ID/answer" -> error(422, "invalid_capture_answer")
                else -> MockResponse().setResponseCode(404)
            }
        }
        val id = captures.create("beli pulsa 50rb")!!
        captures.send(id, ownerAsked = false)
        assertEquals(CaptureActionResult.INVALID, captures.answer(id, CaptureAnswer(QUESTION_ID, optionId = "not-offered")))
        assertEquals(CaptureActionResult.INVALID, captures.answer(id, CaptureAnswer(QUESTION_ID, text = "50rb")))
        assertEquals(CaptureActionResult.STALE, captures.answer(id, CaptureAnswer("00000000-0000-4000-8000-000000000999", optionId = ACCOUNT_ID)))
        assertTrue(requests.none { it.path!!.endsWith("/answer") })
        assertEquals(CaptureActionResult.INVALID, captures.answer(id, CaptureAnswer(QUESTION_ID, accountId = OTHER_ACCOUNT_ID)))
        assertNull(dao.getById(id)!!.pendingAnswerJson)
        assertEquals(TextCaptureState.NEEDS_DETAIL, dao.getById(id)!!.state)
    }

    @Test fun staleQuestionReloadsTheCapture() = runBlocking {
        var reads = 0
        respond = { request ->
            when (request.path) {
                "/api/capture/text" -> asking(clientIdOf(request))
                "/api/capture/$SERVER_ID/answer" -> error(409, "stale_capture_question")
                "/api/capture/$SERVER_ID" -> {
                    reads++
                    asking(CLIENT_OF_ASKING, questionId = "00000000-0000-4000-8000-000000000777")
                }
                else -> MockResponse().setResponseCode(404)
            }
        }
        val id = captures.create("beli pulsa 50rb")!!
        captures.send(id, ownerAsked = false)
        assertEquals(CaptureActionResult.STALE, captures.answer(id, CaptureAnswer(QUESTION_ID, optionId = ACCOUNT_ID)))
        assertEquals(1, reads)
        assertEquals("00000000-0000-4000-8000-000000000777", TextCaptures.question(dao.getById(id)!!)!!.questionId)
    }

    @Test fun offlineSendIsQueuedAndResentOnceWithTheSameClientId() = runBlocking {
        respond = { MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_AFTER_REQUEST) }
        val id = captures.create("kopi 25rb")!!
        val queued = captures.send(id, ownerAsked = false)!!
        assertEquals(TextCaptureState.QUEUED, queued.state)
        assertEquals(TextCaptures.NETWORK, queued.errorCode)
        assertEquals(1, scheduled.get())

        val recordedFor = mutableSetOf<String>()
        respond = { request ->
            val client = clientIdOf(request)
            recordedFor += client
            recorded(request, clientId = client, undoInSeconds = 10)
        }
        assertFalse(captures.sendQueued())
        assertFalse(captures.sendQueued())
        assertEquals(TextCaptureState.RECORDED, dao.getById(id)!!.state)
        assertEquals(setOf(queued.clientCaptureId), recordedFor)
        // The lost first attempt and one resend; once accepted, a capture is never sent again.
        assertEquals(listOf(queued.clientCaptureId, queued.clientCaptureId), capturePosts().map(::clientIdOf))
    }

    @Test fun autoSyncOffLeavesAnOfflineCaptureForKirim() = runBlocking {
        prefs.isAutoSyncEnabled = false
        respond = { MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_AFTER_REQUEST) }
        val id = captures.create("kopi 25rb")!!
        captures.send(id, ownerAsked = false)
        assertEquals(0, scheduled.get())
        captures.retry(id)
        assertEquals(1, scheduled.get())
    }

    @Test fun concurrentSendsMakeOneRequest() = runBlocking {
        respond = { recorded(it, clientId = clientIdOf(it), undoInSeconds = 10) }
        val id = captures.create("kopi 25rb")!!
        listOf(async { captures.send(id, false) }, async { captures.send(id, false) }, async { captures.sendQueued() }).awaitAll()
        assertEquals(1, capturePosts().size)
        assertEquals(TextCaptureState.RECORDED, dao.getById(id)!!.state)
    }

    @Test fun processDeathMidSendIsRecoveredWithoutLossOrDuplicate() = runBlocking {
        val id = captures.create("tarik tunai 200rb")!!
        // The process died after claiming the send: the row is left "sending".
        assertEquals(1, dao.claim(id, now, now - TextCaptures.STALE_CLAIM_MILLIS))
        val client = dao.getById(id)!!.clientCaptureId
        // Within the claim's lifetime no second sender takes it.
        assertNull(dao.sendable(now - TextCaptures.STALE_CLAIM_MILLIS).firstOrNull())
        captures.recover()
        assertEquals(TextCaptureState.QUEUED, dao.getById(id)!!.state)
        assertEquals(1, scheduled.get())
        // The tracker had recorded the first attempt; the resend returns the same capture.
        respond = { recorded(it, clientId = clientIdOf(it), undoInSeconds = 2) }
        captures.sendQueued()
        val recovered = dao.getById(id)!!
        assertEquals(TextCaptureState.RECORDED, recovered.state)
        assertEquals(listOf(client), capturePosts().map(::clientIdOf))
        assertEquals(1, db.openHelper.readableDatabase.query("SELECT COUNT(*) FROM text_captures").use { it.moveToFirst(); it.getInt(0) })
    }

    @Test fun staleClaimIsTakenOverAfterTheSendTimeout() = runBlocking {
        val id = captures.create("kopi 25rb")!!
        dao.claim(id, now, now - TextCaptures.STALE_CLAIM_MILLIS)
        respond = { recorded(it, clientId = clientIdOf(it), undoInSeconds = 10) }
        now += TextCaptures.STALE_CLAIM_MILLIS + 1
        captures.sendQueued()
        assertEquals(TextCaptureState.RECORDED, dao.getById(id)!!.state)
    }

    @Test fun serverErrorsAndRefusalsAreKeptApart() = runBlocking {
        respond = { MockResponse().setResponseCode(503) }
        val transient = captures.create("kopi 25rb")!!
        assertEquals(TextCaptures.SERVER, captures.send(transient, false)!!.errorCode)
        assertTrue(captures.sendQueued())

        respond = { error(422, "invalid_capture_date") }
        val refused = captures.create("besok makan 20rb")!!
        val failed = captures.send(refused, false)!!
        assertEquals(TextCaptureState.FAILED, failed.state)
        assertEquals("invalid_capture_date", failed.errorCode)
        val before = capturePosts().size
        captures.sendQueued()
        // Only the transient capture is resent; the refused one waits for the owner.
        assertEquals(before + 1, capturePosts().size)

        respond = { MockResponse().setResponseCode(401) }
        val auth = captures.create("kopi 26rb")!!
        assertEquals(TextCaptures.AUTH, captures.send(auth, false)!!.errorCode)
        assertEquals(TextCaptureState.QUEUED, dao.getById(auth)!!.state)
    }

    @Test fun withoutAPairedTrackerNothingIsSent() = runBlocking {
        prefs.apiKey = ""
        val id = captures.create("kopi 25rb")!!
        val waiting = captures.send(id, false)!!
        assertEquals(TextCaptureState.QUEUED, waiting.state)
        assertEquals(TextCaptures.NOT_PAIRED, waiting.errorCode)
        assertTrue(requests.isEmpty())
    }

    @Test fun undoAtNinePointNineSecondsIsSentAndAtTenPointOneIsNot() = runBlocking {
        respond = { request ->
            when (request.path) {
                "/api/capture/text" -> recorded(request, clientId = clientIdOf(request), undoInSeconds = 10)
                "/api/capture/$SERVER_ID/undo" -> envelope(captureJson("undone", CLIENT_OF_RECORDED, undoInSeconds = 10))
                else -> MockResponse().setResponseCode(404)
            }
        }
        val late = captures.create("kopi 25rb")!!
        captures.send(late, false)
        val start = now
        now = start + 10_100
        assertEquals(CaptureActionResult.TOO_LATE, captures.undo(late))
        assertTrue(requests.none { it.path!!.endsWith("/undo") })

        withContext(Dispatchers.IO) { db.clearAllTables() }
        ids.set(0)
        now = start
        val early = captures.create("kopi 25rb")!!
        captures.send(early, false)
        now = start + 9_900
        assertEquals(CaptureActionResult.DONE, captures.undo(early))
        assertEquals(TextCaptureState.UNDONE, dao.getById(early)!!.state)
        assertEquals(1, requests.count { it.path!!.endsWith("/undo") })
    }

    @Test fun serverRefusingAnUndoClosesTheWindow() = runBlocking {
        var code = "undo_expired"
        respond = { request ->
            when (request.path) {
                "/api/capture/text" -> recorded(request, clientId = clientIdOf(request), undoInSeconds = 10)
                "/api/capture/$SERVER_ID/undo" -> error(409, code)
                else -> MockResponse().setResponseCode(404)
            }
        }
        val id = captures.create("kopi 25rb")!!
        captures.send(id, false)
        now += 3_000
        assertEquals(CaptureActionResult.TOO_LATE, captures.undo(id))
        assertEquals(0L, dao.getById(id)!!.undoDeadline)
        assertEquals(TextCaptureState.RECORDED, dao.getById(id)!!.state)

        code = "capture_transaction_changed"
        dao.update(dao.getById(id)!!.copy(undoDeadline = now + 5_000))
        assertEquals(CaptureActionResult.CHANGED, captures.undo(id))
    }

    @Test fun undoDeadlineFollowsTheServerClock() = runBlocking {
        // The tracker answered a resend 4 s after it recorded: only 6 s of the window are left.
        respond = { recorded(it, clientId = clientIdOf(it), undoInSeconds = 6) }
        val id = captures.create("kopi 25rb")!!
        assertEquals(now + 6_000, captures.send(id, false)!!.undoDeadline)
    }

    @Test fun deleteFromTrackerOnlyForTheCapturesOwnExpense() = runBlocking {
        var deleteCode = 200
        respond = { request ->
            when {
                request.path == "/api/capture/text" -> recorded(request, clientId = clientIdOf(request), undoInSeconds = 10)
                request.method == "DELETE" && request.path == "/api/transactions/$TRANSACTION_ID" ->
                    if (deleteCode == 200) MockResponse().setBody("""{"ok":true,"message":"Transaction deleted"}""")
                    else MockResponse().setResponseCode(deleteCode).setBody("""{"detail":"Transaction not found"}""")
                else -> MockResponse().setResponseCode(404)
            }
        }
        val id = captures.create("kopi 25rb")!!
        captures.send(id, false)
        assertEquals(CaptureActionResult.DONE, captures.deleteFromTracker(id))
        assertEquals(TextCaptureState.DELETED, dao.getById(id)!!.state)
        assertEquals(CaptureActionResult.NOT_ALLOWED, captures.deleteFromTracker(id))

        ids.set(100)
        deleteCode = 404
        val gone = captures.create("kopi 26rb")!!
        captures.send(gone, false)
        assertEquals(CaptureActionResult.NOT_FOUND, captures.deleteFromTracker(gone))
        assertEquals("not_found", dao.getById(gone)!!.errorCode)
        assertEquals(2, requests.count { it.method == "DELETE" })
    }

    @Test fun deleteFromTrackerIsRefusedForMovesAndOfflineChangesNothing() = runBlocking {
        respond = { request ->
            when (request.path) {
                "/api/capture/text" -> envelope(captureJson("recorded", clientIdOf(request), undoInSeconds = 10, movement = true))
                else -> MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_AFTER_REQUEST)
            }
        }
        val move = captures.create("tarik tunai 200rb")!!
        captures.send(move, false)
        assertEquals(CaptureActionResult.NOT_ALLOWED, captures.deleteFromTracker(move))
        assertTrue(requests.none { it.method == "DELETE" })

        respond = { request ->
            when (request.path) {
                "/api/capture/text" -> recorded(request, clientId = clientIdOf(request), undoInSeconds = 10)
                else -> MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_AFTER_REQUEST)
            }
        }
        val offline = captures.create("kopi 25rb")!!
        captures.send(offline, false)
        assertEquals(CaptureActionResult.OFFLINE, captures.deleteFromTracker(offline))
        assertEquals(TextCaptureState.RECORDED, dao.getById(offline)!!.state)
    }

    @Test fun editPatchesTheTransactionAndEndsTheUndoWindow() = runBlocking {
        respond = { request ->
            when {
                request.path == "/api/capture/text" -> recorded(request, clientId = clientIdOf(request), undoInSeconds = 10)
                request.method == "PATCH" -> MockResponse().setBody("""{"ok":true}""")
                request.path == "/api/capture/$SERVER_ID" -> envelope(captureJson("recorded", CLIENT_OF_RECORDED, undoInSeconds = 10, amount = 30_000))
                else -> MockResponse().setResponseCode(404)
            }
        }
        val id = captures.create("kopi 25rb")!!
        captures.send(id, false)
        assertEquals(CaptureActionResult.DONE, captures.edit(id, TransactionUpdateRequest(type = "expense", amount = 30_000)))
        val edited = dao.getById(id)!!
        assertTrue(edited.edited)
        assertEquals(30_000L, TextCaptures.view(edited)!!.transaction!!.amount)
        assertEquals(CaptureActionResult.TOO_LATE, captures.undo(id))
    }

    @Test fun discardOnlyRemovesCapturesTheTrackerNeverAccepted() = runBlocking {
        val fresh = captures.create("kopi 25rb")!!
        assertTrue(captures.discard(fresh))
        respond = { MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_AFTER_REQUEST) }
        val tried = captures.create("kopi 26rb")!!
        captures.send(tried, false)
        // Its first attempt may have reached the tracker, so only a resend can tell.
        assertFalse(captures.discard(tried))
    }

    @Test fun suggestionsAreReadWithTheDeviceTimeAndFiltered() = runBlocking {
        respond = {
            MockResponse().setBody(
                """{"ok":true,"suggestions":[
                {"text":"parkir ptc 10rb","label":"Parkir PTC","amount":10000,"category_name":"Parkir","account_name":"Tunai","reason":"biasanya jam segini"},
                {"text":"","label":"Kosong","amount":1000},
                {"text":"kopi 25rb","label":null,"amount":25000}]}"""
            )
        }
        val suggestions = captures.suggestions()!!
        assertEquals(listOf("parkir ptc 10rb"), suggestions.map { it.text })
        val request = requests.single()
        assertEquals("2027-01-15T15:00:00+07:00", request.requestUrl!!.queryParameter("device_time"))
        // A raw "+" would reach the tracker as a space and fail its offset check.
        assertTrue(request.path!!.contains("%2B07"))

        respond = { MockResponse().setBody("""{"ok":true,"suggestions":[]}""") }
        assertEquals(emptyList<Any>(), captures.suggestions())
        respond = { MockResponse().setResponseCode(500) }
        assertNull(captures.suggestions())
    }

    // --- Fictional tracker responses -------------------------------------------------------------

    private fun clientIdOf(request: RecordedRequest): String =
        JsonParser.parseString(request.body.clone().readUtf8()).asJsonObject["client_capture_id"].asString

    private fun recorded(request: RecordedRequest, clientId: String, undoInSeconds: Long): MockResponse =
        envelope(captureJson("recorded", clientId, undoInSeconds))

    private fun asking(clientId: String, questionId: String = QUESTION_ID): MockResponse = envelope(
        """{"id":"$SERVER_ID","client_capture_id":"$clientId","status":"needs_detail","text":"beli pulsa 50rb",
            "question":{"question_id":"$questionId","type":"account","role":"source","reason":null,"prompt":"Dari rekening mana?",
              "options":[{"id":"$ACCOUNT_ID","label":"Dompet Contoh","type":"ewallet"}],"allows_text":false,"allows_other":true},
            "summary":{"type":"expense","amount":50000,"date":"2027-01-15T15:00:00+07:00","description":"beli pulsa"},
            "transaction":null,"movement":null,"undo_until":null}"""
    )

    /** The tracker's `Date` header and `undo_until` share its clock, which here equals the phone's. */
    private fun envelope(capture: String) = MockResponse()
        .setHeader("Date", DateTimeFormatter.RFC_1123_DATE_TIME.format(java.time.Instant.ofEpochMilli(now).atZone(ZoneOffset.UTC)))
        .setBody("""{"ok":true,"capture":$capture}""")

    private fun captureJson(status: String, clientId: String, undoInSeconds: Long, movement: Boolean = false, amount: Long = 25_000): String {
        val until = java.time.Instant.ofEpochMilli(now + undoInSeconds * 1000).toString()
        val transaction = if (movement) {
            """{"id":"$TRANSACTION_ID","type":"expense","amount":200000,"account_name":"Bank Contoh","category_name":"Pindah saldo"}"""
        } else {
            """{"id":"$TRANSACTION_ID","type":"expense","amount":$amount,"date":"2027-01-15T15:00:00+07:00","account_name":"Tunai",
               "merchant":"Kopi Contoh","notes":"Kopi","category_name":"Makan & Minum","kakeibo":"want","source":"capture"}"""
        }
        val move = if (movement) {
            """{"id":"00000000-0000-4000-8000-000000000555","source":{"id":"$TRANSACTION_ID","account_name":"Bank Contoh"},
               "target":{"id":"00000000-0000-4000-8000-000000000556","account_name":"Tunai"}}"""
        } else "null"
        return """{"id":"$SERVER_ID","client_capture_id":"$clientId","status":"$status","text":"kopi 25rb","question":null,
            "summary":{"type":"${if (movement) "movement" else "expense"}","amount":$amount,"date":"2027-01-15","description":"kopi"},
            "transaction":${if (status == "recorded") transaction else "null"},"movement":$move,"undo_until":"$until"}"""
    }

    private fun error(code: Int, reason: String) = MockResponse().setResponseCode(code).setBody("""{"detail":{"code":"$reason"}}""")

    private companion object {
        const val SERVER_ID = "00000000-0000-4000-8000-000000000301"
        const val TRANSACTION_ID = "00000000-0000-4000-8000-000000000302"
        const val QUESTION_ID = "00000000-0000-4000-8000-000000000303"
        const val ACCOUNT_ID = "00000000-0000-4000-8000-000000000304"
        const val OTHER_ACCOUNT_ID = "00000000-0000-4000-8000-000000000305"
        // The first id the fixture's generator hands out.
        const val CLIENT_OF_ASKING = "00000000-0000-4000-8000-000000000001"
        const val CLIENT_OF_RECORDED = "00000000-0000-4000-8000-000000000001"
    }
}
