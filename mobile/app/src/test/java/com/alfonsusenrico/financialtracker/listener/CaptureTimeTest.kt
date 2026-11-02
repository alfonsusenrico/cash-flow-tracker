package com.alfonsusenrico.financialtracker.listener

import android.content.Context
import androidx.room.Room
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.model.TextCaptureState
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.data.textcapture.CaptureActionResult
import com.alfonsusenrico.financialtracker.listener.data.textcapture.CaptureAnswer
import com.alfonsusenrico.financialtracker.listener.data.textcapture.TextCaptures
import com.alfonsusenrico.financialtracker.listener.data.textcapture.isAnswerable
import com.google.gson.JsonParser
import kotlinx.coroutines.runBlocking
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.annotation.Config
import java.time.ZoneId
import java.util.Collections

/** Catat understands time: it declares the `time` question, answers it by chip or text, and keeps the time label. */
@RunWith(AndroidJUnit4::class)
@Config(sdk = [34], application = TestApplication::class)
class CaptureTimeTest {
    private val context = ApplicationProvider.getApplicationContext<Context>()
    private val prefs get() = AppPreferences(context)
    private lateinit var db: AppDatabase
    private lateinit var server: MockWebServer
    private val requests = Collections.synchronizedList(mutableListOf<RecordedRequest>())
    private val serverId = "00000000-0000-4000-8000-000000007001"
    private val questionId = "00000000-0000-4000-8000-000000007002"
    private val transactionId = "00000000-0000-4000-8000-000000007003"

    private val captures get() = TextCaptures(
        context, db, clock = { 1_800_000_000_000L }, zone = ZoneId.of("Asia/Jakarta"), scheduleResend = {},
        newId = { "00000000-0000-4000-8000-000000007100" }
    )

    private fun capture(body: String) = MockResponse().setBody("""{"ok":true,"capture":$body}""")

    private val asking = """{"id":"$serverId","client_capture_id":"00000000-0000-4000-8000-000000007100","status":"needs_detail",
        "text":"parkir 10rb tadi","question":{"question_id":"$questionId","type":"time","role":null,"reason":"ambiguous",
        "prompt":"Jam berapa?","options":[{"id":"t1","label":"Hari ini 14.00","date":"2027-01-15T14:00:00+07:00"},
        {"id":"t2","label":"Kemarin 14.00","date":"2027-01-14T14:00:00+07:00"}],"allows_text":true,"allows_other":false},
        "summary":{"type":"expense","amount":10000,"date":null,"time_basis":null,"time_evidence":null,"time_label":null,"description":"parkir"}}"""

    private val recorded = """{"id":"$serverId","client_capture_id":"00000000-0000-4000-8000-000000007100","status":"recorded",
        "text":"parkir 10rb tadi","question":null,
        "summary":{"type":"expense","amount":10000,"date":"2027-01-14T14:00:00+07:00","time_basis":"owner","time_evidence":null,
        "time_label":"Kemarin 14.00","description":"parkir"},
        "transaction":{"id":"$transactionId","type":"expense","amount":10000,"account_name":"Tunai","merchant":"Parkir"},
        "recorded_at":"2027-01-15T08:00:00+00:00","undo_until":"2027-01-15T08:00:10+00:00"}"""

    @Before fun open() {
        db = Room.inMemoryDatabaseBuilder(context, AppDatabase::class.java).build()
        server = MockWebServer()
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse {
                requests += request
                return when (request.path) {
                    "/api/capture/text" -> capture(asking)
                    "/api/capture/$serverId/answer" -> capture(recorded)
                    else -> MockResponse().setResponseCode(404)
                }
            }
        }
        server.start()
        TestEndpoint.set(context, server.url("/").toString())
        prefs.apiKey = "cfk_synthetic_capture_time_01"
    }

    @After fun close() {
        db.close()
        server.shutdown()
        prefs.apiKey = ""
    }

    private fun json(path: String) = JsonParser.parseString(requests.single { it.path == path }.body.readUtf8()).asJsonObject

    @Test fun theCaptureDeclaresThatThisPhoneAnswersTimeQuestions() = runBlocking {
        val id = captures.create("parkir 10rb tadi")!!
        val sent = captures.send(id, ownerAsked = true)!!
        val types = json("/api/capture/text")["question_types"].asJsonArray.map { it.asString }
        assertEquals(listOf("amount", "kind", "account", "category", "time"), types)
        // The time question is answerable here, not a refusal.
        assertEquals(TextCaptureState.NEEDS_DETAIL, sent.state)
        assertTrue(TextCaptures.question(sent)!!.isAnswerable())
        assertEquals("2027-01-14T14:00:00+07:00", TextCaptures.question(sent)!!.options!![1].date)
    }

    @Test fun aChipAnswerSendsItsOptionAndTheSummaryKeepsTheTimeLabel() = runBlocking {
        val id = captures.create("parkir 10rb tadi")!!
        captures.send(id, ownerAsked = true)
        assertEquals(CaptureActionResult.DONE, captures.answer(id, CaptureAnswer(questionId, optionId = "t2")))
        val answer = json("/api/capture/$serverId/answer")
        assertEquals("t2", answer["option_id"].asString)
        assertTrue(!answer.has("text"))
        val stored = db.textCaptureDao().getById(id)!!
        assertEquals(TextCaptureState.RECORDED, stored.state)
        assertEquals("Kemarin 14.00", TextCaptures.view(stored)!!.summary!!.timeLabel)
    }

    @Test fun theOwnersOwnWordsAreSentAsText() = runBlocking {
        val id = captures.create("parkir 10rb tadi")!!
        captures.send(id, ownerAsked = true)
        assertEquals(CaptureActionResult.DONE, captures.answer(id, CaptureAnswer(questionId, text = "kemarin jam 2 siang")))
        assertEquals("kemarin jam 2 siang", json("/api/capture/$serverId/answer")["text"].asString)
    }

    @Test fun anAnswerOutsideTheTimeQuestionIsRefusedOnThePhone() = runBlocking {
        val id = captures.create("parkir 10rb tadi")!!
        captures.send(id, ownerAsked = true)
        val refused = captures.answer(id, CaptureAnswer(questionId, optionId = "t9"))
        assertTrue(refused != CaptureActionResult.DONE)
        assertTrue(requests.none { it.path == "/api/capture/$serverId/answer" })
    }
}
