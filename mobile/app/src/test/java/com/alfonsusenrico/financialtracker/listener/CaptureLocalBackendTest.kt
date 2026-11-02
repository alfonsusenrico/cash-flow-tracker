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
import kotlinx.coroutines.runBlocking
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.json.JSONObject
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertTrue
import org.junit.Assume.assumeTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.annotation.Config

/**
 * The capture client against the synthetic local backend of `scripts/verify/capture_backend.py`, on the
 * JVM. Runs only with `CAPTURE_LOCAL_BACKEND=http://127.0.0.1:18093`; fictional keys and texts only.
 */
@RunWith(AndroidJUnit4::class)
@Config(sdk = [34], application = TestApplication::class)
class CaptureLocalBackendTest {
    private val context = ApplicationProvider.getApplicationContext<Context>()
    private val backend: String? = System.getenv("CAPTURE_LOCAL_BACKEND")
    private lateinit var db: AppDatabase
    private val captures get() = TextCaptures(context, db, scheduleResend = {})
    private val http = OkHttpClient()

    @Before fun open() {
        assumeTrue(backend != null)
        require(java.net.URI(backend!!).host == "127.0.0.1")
        db = Room.inMemoryDatabaseBuilder(context, AppDatabase::class.java).build()
        TestEndpoint.set(context, backend)
        AppPreferences(context).apiKey = HISTORY_KEY
    }

    @After fun close() {
        if (backend == null) return
        db.close()
        AppPreferences(context).apiKey = ""
    }

    private fun call(method: String, path: String, body: String? = null, key: String = HISTORY_KEY): Pair<Int, JSONObject?> {
        val request = Request.Builder().url("$backend$path").header("Authorization", "Bearer $key")
            .method(method, body?.toRequestBody("application/json".toMediaType())).build()
        http.newCall(request).execute().use { response ->
            val text = response.body?.string().orEmpty()
            return response.code to text.takeIf { it.startsWith("{") }?.let(::JSONObject)
        }
    }

    private suspend fun recorded(text: String): Long {
        val id = captures.create(text)!!
        val sent = captures.send(id, ownerAsked = false)!!
        assertEquals(TextCaptureState.RECORDED, sent.state)
        return id
    }

    @Test fun suggestionsComeFromHistoryAndAreEmptyWithout() = runBlocking {
        // Which groups rank depends on what earlier runs recorded or undid (backend D2); the owner's history
        // always yields some, each a valid prefill.
        val present = captures.suggestions()!!
        assertTrue(present.isNotEmpty() && present.size <= 6)
        assertTrue(present.all { it.amount != null && it.amount!! > 0 && !it.label.isNullOrBlank() })
        AppPreferences(context).apiKey = EMPTY_KEY
        assertEquals(emptyList<Any>(), captures.suggestions())
    }

    @Test fun confidentCaptureIsRecordedAsACapture() = runBlocking {
        val id = recorded("parkir contoh 10rb")
        val capture = db.textCaptureDao().getById(id)!!
        val (code, body) = call("GET", "/api/transactions/${capture.transactionId}")
        assertEquals(200, code)
        assertEquals("capture", body!!.toString().let { Regex("\"source\":\"(\\w+)\"").find(it)!!.groupValues[1] })
        assertEquals(10_000L, TextCaptures.view(capture)!!.transaction!!.amount)
    }

    @Test fun unsureCaptureAsksOneQuestionAtATime() = runBlocking {
        // A phrase never sent before, so neither memory nor history can answer for the owner.
        val phrase = (1..8).map { ('a'..'z').random() }.joinToString("")
        val id = captures.create("makan $phrase 45rb")!!
        assertEquals(TextCaptureState.NEEDS_DETAIL, captures.send(id, false)!!.state)
        val asked = mutableListOf<String>()
        repeat(4) {
            val capture = db.textCaptureDao().getById(id)!!
            if (capture.state != TextCaptureState.NEEDS_DETAIL) return@repeat
            val question = TextCaptures.question(capture)!!
            asked += question.type!!
            val option = question.options!!.first { question.type != "account" || it.label == "Tunai" }
            assertEquals(CaptureActionResult.DONE, captures.answer(id, CaptureAnswer(question.questionId!!, optionId = option.id)))
        }
        assertEquals(listOf("account", "category"), asked)
        assertEquals(TextCaptureState.RECORDED, db.textCaptureDao().getById(id)!!.state)
    }

    @Test fun undoWithinTheWindowRemovesTheTransaction() = runBlocking {
        val id = recorded("parkir contoh 10rb")
        val transaction = db.textCaptureDao().getById(id)!!.transactionId
        assertEquals(CaptureActionResult.DONE, captures.undo(id))
        assertEquals(TextCaptureState.UNDONE, db.textCaptureDao().getById(id)!!.state)
        assertEquals(404, call("GET", "/api/transactions/$transaction").first)
    }

    @Test fun afterTheWindowUndoIsRefusedAndHapusDariTrackerDeletes() = runBlocking {
        val id = recorded("parkir contoh 10rb")
        val capture = db.textCaptureDao().getById(id)!!
        Thread.sleep(16_000)
        assertEquals(CaptureActionResult.TOO_LATE, captures.undo(id))
        val (code, body) = call("POST", "/api/capture/${capture.serverId}/undo", "{}")
        assertEquals(409, code)
        assertEquals("undo_expired", body!!.getJSONObject("detail").getString("code"))
        assertEquals(CaptureActionResult.DONE, captures.deleteFromTracker(id))
        assertEquals(TextCaptureState.DELETED, db.textCaptureDao().getById(id)!!.state)
        assertEquals(404, call("GET", "/api/transactions/${capture.transactionId}").first)
        assertEquals(CaptureActionResult.NOT_ALLOWED, captures.deleteFromTracker(id))
    }

    @Test fun offlineCaptureIsQueuedThenSentOnce() = runBlocking {
        TestEndpoint.set(context, "http://127.0.0.1:1")
        val id = captures.create("parkir contoh 10rb")!!
        val queued = captures.send(id, false)!!
        assertEquals(TextCaptureState.QUEUED, queued.state)
        assertEquals(TextCaptures.NETWORK, queued.errorCode)
        TestEndpoint.set(context, backend!!)
        captures.sendQueued()
        val sent = db.textCaptureDao().getById(id)!!
        assertEquals(TextCaptureState.RECORDED, sent.state)
        // The tracker answers a repeat of the same client id with the same capture and no second transaction.
        val (code, body) = call(
            "POST", "/api/capture/text",
            """{"client_capture_id":"${sent.clientCaptureId}","text":"${sent.text}","device_time":"${sent.deviceTime}"}"""
        )
        assertEquals(200, code)
        val again = body!!.getJSONObject("capture")
        assertEquals(sent.serverId, again.getString("id"))
        assertEquals(sent.transactionId, again.getJSONObject("transaction").getString("id"))
        assertNotNull(sent.undoDeadline)
    }

    private companion object {
        const val HISTORY_KEY = "fictional-capture-key"
        const val EMPTY_KEY = "fictional-empty-key"
    }
}
