package com.alfonsusenrico.financialtracker.listener

import androidx.test.ext.junit.runners.AndroidJUnit4
import com.alfonsusenrico.financialtracker.listener.data.network.serverErrorCode
import com.alfonsusenrico.financialtracker.listener.data.network.serverMessage
import com.alfonsusenrico.financialtracker.listener.data.sync.PurposeAnswers
import com.google.gson.JsonParser
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.ResponseBody.Companion.toResponseBody
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.annotation.Config
import retrofit2.Response
import java.io.File

/** Error bodies captured from the localize-server-text branch, read by the companion's own parsers. */
@RunWith(AndroidJUnit4::class)
@Config(sdk = [34], application = TestApplication::class)
class CapturedServerTextTest {
    private fun response(status: Int, body: String): Response<Any> =
        Response.error(status, body.toResponseBody("application/json".toMediaType()))

    @Test fun everyCapturedBodyShowsTheServerMessageAndCode() {
        val dir = File(javaClass.classLoader!!.getResource("server-text")!!.toURI())
        val files = dir.listFiles()!!.filter { it.name.endsWith(".json") }.sortedBy { it.name }
        assertTrue(files.size >= 10)
        for (file in files) {
            val captured = JsonParser.parseString(file.readText()).asJsonObject
            val status = captured["status"].asInt
            val body = captured["body"].toString()
            val detail = captured["body"].asJsonObject["detail"]
            if (detail.isJsonObject) {
                val expectedMessage = detail.asJsonObject["message"].asString
                val expectedCode = detail.asJsonObject["code"].asString
                assertEquals(file.name, expectedMessage, serverMessage(response(status, body)))
                assertEquals(file.name, expectedCode, serverErrorCode(response(status, body)))
                assertEquals(file.name, expectedCode, PurposeAnswers.errorCode(response(status, body)))
            } else {
                // FastAPI's schema-validation list keeps its shape; the companion falls back to the status.
                assertEquals(file.name, "HTTP $status", serverMessage(response(status, body)))
            }
            println("${file.name}: ${serverMessage(response(status, body))}")
        }
    }
}
