package com.alfonsusenrico.financialtracker.listener

import android.content.Context
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import com.alfonsusenrico.financialtracker.listener.data.pairing.PairingCode
import com.alfonsusenrico.financialtracker.listener.data.pairing.PairingRepository
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import kotlinx.coroutines.runBlocking
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import okhttp3.mockwebserver.SocketPolicy
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.annotation.Config
import java.net.URLEncoder

@RunWith(AndroidJUnit4::class)
@Config(sdk = [34], application = TestApplication::class)
class PairingRepositoryTest {
    private val context = ApplicationProvider.getApplicationContext<Context>()
    private val prefs get() = AppPreferences(context)
    private lateinit var server: MockWebServer
    private val newKey = "cfk_synthetic_new_pairing_key_0123456789"
    private val oldKey = "cfk_old_synthetic_pairing_key_0123456789"
    private val oldId = "00000000-0000-4000-8000-000000000111"
    private fun code() = PairingCode.parse("cashflow://pair?v=1&server=${URLEncoder.encode(prefs.serverUrl, "UTF-8")}&code=0123456789", prefs.serverUrl)!!
    private fun identity(owner: String = "synthetic-owner") = MockResponse().setBody("""{"ok":true,"user":{"id":"$owner"}}""")
    private fun exchanged() = MockResponse().setBody("""{"ok":true,"api_key":"$newKey"}""")

    @Before fun open() {
        server = MockWebServer().apply { start() }
        TestEndpoint.set(context, server.url("/").toString())
        prefs.apiKey = ""
    }
    @After fun close() { server.shutdown(); prefs.apiKey = "" }

    @Test fun savesBeforeAuthenticatedCheckAndRetriesWithoutAnotherExchange() = runBlocking {
        var authenticated = false
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse = when(request.path) {
                "/api/devices/pair" -> { assertNull(request.getHeader("Authorization")); exchanged() }
                "/api/auth/me" -> {
                    assertTrue("Exchanged key is saved before verification", prefs.apiKey == newKey)
                    assertTrue(request.getHeader("Authorization") == "Bearer $newKey")
                    if (authenticated) identity() else MockResponse().setResponseCode(503)
                }
                else -> MockResponse().setResponseCode(404)
            }
        }
        val repository = PairingRepository(context)
        val first = repository.exchange(code(), "Synthetic Phone")
        assertFalse(first.connected); assertTrue(first.keySaved)
        assertTrue(prefs.apiKey == newKey)
        authenticated = true
        assertTrue(repository.verify().connected)
        assertEquals(3, server.requestCount)
        val paths = List(3) { server.takeRequest().path }
        assertEquals(listOf("/api/devices/pair", "/api/auth/me", "/api/auth/me"), paths)
    }
    @Test fun expiredCodeDoesNotChangePairing() = runBlocking {
        server.enqueue(MockResponse().setResponseCode(410))
        val contextBefore = prefs.pairingContext
        val outcome = PairingRepository(context).exchange(code(), "Synthetic Phone")
        assertFalse(outcome.connected)
        assertTrue(outcome.message.contains("kedaluwarsa"))
        assertEquals(contextBefore, prefs.pairingContext)
        assertEquals("", prefs.apiKey)
    }
    @Test fun rateLimitDoesNotRetryAndHidesServerBody() = runBlocking {
        server.enqueue(MockResponse().setResponseCode(429).setBody("sensitive-raw-diagnostics"))
        val outcome = PairingRepository(context).exchange(code(), "Synthetic Phone")
        assertTrue(outcome.message.contains("Tunggu"))
        assertFalse(outcome.message.contains("sensitive-raw-diagnostics"))
        assertEquals(1, server.requestCount)
    }
    @Test fun exchangeNeverFollowsRedirects() = runBlocking {
        server.enqueue(MockResponse().setResponseCode(307).addHeader("Location", server.url("/leak")))
        val outcome = PairingRepository(context).exchange(code(), "Synthetic Phone")
        assertFalse(outcome.connected)
        assertEquals(1, server.requestCount)
    }
    @Test fun disconnectAfterExchangeDoesNotSilentlyRepeatTheCode() = runBlocking {
        server.enqueue(MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_AFTER_REQUEST))
        val outcome = PairingRepository(context).exchange(code(), "Synthetic Phone")
        assertFalse(outcome.connected)
        assertTrue(outcome.message.contains("kode QR baru"))
        assertEquals(1, server.requestCount)
    }
    @Test fun manualKeyIsTestedBeforeItIsSaved() = runBlocking {
        val repository = PairingRepository(context)
        server.enqueue(MockResponse().setResponseCode(401))
        assertFalse(repository.connectManual(newKey).connected)
        assertEquals("", prefs.apiKey)
        server.enqueue(identity())
        assertTrue(repository.connectManual(newKey).connected)
        assertTrue(prefs.apiKey == newKey)
        assertTrue(List(2) { server.takeRequest().path }.all { it == "/api/auth/me" })
    }
    @Test fun invalidManualKeyDoesNotSendOrOverwrite() = runBlocking {
        val repository = PairingRepository(context)
        assertFalse(repository.connectManual("invalid").connected)
        assertEquals(0, server.requestCount)
        assertEquals("", prefs.apiKey)
    }
    @Test fun rePairUsesStableIdAndRevokesOnlyAfterAuthentication() = runBlocking {
        prefs.apiKey = oldKey
        prefs.deviceId = "synthetic-stable-device"
        server.enqueue(identity())
        server.enqueue(MockResponse().setBody("""{"ok":true,"devices":[{"id":"$oldId","key_prefix":"${oldKey.take(8)}"}]}"""))
        server.enqueue(exchanged())
        server.enqueue(identity())
        server.enqueue(MockResponse().setBody("{}"))
        assertTrue(PairingRepository(context).exchange(code(), "Synthetic Phone").connected)
        val requests = List(5) { server.takeRequest() }
        assertTrue(requests[2].body.readUtf8().contains("synthetic-stable-device"))
        assertEquals(listOf("/api/auth/me", "/api/devices", "/api/devices/pair", "/api/auth/me", "/api/devices/$oldId"), requests.map { it.path })
        assertEquals("DELETE", requests.last().method)
        assertTrue(requests.last().getHeader("Authorization") == "Bearer $newKey")
    }
    @Test fun rePairNeverRevokesAnotherOwnersKey() = runBlocking {
        prefs.apiKey = oldKey
        server.enqueue(identity("old-owner"))
        server.enqueue(MockResponse().setBody("""{"ok":true,"devices":[{"id":"$oldId","key_prefix":"${oldKey.take(8)}"}]}"""))
        server.enqueue(exchanged())
        server.enqueue(identity("new-owner"))
        assertTrue(PairingRepository(context).exchange(code(), "Synthetic Phone").connected)
        assertEquals(4, server.requestCount)
    }
    @Test fun failedRevocationKeepsNewKeyAndGivesWebRecovery() = runBlocking {
        prefs.apiKey = oldKey
        server.enqueue(identity())
        server.enqueue(MockResponse().setBody("""{"ok":true,"devices":[{"id":"$oldId","key_prefix":"${oldKey.take(8)}"}]}"""))
        server.enqueue(exchanged()); server.enqueue(identity()); server.enqueue(MockResponse().setResponseCode(503))
        val result = PairingRepository(context).exchange(code(), "Synthetic Phone")
        assertTrue(result.connected)
        assertTrue(result.message.contains("Cabut"))
        assertTrue(prefs.apiKey == newKey)
    }
}
