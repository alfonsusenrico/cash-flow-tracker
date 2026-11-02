package com.alfonsusenrico.financialtracker.listener.health

import android.content.Context
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import com.alfonsusenrico.financialtracker.listener.TestApplication
import com.alfonsusenrico.financialtracker.listener.TestEndpoint
import com.alfonsusenrico.financialtracker.listener.data.health.HealthReporter
import com.alfonsusenrico.financialtracker.listener.data.health.HealthReports
import com.alfonsusenrico.financialtracker.listener.data.health.HealthSnapshot
import com.alfonsusenrico.financialtracker.listener.data.health.SelfTestState
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.data.prefs.ServerContact
import com.alfonsusenrico.financialtracker.listener.ui.hub.HubInputs
import com.alfonsusenrico.financialtracker.listener.ui.hub.HubStatus
import com.alfonsusenrico.financialtracker.listener.ui.hub.ProblemKind
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import kotlinx.coroutines.runBlocking
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.SocketPolicy
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.annotation.Config

/** `POST /api/devices/health` against a fictional tracker: the payload the server validates, and every answer it gives. */
@RunWith(AndroidJUnit4::class)
@Config(sdk = [34], application = TestApplication::class)
class HealthReporterTest {
    private val context = ApplicationProvider.getApplicationContext<Context>()
    private val prefs get() = AppPreferences(context)
    private lateinit var server: MockWebServer
    private var now = 1_793_600_000_000L // 2026-11-02, synthetic

    private val passed = HealthSnapshot(
        appVersion = "2.4.0", listenerConnected = true, notificationAccess = true, postNotifications = false,
        batteryUnrestricted = false, selfTest = SelfTestState(SelfTestState.PASSED, null, 1_793_599_000_000L),
        lastCaptureAt = 1_793_598_000_000L, queuePending = 2
    )

    private fun reporter(snapshot: HealthSnapshot = passed) = HealthReporter(context, prefs, collect = { snapshot }, now = { now })

    @Before fun open() {
        server = MockWebServer().apply { start() }
        TestEndpoint.set(context, server.url("/").toString())
        prefs.apiKey = "cfk_synthetic_health_key_0001"
    }

    @After fun close() {
        server.shutdown()
        prefs.apiKey = ""
        prefs.healthNotBefore = 0
        prefs.healthUnsupportedAt = 0
    }

    private fun body(): JsonObject {
        val request = server.takeRequest()
        assertEquals("POST", request.method)
        assertEquals("/api/devices/health", request.path)
        assertEquals("Bearer cfk_synthetic_health_key_0001", request.getHeader("Authorization"))
        return JsonParser.parseString(request.body.readUtf8()).asJsonObject
    }

    @Test fun aPassedReportCarriesEveryContractFieldWithItsType() = runBlocking {
        server.enqueue(MockResponse().setBody("""{"ok":true,"health":{"ready":true}}"""))
        assertEquals(HealthReporter.Outcome.SENT, reporter().send())
        val json = body()
        assertEquals(
            setOf("app_version", "listener_connected", "notification_access", "post_notifications", "battery_unrestricted",
                "self_test", "last_capture_at", "queue_pending"),
            json.keySet()
        )
        assertFalse("the server resolves the device from the key", json.has("device_id"))
        assertEquals("2.4.0", json["app_version"].asString)
        listOf("listener_connected", "notification_access", "post_notifications", "battery_unrestricted").forEach {
            assertTrue("$it is a JSON boolean", json[it].asJsonPrimitive.isBoolean)
        }
        assertFalse(json["post_notifications"].asBoolean)
        assertTrue(json["queue_pending"].asJsonPrimitive.isNumber)
        assertEquals(2, json["queue_pending"].asInt)
        val test = json["self_test"].asJsonObject
        assertEquals("passed", test["status"].asString)
        assertTrue("passed has a null step", test["failed_step"].isJsonNull)
        assertTrue(Regex("""\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}\+00:00""").matches(test["at"].asString))
        assertTrue(Regex(""".+\+00:00""").matches(json["last_capture_at"].asString))
        assertEquals(now, prefs.healthReportedAt)
        assertEquals(ServerContact.OK, prefs.serverContact)
    }

    @Test fun aFailedTestNamesItsStepAndANotRunTestHasNoTime() = runBlocking {
        server.enqueue(MockResponse().setBody("""{"ok":true}"""))
        server.enqueue(MockResponse().setBody("""{"ok":true}"""))
        reporter(passed.copy(selfTest = SelfTestState(SelfTestState.FAILED, "capture", 1_793_599_000_000L), lastCaptureAt = 0)).send()
        val failed = body()
        assertEquals("failed", failed["self_test"].asJsonObject["status"].asString)
        assertEquals("capture", failed["self_test"].asJsonObject["failed_step"].asString)
        assertFalse(failed["self_test"].asJsonObject["at"].isJsonNull)
        assertTrue("never captured is null, not 0", failed["last_capture_at"].isJsonNull)

        reporter(passed.copy(selfTest = SelfTestState(SelfTestState.NOT_RUN, null, 0))).send()
        val notRun = body()["self_test"].asJsonObject
        assertEquals("not_run", notRun["status"].asString)
        assertTrue(notRun["failed_step"].isJsonNull)
        assertTrue(notRun["at"].isJsonNull)
    }

    @Test fun aStoredResultThatBreaksTheRulesIsReportedAsNotRun() {
        val noStep = passed.copy(selfTest = SelfTestState(SelfTestState.FAILED, null, 1_793_599_000_000L)).reportedSelfTest()
        assertEquals(SelfTestState.NOT_RUN, noStep.status)
        val unknownStep = passed.copy(selfTest = SelfTestState(SelfTestState.FAILED, "listener", 1)).reportedSelfTest()
        assertEquals(SelfTestState.NOT_RUN, unknownStep.status)
        val noTime = passed.copy(selfTest = SelfTestState(SelfTestState.PASSED, null, 0)).reportedSelfTest()
        assertEquals(SelfTestState.NOT_RUN, noTime.status)
        val stepOnPassed = passed.copy(selfTest = SelfTestState(SelfTestState.PASSED, "post", 5)).reportedSelfTest()
        assertEquals(null, stepOnPassed.failedStep)
        val json = JsonParser.parseString(passed.copy(queuePending = -3).toJson()).asJsonObject
        assertEquals(0, json["queue_pending"].asInt)
    }

    @Test fun theVersionKeepsOnlyTheCharactersTheContractAllows() {
        assertEquals("2.4.0", HealthSnapshot.version("2.4.0"))
        assertEquals("2.4.0-debug", HealthSnapshot.version("2.4.0-debug"))
        assertEquals("2.4.0beta", HealthSnapshot.version("2.4.0 beta/é"))
        assertEquals(32, HealthSnapshot.version("9".repeat(40)).length)
        assertEquals("unknown", HealthSnapshot.version("  "))
        val version = JsonParser.parseString(passed.copy(appVersion = "2.4.0 (27)").toJson()).asJsonObject["app_version"].asString
        assertTrue(Regex("^[0-9A-Za-z.+_-]{1,32}$").matches(version))
    }

    @Test fun anOlderServerIsToleratedQuietlyAndAskedAgainLater() = runBlocking {
        server.enqueue(MockResponse().setResponseCode(404).setBody("""{"detail":"Not Found"}"""))
        assertEquals(HealthReporter.Outcome.UNSUPPORTED, reporter().send())
        assertEquals(now, prefs.healthUnsupportedAt)
        // A 404 says nothing about the key or the network: the hub shows no problem for it.
        assertEquals(null, prefs.serverContact)
        // The trigger retries with backoff; periodic reports pause, then ask again.
        assertTrue(HealthReports.retries(HealthReporter.Outcome.UNSUPPORTED, attempt = 0, periodic = false))
        assertFalse(HealthReports.retries(HealthReporter.Outcome.UNSUPPORTED, attempt = HealthReports.MAX_ATTEMPTS - 1, periodic = false))
        assertFalse(HealthReports.shouldSend(periodic = true, unsupportedAt = now, now = now + 60_000))
        assertTrue(HealthReports.shouldSend(periodic = true, unsupportedAt = now, now = now + HealthReports.UNSUPPORTED_PAUSE_MILLIS))
        assertTrue(HealthReports.shouldSend(periodic = false, unsupportedAt = now, now = now + 60_000))
    }

    @Test fun aRevokedKeyStopsReportsUntilThePhoneIsPairedAgain() = runBlocking {
        server.enqueue(MockResponse().setResponseCode(401).setBody("""{"detail":{"code":"device_key_invalid"}}"""))
        assertEquals(HealthReporter.Outcome.REFUSED, reporter().send())
        assertEquals(1, server.requestCount)
        assertEquals(ServerContact.REFUSED, prefs.serverContact)
        // The hub shows the existing "connection revoked" problem with its re-pair fix.
        val inputs = HubInputs(
            paired = true, notificationAccess = true, listenerConnected = true, listenerDisconnectedAt = 0, listenerConnectedAt = 0,
            serverContact = prefs.serverContact, online = true, autoSync = true, batteryUnrestricted = true, postNotifications = true,
            capturedToday = 1, lastCaptureApp = null, lastCaptureAt = 0, lastSyncAt = 0, waiting = 0, oldestWaitingAt = null,
            selfTestPassedAt = 0, oem = null, now = now
        )
        assertEquals(ProblemKind.KEY_REFUSED, HubStatus.of(inputs).problem!!.kind)
        // No further request while the key stays the same.
        assertEquals(HealthReporter.Outcome.REFUSED, reporter().send())
        assertEquals(1, server.requestCount)
        assertFalse(HealthReports.retries(HealthReporter.Outcome.REFUSED, 0, periodic = false))
        // A new key is a new pairing, and reports resume.
        prefs.apiKey = "cfk_synthetic_health_key_0002"
        server.enqueue(MockResponse().setBody("""{"ok":true}"""))
        assertEquals(HealthReporter.Outcome.SENT, reporter().send())
        assertEquals(2, server.requestCount)
    }

    @Test fun aRateLimitWaitsForItsRetryAfter() = runBlocking {
        server.enqueue(MockResponse().setResponseCode(429).setHeader("Retry-After", "60"))
        assertEquals(HealthReporter.Outcome.RETRY, reporter().send())
        assertEquals(now + 60_000, prefs.healthNotBefore)
        assertEquals("a rate limit is not an unreachable server", null, prefs.serverContact)
        now += 30_000
        assertEquals(HealthReporter.Outcome.RETRY, reporter().send())
        assertEquals("nothing is sent inside the wait", 1, server.requestCount)
        now += 30_000
        server.enqueue(MockResponse().setBody("""{"ok":true}"""))
        assertEquals(HealthReporter.Outcome.SENT, reporter().send())
        assertEquals(2, server.requestCount)
    }

    @Test fun anUnreachableServerIsRetriedWithBackoff() = runBlocking {
        server.enqueue(MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_AT_START))
        assertEquals(HealthReporter.Outcome.RETRY, reporter().send())
        assertEquals(ServerContact.UNREACHABLE, prefs.serverContact)
        assertTrue(HealthReports.retries(HealthReporter.Outcome.RETRY, 0, periodic = false))
        server.enqueue(MockResponse().setResponseCode(422).setBody("""{"detail":"invalid"}"""))
        assertEquals(HealthReporter.Outcome.REJECTED, reporter().send())
        assertFalse("a refused body is not repeated", HealthReports.retries(HealthReporter.Outcome.REJECTED, 0, periodic = false))
    }

    @Test fun anUnpairedPhoneSendsNothing() = runBlocking {
        prefs.apiKey = ""
        assertEquals(HealthReporter.Outcome.NOT_PAIRED, reporter().send())
        assertEquals(0, server.requestCount)
    }
}
