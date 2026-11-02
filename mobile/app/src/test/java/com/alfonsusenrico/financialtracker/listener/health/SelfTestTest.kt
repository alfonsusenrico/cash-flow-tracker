package com.alfonsusenrico.financialtracker.listener.health

import android.content.Context
import androidx.room.Room
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import com.alfonsusenrico.financialtracker.listener.TestApplication
import com.alfonsusenrico.financialtracker.listener.TestEndpoint
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.health.HealthReporter
import com.alfonsusenrico.financialtracker.listener.data.health.HealthSnapshot
import com.alfonsusenrico.financialtracker.listener.data.health.SelfTestCatches
import com.alfonsusenrico.financialtracker.listener.data.health.SelfTestMarker
import com.alfonsusenrico.financialtracker.listener.data.health.SelfTestPoster
import com.alfonsusenrico.financialtracker.listener.data.health.SelfTestRunner
import com.alfonsusenrico.financialtracker.listener.data.health.SelfTestState
import com.alfonsusenrico.financialtracker.listener.data.health.SelfTestStep
import com.alfonsusenrico.financialtracker.listener.data.health.StepState
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.service.NotificationGate
import com.google.gson.JsonParser
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.annotation.Config

/**
 * The setup self-test: post a marked notification, catch it with the phone's own listener, report ready. The
 * marked notification never reaches capture, storage or the tracker as a notification.
 */
@RunWith(AndroidJUnit4::class)
@Config(sdk = [34], application = TestApplication::class)
class SelfTestTest {
    private val context = ApplicationProvider.getApplicationContext<Context>()
    private val prefs get() = AppPreferences(context)
    private val own = "com.alfonsusenrico.financialtracker.listener.debug"
    private val token = "00000000-0000-4000-8000-00000000c0de"

    /** Posts by handing the notification to the listener's gate, as Android would; [deliver] false drops it. */
    private inner class FakePoster(val canPost: Boolean = true, val deliver: Boolean = true) : SelfTestPoster {
        val posted = mutableListOf<String>()
        var removed = 0
        override fun post(token: String): Boolean {
            if (!canPost) return false
            posted += token
            if (deliver) {
                val route = NotificationGate.route(own, own, SelfTestMarker.TAG, token) { true }
                if (route is NotificationGate.Route.SelfTest) SelfTestCatches.caught(route.token)
            }
            return true
        }
        override fun remove() { removed++ }
    }

    private val steps = mutableListOf<Pair<SelfTestStep, StepState>>()
    private var laterReports = 0

    private fun runner(poster: SelfTestPoster, report: suspend () -> HealthReporter.Outcome) =
        SelfTestRunner(poster, prefs, report, requestLaterReport = { laterReports++ }, newToken = { token })

    @Test fun aCaughtTestNotificationIsReportedAndPasses() = runTest {
        val poster = FakePoster()
        val outcome = runner(poster) { HealthReporter.Outcome.SENT }.run { step, state -> steps += step to state }
        assertTrue(outcome.passed)
        assertNull(outcome.failedStep)
        assertEquals(listOf(token), poster.posted)
        assertEquals("the test notification is removed afterwards", 1, poster.removed)
        assertEquals(SelfTestState.PASSED, prefs.selfTestStatus)
        assertTrue(prefs.selfTestAt > 0)
        assertEquals(
            listOf(SelfTestStep.POST to StepState.DONE, SelfTestStep.CAPTURE to StepState.DONE, SelfTestStep.SERVER to StepState.DONE),
            steps.filter { it.second == StepState.DONE }
        )
        assertEquals(0, laterReports)
    }

    @Test fun aNotificationThatCannotShowFailsAtPost() = runTest {
        val outcome = runner(FakePoster(canPost = false)) { error("no report before the notification shows") }.run()
        assertEquals(SelfTestStep.POST, outcome.failedStep)
        assertEquals(SelfTestState.FAILED, prefs.selfTestStatus)
        assertEquals("post", prefs.selfTestFailedStep)
        assertEquals("the failure still reaches the tracker later", 1, laterReports)
    }

    @Test fun aNotificationTheListenerNeverHearsFailsAtCaptureAfterTenSeconds() = runTest {
        val poster = FakePoster(deliver = false)
        val start = testScheduler.currentTime
        val outcome = runner(poster) { error("not reported") }.run()
        assertEquals(SelfTestStep.CAPTURE, outcome.failedStep)
        assertEquals(SelfTestRunner.CATCH_TIMEOUT_MILLIS, testScheduler.currentTime - start)
        assertEquals("capture", prefs.selfTestFailedStep)
        assertEquals(1, poster.removed)
    }

    @Test fun anUnreachableServerFailsAtServerAndIsRetriedAlone() = runTest {
        val outcome = runner(FakePoster()) { HealthReporter.Outcome.RETRY }.run()
        assertEquals(SelfTestStep.SERVER, outcome.failedStep)
        assertEquals("server", prefs.selfTestFailedStep)
        assertEquals(1, laterReports)
        val retried = runner(FakePoster()) { HealthReporter.Outcome.SENT }.reportReady()
        assertTrue(retried.passed)
        assertEquals(SelfTestState.PASSED, prefs.selfTestStatus)
    }

    @Test fun anOlderServerWithoutHealthStillLetsThePhonePass() = runTest {
        assertTrue(runner(FakePoster()) { HealthReporter.Outcome.UNSUPPORTED }.run().passed)
    }

    @Test fun theGateAnswersOnlyThisAppsMarkedNotification() {
        assertEquals(NotificationGate.Route.SelfTest(token), NotificationGate.route(own, own, SelfTestMarker.TAG, token) { true })
        // Without the tag or token, or from another app, a marker means nothing.
        assertEquals(NotificationGate.Route.Drop, NotificationGate.route(own, own, null, token) { true })
        assertEquals(NotificationGate.Route.Drop, NotificationGate.route(own, own, SelfTestMarker.TAG, null) { true })
        assertEquals(NotificationGate.Route.Capture, NotificationGate.route("com.gojek.app", own, SelfTestMarker.TAG, token) { true })
        assertEquals(NotificationGate.Route.Drop, NotificationGate.route("com.whatsapp", own, SelfTestMarker.TAG, token) { true })
        assertEquals(NotificationGate.Route.Drop, NotificationGate.route("com.gojek.app", own, null, null) { false })
        assertFalse("a test nobody waits for is not caught", SelfTestCatches.caught("00000000-0000-4000-8000-0000000000ff"))
    }

    @Test fun theSelfTestOnlyReportsHealthAndNeverStoresOrSendsANotification() = runBlocking {
        val db = Room.inMemoryDatabaseBuilder(context, AppDatabase::class.java).allowMainThreadQueries().build()
        val server = MockWebServer().apply { start() }
        try {
            TestEndpoint.set(context, server.url("/").toString())
            prefs.apiKey = "cfk_synthetic_selftest_key_01"
            server.enqueue(MockResponse().setBody("""{"ok":true,"health":{"ready":true}}"""))
            val snapshot = { HealthSnapshot("2.4.0", true, true, true, true, SelfTestState(prefs.selfTestStatus, prefs.selfTestFailedStep, prefs.selfTestAt), 0, 0) }
            val outcome = runner(FakePoster()) { HealthReporter(context, prefs, collect = snapshot).send() }.run()
            assertTrue(outcome.passed)
            assertEquals("only the health report went out", 1, server.requestCount)
            val request = server.takeRequest()
            assertEquals("/api/devices/health", request.path)
            val body = JsonParser.parseString(request.body.readUtf8()).asJsonObject
            assertEquals("passed", body["self_test"].asJsonObject["status"].asString)
            assertFalse("no notification text travels in the report", request.body.toString().contains("Tes Cash Flow Tracker"))
            assertEquals("nothing was stored as a capture", 0, db.notificationDao().getUnsyncedNotifications(prefs.pairingContext).size)
            assertEquals(0, db.notificationDao().countUnsubmitted(prefs.pairingContext))
        } finally {
            server.shutdown()
            db.close()
            prefs.apiKey = ""
        }
    }
}
