package com.alfonsusenrico.financialtracker.listener.report

import android.content.Context
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import com.alfonsusenrico.financialtracker.listener.TestApplication
import com.alfonsusenrico.financialtracker.listener.TestEndpoint
import com.alfonsusenrico.financialtracker.listener.data.network.model.LatestReportResponse
import com.alfonsusenrico.financialtracker.listener.data.network.model.ReportPeriod
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.data.prefs.StoredReport
import com.alfonsusenrico.financialtracker.listener.data.report.ReportReadiness
import com.alfonsusenrico.financialtracker.listener.data.report.WebLinks
import kotlinx.coroutines.runBlocking
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.annotation.Config

/** "Laporan siap" from `GET /api/reports/monthly/latest`: stored for the hub, notified once, opened on the web. */
@RunWith(AndroidJUnit4::class)
@Config(sdk = [34], application = TestApplication::class)
class ReportReadinessTest {
    private val context = ApplicationProvider.getApplicationContext<Context>()
    private val prefs get() = AppPreferences(context)
    private lateinit var server: MockWebServer
    private val notified = mutableListOf<StoredReport>()
    private fun readiness() = ReportReadiness(context, prefs, notify = { notified += it; true }, now = { 5_000L })

    private val ready = """{"ok":true,"timezone":"Asia/Jakarta","basis":"month","ready":true,
        "period":{"label":"Oktober 2026","start":"2026-09-30T17:00:00+00:00","end":"2026-10-31T17:00:00+00:00",
        "start_date":"2026-10-01","end_date":"2026-10-31"},"url":"/laporan?basis=month&start=2026-10-01",
        "pending_review":3,"unreviewed":1}"""

    @Before fun open() {
        server = MockWebServer().apply { start() }
        TestEndpoint.set(context, server.url("/").toString())
        prefs.apiKey = "cfk_synthetic_report_key_0001"
    }

    @After fun close() {
        server.shutdown()
        prefs.apiKey = ""
    }

    @Test fun aReadyReportIsStoredAndNotifiedOncePerPeriod() = runBlocking {
        server.enqueue(MockResponse().setBody(ready))
        server.enqueue(MockResponse().setBody(ready))
        val first = readiness().refresh()!!
        assertEquals(StoredReport("Oktober 2026", "/laporan?basis=month&start=2026-10-01", "2026-10-01", opened = false), first)
        assertEquals("/api/reports/monthly/latest", server.takeRequest().path)
        readiness().refresh()
        assertEquals("the same period is notified once", 1, notified.size)
        assertEquals(5_000L, prefs.reportCheckedAt)
        prefs.markReportOpened("2026-10-01")
        assertTrue(prefs.readyReport(prefs.pairingContext)!!.opened)
    }

    @Test fun anOlderServerOrANotReadyAnswerShowsNothing() = runBlocking {
        server.enqueue(MockResponse().setResponseCode(404))
        assertNull(readiness().refresh())
        server.enqueue(MockResponse().setBody("""{"ok":true,"ready":false,"period":null,"url":null}"""))
        assertNull(readiness().refresh())
        assertTrue(notified.isEmpty())
    }

    @Test fun onlyTheTrackersOwnReportPathIsAccepted() {
        fun body(url: String?, label: String? = "Oktober 2026", start: String? = "2026-10-01") =
            LatestReportResponse(ok = true, ready = true, period = ReportPeriod(label, start), url = url)
        assertEquals("2026-10-01", ReportReadiness.valid(body("/laporan?basis=month&start=2026-10-01"))!!.start)
        assertNull(ReportReadiness.valid(body("https://elsewhere.invalid/laporan")))
        assertNull(ReportReadiness.valid(body("//elsewhere.invalid/laporan")))
        assertNull(ReportReadiness.valid(body("/ledger")))
        assertNull(ReportReadiness.valid(body("/laporan", label = " ")))
        assertNull(ReportReadiness.valid(body("/laporan", start = "Oktober")))
        assertNull(ReportReadiness.valid(body("/laporan").copy(ready = false)))
        assertEquals("https://tracker.example.invalid/laporan", WebLinks.url("/laporan", "https://tracker.example.invalid/"))
        assertNull(WebLinks.url("javascript:alert(1)", "https://tracker.example.invalid"))
    }

    @Test fun aRePairForgetsTheReportNotice() = runBlocking {
        server.enqueue(MockResponse().setBody(ready))
        readiness().refresh()
        prefs.apiKey = "cfk_synthetic_report_key_0002"
        assertNull(prefs.readyReport(prefs.pairingContext))
    }
}
