package com.alfonsusenrico.financialtracker.listener.ui

import android.app.Application
import android.os.Looper
import androidx.lifecycle.viewModelScope
import androidx.room.Room
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import com.alfonsusenrico.financialtracker.listener.TestEndpoint
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.ui.shell.CompanionViewModel
import com.alfonsusenrico.financialtracker.listener.ui.shell.LedgerSummaryState
import kotlinx.coroutines.cancel
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.Shadows.shadowOf
import org.robolectric.annotation.Config
import java.util.concurrent.TimeUnit

/** The tracker's cash-flow summary across refreshes, process restarts and pairings, with synthetic responses only. */
@RunWith(AndroidJUnit4::class)
@Config(sdk = [34], application = com.alfonsusenrico.financialtracker.listener.TestApplication::class)
class LedgerSummaryRefreshTest {
    private val app = ApplicationProvider.getApplicationContext<Application>()
    private val prefs get() = AppPreferences(app)
    private lateinit var db: AppDatabase
    private lateinit var server: MockWebServer
    private val viewModels = mutableListOf<CompanionViewModel>()
    @Volatile private var summaryResponse: () -> MockResponse = { MockResponse().setBody(summaryJson(outflow = 200_000)) }

    @Before fun open() {
        db = Room.inMemoryDatabaseBuilder(app, AppDatabase::class.java).build()
        server = MockWebServer()
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse = when (request.path) {
                "/api/dashboard/cash-flow-summary?period=today" -> summaryResponse()
                "/api/accounts" -> MockResponse().setBody("""{"ok":true,"accounts":[]}""")
                "/api/categories" -> MockResponse().setBody("""{"ok":true,"categories":[]}""")
                else -> MockResponse().setResponseCode(404)
            }
        }
        server.start()
        TestEndpoint.set(app, server.url("/").toString())
        prefs.apiKey = "fictional-ledger-key"
    }

    @After fun close() {
        viewModels.forEach { it.viewModelScope.cancel() }
        db.close()
        server.shutdown()
        prefs.apiKey = ""
    }

    /** A new view model reads storage the way the app does after its process was killed. */
    private fun viewModel() = CompanionViewModel(app, db).also { viewModels += it }

    private fun CompanionViewModel.refreshed(): LedgerSummaryState {
        refreshLedgerSummary()
        val deadline = System.currentTimeMillis() + 5_000
        while (ledgerSummary.value.loading) {
            check(System.currentTimeMillis() < deadline) { "refresh did not finish: ${ledgerSummary.value}" }
            shadowOf(Looper.getMainLooper()).idle()
            Thread.sleep(10)
        }
        return ledgerSummary.value
    }

    private fun cacheSummary() {
        val state = viewModel().refreshed()
        assertEquals(200_000L, state.summary!!.outflow)
    }

    @Test fun aSuccessfulRefreshIsShownAsCachedAfterARestart() {
        assertNull(viewModel().ledgerSummary.value.summary)
        val fresh = viewModel().refreshed()
        assertFalse(fresh.cached)
        assertFalse(fresh.stale)
        val request = server.takeRequestFor("/api/dashboard/cash-flow-summary?period=today")
        assertEquals("Bearer fictional-ledger-key", request.getHeader("Authorization"))

        val restored = viewModel().ledgerSummary.value
        assertTrue(restored.cached)
        assertFalse(restored.stale)
        assertEquals(fresh.summary, restored.summary)
        assertEquals("2026-10-30T03:00:01+00:00", restored.summary!!.generatedAt)
    }

    @Test fun aSummaryIsNeverShownForAnotherPairing() {
        cacheSummary()
        val firstPairing = prefs.pairingContext
        prefs.apiKey = "fictional-other-key"
        assertNull(viewModel().ledgerSummary.value.summary)
        assertNull(prefs.ledgerSummaryJson(firstPairing))

        // Returning to the first key restores its pairing context but not the figures fetched before the switch.
        prefs.apiKey = "fictional-ledger-key"
        assertEquals(firstPairing, prefs.pairingContext)
        assertNull(viewModel().ledgerSummary.value.summary)
        assertFalse(prefs.saveLedgerSummary("fictional-stale-pairing", summaryJson(outflow = 1)))
        assertNull(prefs.ledgerSummaryJson("fictional-stale-pairing"))
    }

    @Test fun aRefreshAfterRepairingDropsTheOldFiguresFromTheOpenScreen() {
        val model = viewModel()
        assertEquals(200_000L, model.refreshed().summary!!.outflow)
        prefs.apiKey = "fictional-other-key"
        assertNull(model.currentLedgerSummary(model.ledgerSummary.value).summary)
        summaryResponse = { MockResponse().setResponseCode(503) }
        val state = model.refreshed()
        assertNull(state.summary)
        assertTrue(state.stale)
    }

    @Test fun aFailedRefreshKeepsTheCachedFiguresAsStale() {
        cacheSummary()
        val model = viewModel()
        summaryResponse = { MockResponse().setResponseCode(503) }
        val serverError = model.refreshed()
        assertTrue(serverError.stale)
        assertEquals(200_000L, serverError.summary!!.outflow)
        assertEquals("Arus kas belum dapat diperbarui.", serverError.error)

        TestEndpoint.set(app, "http://127.0.0.1:1")
        val offline = model.refreshed()
        assertTrue(offline.stale)
        assertEquals(200_000L, offline.summary!!.outflow)
        assertEquals("Tidak terhubung ke tracker.", offline.error)
        assertEquals(200_000L, viewModel().ledgerSummary.value.summary!!.outflow)
    }

    @Test fun aRefusedKeyDropsTheFiguresAndAsksToReconnect() {
        for (code in listOf(401, 403)) {
            summaryResponse = { MockResponse().setBody(summaryJson(outflow = 200_000)) }
            cacheSummary()
            summaryResponse = { MockResponse().setResponseCode(code) }
            val refused = viewModel().refreshed()
            assertNull("HTTP $code", refused.summary)
            assertFalse(refused.stale)
            assertEquals("Hubungkan kembali tracker untuk melihat arus kas.", refused.error)
            assertNull("HTTP $code", viewModel().ledgerSummary.value.summary)
        }
    }

    @Test fun anIncompletePayloadNeverReplacesTheCachedFigures() {
        cacheSummary()
        summaryResponse = { MockResponse().setBody(summaryJson(outflow = 999_000, net = 1)) }
        val rejected = viewModel().refreshed()
        assertTrue(rejected.stale)
        assertEquals(200_000L, rejected.summary!!.outflow)
        assertEquals(200_000L, viewModel().ledgerSummary.value.summary!!.outflow)

        summaryResponse = { MockResponse().setBody("""{"ok":true,"period":"today"}""") }
        assertEquals(200_000L, viewModel().refreshed().summary!!.outflow)
    }

    @Test fun anUnpairedPhoneAsksToConnectWithoutCallingTheTracker() {
        prefs.apiKey = ""
        val before = server.requestCount
        val state = viewModel().refreshed()
        assertNull(state.summary)
        assertEquals("Hubungkan tracker untuk melihat arus kas.", state.error)
        assertEquals(before, server.requestCount)
    }

    private fun MockWebServer.takeRequestFor(path: String): RecordedRequest {
        while (true) {
            val request = takeRequest(5, TimeUnit.SECONDS) ?: error("no request for $path")
            if (request.path == path) return request
        }
    }

    private fun summaryJson(outflow: Long, net: Long = 500_000L - outflow, topCategories: String? = null): String {
        val inflow = 500_000L
        return """{"ok":true,"period":"today","timezone":"Asia/Jakarta","currency":"IDR",""" +
            """"start":"2026-10-29T17:00:00+00:00","end":"2026-10-30T03:00:00+00:00",""" +
            """"generated_at":"2026-10-30T03:00:01+00:00","inflow":$inflow,"outflow":$outflow,""" +
            """"net":$net,"gross_outflow":$outflow,"reimbursement_offset":0,""" +
            """"reimbursement_unallocated":0,"reimbursement_excess":0""" +
            (topCategories?.let { ""","top_categories":$it""" } ?: "") + "}"
    }

    private val twoCategories = """[{"category_id":"00000000-0000-4000-8000-000000000201","name":"Makan & Minum",""" +
        """"kakeibo_type":"need","outflow":150000},{"category_id":"00000000-0000-4000-8000-000000000202",""" +
        """"name":"Kopi","kakeibo_type":"want","outflow":50000}]"""

    @Test fun topCategoriesAreCachedWithTheSummaryAndShareItsFreshness() {
        summaryResponse = { MockResponse().setBody(summaryJson(outflow = 200_000, topCategories = twoCategories)) }
        val fresh = viewModel().refreshed()
        assertEquals(listOf("Makan & Minum", "Kopi"), fresh.summary!!.shownTopCategories()!!.map { it.name })

        val restored = viewModel().ledgerSummary.value
        assertTrue(restored.cached)
        assertEquals(listOf(150_000L, 50_000L), restored.summary!!.shownTopCategories()!!.map { it.outflow })

        summaryResponse = { MockResponse().setResponseCode(503) }
        val stale = viewModel().refreshed()
        assertTrue(stale.stale)
        assertEquals(listOf("Makan & Minum", "Kopi"), stale.summary!!.shownTopCategories()!!.map { it.name })
    }

    @Test fun anUncategorisedEntryIsCachedWithItsNullIdAndRestored() {
        val withUncategorised = """[{"category_id":"00000000-0000-4000-8000-000000000201","name":"Makan & Minum",""" +
            """"kakeibo_type":"need","outflow":150000},{"category_id":null,"name":"Tanpa kategori",""" +
            """"kakeibo_type":null,"outflow":50000}]"""
        summaryResponse = { MockResponse().setBody(summaryJson(outflow = 200_000, topCategories = withUncategorised)) }
        val fresh = viewModel().refreshed()
        assertEquals(listOf("Makan & Minum", "Tanpa kategori"), fresh.summary!!.shownTopCategories()!!.map { it.name })

        val restored = viewModel().ledgerSummary.value
        assertTrue(restored.cached)
        assertEquals(fresh.summary, restored.summary)
        val uncategorised = restored.summary!!.shownTopCategories()!![1]
        assertNull(uncategorised.categoryId)
        assertEquals("Tanpa kategori", uncategorised.name)
        assertNull(uncategorised.kakeiboType)
        assertEquals(50_000L, uncategorised.outflow)
    }

    @Test fun aNewSummaryReplacesTheCachedCategoriesEvenWhenItHasNone() {
        summaryResponse = { MockResponse().setBody(summaryJson(outflow = 200_000, topCategories = twoCategories)) }
        viewModel().refreshed()
        summaryResponse = { MockResponse().setBody(summaryJson(outflow = 0, topCategories = "[]")) }
        assertEquals(emptyList<Any>(), viewModel().refreshed().summary!!.shownTopCategories())
        assertEquals(emptyList<Any>(), viewModel().ledgerSummary.value.summary!!.shownTopCategories())

        // An older tracker: the field is absent, so the restored summary has no categories rather than an empty list.
        summaryResponse = { MockResponse().setBody(summaryJson(outflow = 200_000)) }
        viewModel().refreshed()
        assertNull(viewModel().ledgerSummary.value.summary!!.shownTopCategories())
    }

    @Test fun repairingDropsTheCachedCategoriesWithTheSummary() {
        summaryResponse = { MockResponse().setBody(summaryJson(outflow = 200_000, topCategories = twoCategories)) }
        viewModel().refreshed()
        prefs.apiKey = "fictional-other-key"
        assertNull(viewModel().ledgerSummary.value.summary)
    }
}
