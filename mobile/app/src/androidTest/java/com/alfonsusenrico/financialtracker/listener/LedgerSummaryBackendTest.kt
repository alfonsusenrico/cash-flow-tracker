package com.alfonsusenrico.financialtracker.listener

import android.app.Application
import androidx.compose.ui.test.assertIsDisplayed
import androidx.compose.ui.test.junit4.createComposeRule
import androidx.compose.ui.test.onNodeWithContentDescription
import androidx.test.platform.app.InstrumentationRegistry
import com.alfonsusenrico.financialtracker.listener.data.network.ApiClient
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.ui.home.HomeRoute
import com.alfonsusenrico.financialtracker.listener.ui.shell.CompanionViewModel
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionTheme
import kotlinx.coroutines.runBlocking
import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.OkHttpClient
import okhttp3.Request
import org.json.JSONObject
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Assume.assumeTrue
import org.junit.Before
import org.junit.Rule
import org.junit.Test

/** Local synthetic backend only; no production or personal financial data. */
class LedgerSummaryBackendTest {
    @get:Rule val compose = createComposeRule()
    private val context get() = InstrumentationRegistry.getInstrumentation().targetContext

    @Before fun setup() {
        assumeTrue(InstrumentationRegistry.getArguments().getString("localBackend") == "true")
        check(context.packageName.endsWith(".verification"))
        TestEndpoint.set(context, "http://127.0.0.1:18000")
        AppPreferences(context).apiKey = "fictional-phone-key"
    }

    @After fun cleanup() {
        if (context.packageName.endsWith(".verification")) AppPreferences(context).apiKey = ""
    }

    @Test fun serverSummaryMatchesLedgerAndAppearsOnHome(): Unit = runBlocking {
        val response = ApiClient(context).createService().getLedgerCashFlowSummary()
        assertTrue(response.isSuccessful)
        val summary = response.body()!!
        assertTrue(summary.isComplete())
        val ledgerUrl = "http://127.0.0.1:18000/api/transactions".toHttpUrl().newBuilder()
            .addQueryParameter("from_date", summary.start)
            .addQueryParameter("to_date", summary.end)
            .build()
        val ledgerRequest = Request.Builder().url(ledgerUrl).header("Authorization", "Bearer fictional-phone-key").build()
        OkHttpClient().newCall(ledgerRequest).execute().use { result ->
            assertTrue(result.isSuccessful)
            val figures = JSONObject(result.body!!.string()).getJSONObject("summary").getJSONObject("cumulative")
            assertEquals(summary.inflow, figures.getLong("inflow"))
            assertEquals(summary.outflow, figures.getLong("outflow"))
            assertEquals(summary.net, figures.getLong("net"))
        }
        val model = CompanionViewModel(context.applicationContext as Application)
        compose.setContent {
            CompanionTheme(dark = false) {
                HomeRoute(model, listenerPermissionGranted = true, onOpenQuestions = {}, onOpenActivity = {}, onOpenSettings = {})
            }
        }
        compose.waitUntil(10_000) { model.ledgerSummary.value.summary != null }
        compose.onNodeWithContentDescription("Uang keluar tracker Rp300.000").assertIsDisplayed()
    }
}
