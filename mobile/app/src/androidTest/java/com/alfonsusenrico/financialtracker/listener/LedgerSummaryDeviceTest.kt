package com.alfonsusenrico.financialtracker.listener

import android.app.Application
import androidx.compose.ui.test.assertIsDisplayed
import androidx.compose.ui.test.junit4.createComposeRule
import androidx.compose.ui.test.onNodeWithContentDescription
import androidx.compose.ui.test.onNodeWithText
import androidx.compose.ui.test.performClick
import androidx.compose.ui.test.performScrollTo
import androidx.test.platform.app.InstrumentationRegistry
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.model.RawNotificationEntity
import kotlinx.coroutines.runBlocking
import java.util.UUID
import com.alfonsusenrico.financialtracker.listener.ui.home.HomeRoute
import com.alfonsusenrico.financialtracker.listener.ui.shell.CompanionViewModel
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionTheme
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test

/** Only the isolated verification app and synthetic responses are used. */
class LedgerSummaryDeviceTest {
    @get:Rule val compose = createComposeRule()
    private val context get() = InstrumentationRegistry.getInstrumentation().targetContext
    private lateinit var server: MockWebServer
    @Volatile private var failSummary = false

    @Before fun setup() {
        check(context.packageName.endsWith(".verification"))
        server = MockWebServer()
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse = when {
                request.path?.startsWith("/api/dashboard/cash-flow-summary") == true -> {
                    if (failSummary) MockResponse().setResponseCode(503)
                    else MockResponse().setResponseCode(200).setBody("""{"ok":true,"period":"today","timezone":"Asia/Jakarta","currency":"IDR","start":"2026-10-29T17:00:00+00:00","end":"2026-10-30T03:00:00+00:00","generated_at":"2026-10-30T03:00:01+00:00","inflow":5000000,"outflow":300000,"net":4700000,"gross_outflow":300000,"reimbursement_offset":0,"reimbursement_unallocated":0,"reimbursement_excess":0}""")
                }
                request.path == "/api/accounts" -> MockResponse().setBody("""{"ok":true,"accounts":[]}""")
                request.path == "/api/categories" -> MockResponse().setBody("""{"ok":true,"categories":[]}""")
                else -> MockResponse().setResponseCode(404)
            }
        }
        server.start()
        TestEndpoint.set(context, server.url("/").toString())
        AppPreferences(context).apiKey = "synthetic-verification"
    }

    @After fun close() {
        AppPreferences(context).apiKey = ""
        server.shutdown()
    }

    @Test fun serverAmountIsTheOnlyMoneyAndTheLocalNotificationIsOnlyCounted(): Unit = runBlocking {
        val dao = AppDatabase.getDatabase(context).notificationDao()
        val eventId = UUID.randomUUID().toString()
        val transactionId = UUID.randomUUID().toString()
        val id = dao.insertOrIgnore(RawNotificationEntity(
            deviceId = "synthetic", packageName = "com.example.synthetic", appLabel = "Synthetic",
            title = "Synthetic expense", bodyText = "Synthetic expense", postTime = System.currentTimeMillis(),
            payloadHash = UUID.randomUUID().toString(), isSynced = true, processingStatus = "recorded",
            recordKey = "synthetic:$eventId", backendEventId = eventId,
            pairingContext = AppPreferences(context).pairingContext, isFinancial = true,
            resultJson = """{"event_id":"$eventId","status":"recorded","record_key":"synthetic:$eventId","transaction_id":"$transactionId","type":"expense","description":"Synthetic expense","source":"Synthetic account","amount":300000,"currency":"IDR"}"""
        ))
        try {
            val model = CompanionViewModel(context.applicationContext as Application)
            compose.setContent {
                CompanionTheme(dark = false) {
                    HomeRoute(model, listenerPermissionGranted = true, onOpenQuestions = {}, onOpenActivity = {}, onOpenSettings = {})
                }
            }
            compose.waitUntil(10_000) { model.ledgerSummary.value.summary != null && model.lists.value.visible.any { it.id == id } }
            compose.onNodeWithContentDescription("Uang keluar tracker Rp300.000").assertExists()
            compose.onNodeWithContentDescription("Aktivitas ponsel hari ini", substring = true).assertExists()
            compose.onNodeWithContentDescription("notifikasi tertangkap belum dimuat", substring = true).assertDoesNotExist()
            compose.onNodeWithContentDescription("Keluar tercatat hari ini", substring = true).assertDoesNotExist()
            compose.onNodeWithContentDescription("Uang keluar tracker Rp600.000").assertDoesNotExist()
        } finally {
            dao.deleteById(id)
        }
    }

    @Test fun summaryShowsServerAmountsAndStaleStateAfterRefreshFailure() {
        val model = CompanionViewModel(context.applicationContext as Application)
        compose.setContent {
            CompanionTheme(dark = false) {
                HomeRoute(model, listenerPermissionGranted = true, onOpenQuestions = {}, onOpenActivity = {}, onOpenSettings = {})
            }
        }
        compose.waitUntil(10_000) { model.ledgerSummary.value.summary != null }
        compose.onNodeWithContentDescription("Uang keluar tracker Rp300.000").assertIsDisplayed()
        compose.onNodeWithContentDescription("Masuk Rp5.000.000").assertExists()
        compose.onNodeWithContentDescription("Neto Rp4.700.000").assertExists()
        failSummary = true
        compose.onNodeWithText("Perbarui arus kas").performScrollTo().performClick()
        compose.waitUntil(10_000) { model.ledgerSummary.value.stale }
        compose.onNodeWithText("Belum diperbarui", substring = true).performScrollTo().assertIsDisplayed()
        compose.onNodeWithContentDescription("Uang keluar tracker Rp300.000").assertExists()
    }
}
