package com.alfonsusenrico.financialtracker.listener

import android.app.Application
import android.content.Context
import android.util.Log
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.work.Configuration
import androidx.work.Data
import androidx.work.testing.SynchronousExecutor
import androidx.work.testing.TestListenableWorkerBuilder
import androidx.work.testing.WorkManagerTestInitHelper
import com.alfonsusenrico.financialtracker.listener.PurposeFixtures.EVENT
import com.alfonsusenrico.financialtracker.listener.PurposeFixtures.HASH
import com.alfonsusenrico.financialtracker.listener.PurposeFixtures.ignoredJson
import com.alfonsusenrico.financialtracker.listener.PurposeFixtures.resultJson
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.model.RawNotificationEntity
import com.alfonsusenrico.financialtracker.listener.data.network.model.IngestResultItem
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.data.sync.NotificationCompletionWorker
import com.alfonsusenrico.financialtracker.listener.data.sync.NotificationResults
import com.google.gson.Gson
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
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

/** A refresh of a settled review row applies the server's newer outcome and survives failures. */
@RunWith(AndroidJUnit4::class)
@Config(sdk = [34], application = TestApplication::class)
class ReviewRefreshWorkerTest {
    private val context = ApplicationProvider.getApplicationContext<Context>()
    private lateinit var db: AppDatabase
    private lateinit var server: MockWebServer
    private lateinit var pairing: String
    private var rowId = 0L

    @Before fun open() = runBlocking {
        WorkManagerTestInitHelper.initializeTestWorkManager(
            context, Configuration.Builder().setMinimumLoggingLevel(Log.DEBUG).setExecutor(SynchronousExecutor()).build()
        )
        server = MockWebServer().apply { start() }
        AppPreferences(context).apply {
            TestEndpoint.set(context, server.url("/").toString())
            apiKey = "fictional-review-refresh-key"
        }
        pairing = AppPreferences(context).pairingContext
        // The worker opens the app's database, so the test uses it too and starts from empty tables.
        db = AppDatabase.getDatabase(context)
        withContext(Dispatchers.IO) { db.clearAllTables() }
        rowId = db.notificationDao().insertOrIgnore(RawNotificationEntity(
            deviceId = "fictional", packageName = "com.gojek.app", appLabel = "Gojek", postTime = 1,
            title = "Promo", bodyText = "Diskon ongkir hari ini", payloadHash = HASH
        ))
        val review = Gson().fromJson(
            """{"payload_hash": "$HASH", "event_id": "$EVENT", "status": "needs_review", "error_code": "fictional_noise"}""",
            IngestResultItem::class.java
        )
        assertTrue(NotificationResults(context, db).consume(db.notificationDao().getById(rowId)!!, review, pairing))
        assertEquals("needs_review", row().processingStatus)
    }

    @After fun close() {
        server.shutdown()
        AppPreferences(context).apiKey = ""
    }

    private suspend fun row() = db.notificationDao().getById(rowId)!!

    private suspend fun refresh() = TestListenableWorkerBuilder<NotificationCompletionWorker>(context)
        .setInputData(Data.Builder().putString(NotificationCompletionWorker.EVENT_ID, EVENT)
            .putBoolean(NotificationCompletionWorker.REFRESH, true).build())
        .build().doWork()

    private suspend fun visible() = db.notificationDao().getAllNotificationsFlow().first().map { it.id }

    @Test fun ignoredOnTheServerBecomesIgnoredAndLeavesTheList() = runBlocking {
        assertEquals(listOf(rowId), visible())
        server.enqueue(MockResponse().setBody("""{"ok": true, "result": ${ignoredJson()}}"""))
        refresh()
        assertEquals("ignored", row().processingStatus)
        assertNull(row().processingError)
        assertTrue(visible().isEmpty())
        assertEquals(listOf(rowId), db.notificationDao().getIgnoredNotificationsFlow().first().map { it.id })
    }

    @Test fun recordedOnTheServerBecomesRecordedWithItsResult() = runBlocking {
        server.enqueue(MockResponse().setBody("""{"ok": true, "result": ${resultJson(question = null)}}"""))
        refresh()
        assertEquals("recorded", row().processingStatus)
        assertNull(row().processingError)
        assertTrue(row().resultJson!!.contains("Pengeluaran Groceries"))
    }

    @Test fun unchangedReviewStaysAsItIs() = runBlocking {
        server.enqueue(MockResponse().setBody(
            """{"ok": true, "result": {"payload_hash": "$HASH", "event_id": "$EVENT", "status": "needs_review", "error_code": "fictional_noise"}}"""
        ))
        refresh()
        assertEquals("needs_review", row().processingStatus)
        assertEquals("fictional_noise", row().processingError)
        assertNull(row().nextCheckAt)
    }

    @Test fun failedReadsLeaveTheReviewRowAsItIs() = runBlocking {
        for (code in listOf(401, 403, 404, 503)) {
            server.enqueue(MockResponse().setResponseCode(code).setBody("""{"ok": false}"""))
            refresh()
            assertEquals("needs_review", row().processingStatus)
            assertEquals("fictional_noise", row().processingError)
            assertNull(row().nextCheckAt)
            assertEquals(0, row().checkAttempt)
        }
    }
}
