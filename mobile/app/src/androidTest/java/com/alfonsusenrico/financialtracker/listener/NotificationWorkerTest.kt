package com.alfonsusenrico.financialtracker.listener

import android.app.NotificationManager
import android.content.Context
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import androidx.work.Data
import androidx.work.testing.TestListenableWorkerBuilder
import androidx.work.WorkManager
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.model.RawNotificationEntity
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.data.sync.NotificationCompletionWorker
import com.alfonsusenrico.financialtracker.listener.data.sync.NotificationSyncWorker
import kotlinx.coroutines.runBlocking
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import java.util.UUID
import java.util.concurrent.TimeUnit

@RunWith(AndroidJUnit4::class)
class NotificationWorkerTest {
    private val context = InstrumentationRegistry.getInstrumentation().targetContext
    private val dao = AppDatabase.getDatabase(context).notificationDao()
    private val prefs = AppPreferences(context)
    private val rows = mutableListOf<Long>()
    private val server = MockWebServer()

    @Before fun setup() {
        check(context.packageName.endsWith(".verification")) { "Use the separate verification app ID" }
        server.start()
        prefs.isAutoSyncEnabled = false
        TestEndpoint.set(context, server.url("/").toString())
        prefs.apiKey = "fictional-test-key"
    }

    @After fun cleanup() = runBlocking {
        WorkManager.getInstance(context).cancelAllWork().result.get(10, TimeUnit.SECONDS)
        rows.forEach { dao.deleteById(it) }
        prefs.apiKey = ""
        server.shutdown()
        val manager = context.getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager
        manager.activeNotifications.filter { it.tag?.startsWith("notification:fictional") == true }
            .forEach { manager.cancel(it.tag, it.id) }
    }

    private suspend fun addRow(hash: String): Long {
        val id = dao.insertOrIgnore(RawNotificationEntity(
            deviceId = "fictional-device", packageName = "com.jago.digitalbanking", appLabel = "Bank Jago",
            postTime = 1000, payloadHash = hash, title = "Jago",
            bodyText = "You've paid Rp125.000 to Kedai Awan", isFinancial = true,
            expectedAmount = 125000.0, expectedDirection = "out", eventClass = "expense"
        ))
        rows.add(id)
        return id
    }

    private fun completionWorker(event: String) = TestListenableWorkerBuilder<NotificationCompletionWorker>(
        context, inputData = Data.Builder().putString(NotificationCompletionWorker.EVENT_ID, event).build()
    ).build()

    @Test fun uploadQueuedThenResultLookupRecordsWithoutResubmitting() = runBlocking {
        val hash = "fictional-worker-${UUID.randomUUID()}"
        val event = UUID.randomUUID().toString()
        val transaction = UUID.randomUUID().toString()
        val id = addRow(hash)
        server.enqueue(MockResponse().setBody("""{"ok":true,"results":[{"payload_hash":"$hash","event_id":"$event","status":"queued"}]}"""))
        TestListenableWorkerBuilder<NotificationSyncWorker>(context).build().doWork()
        val submitted = server.takeRequest(5, TimeUnit.SECONDS)!!
        assertEquals("/api/ingest/notifications", submitted.path)
        assertEquals("Bearer fictional-test-key", submitted.getHeader("Authorization"))
        val queued = dao.getById(id)!!
        assertTrue(queued.isSynced)
        assertEquals("queued", queued.processingStatus)
        assertEquals(event, queued.backendEventId)
        assertNull(queued.editableTransactionId)
        dao.updateCheckFailure(id, prefs.pairingContext, "queued", "", 0, 0)
        server.enqueue(MockResponse().setBody("""{"ok":true,"result":{"payload_hash":"$hash","event_id":"$event","status":"recorded","record_key":"notification:fictional-$event","type":"expense","description":"Kedai Awan","source":"Bank Jago","amount":125000,"currency":"IDR","transaction_id":"$transaction"}}"""))
        completionWorker(event).doWork()
        assertEquals("/api/ingest/notifications/$event/result", server.takeRequest(5, TimeUnit.SECONDS)!!.path)
        val recorded = dao.getById(id)!!
        assertEquals("recorded", recorded.processingStatus)
        assertEquals(transaction, recorded.editableTransactionId)
        assertNull(recorded.nextCheckAt)
        completionWorker(event).doWork()
        assertNull(server.takeRequest(100, TimeUnit.MILLISECONDS))
    }

    @Test fun transientFailureRetainsEventAndNotFoundSuspendsLookupWithoutReplay() = runBlocking {
        val hash = "fictional-recovery-${UUID.randomUUID()}"
        val event = UUID.randomUUID().toString()
        val id = addRow(hash)
        dao.saveCompletion(id, true, 1000, event, "queued", null, null, null, prefs.pairingContext, 0, 0, null)
        server.enqueue(MockResponse().setResponseCode(503).setBody("fictional-private-body"))
        completionWorker(event).doWork()
        server.takeRequest(5, TimeUnit.SECONDS)
        val deferred = dao.getById(id)!!
        assertEquals("queued", deferred.processingStatus)
        assertEquals("server_unavailable", deferred.processingError)
        assertTrue(deferred.nextCheckAt!! > System.currentTimeMillis())
        assertEquals(event, deferred.backendEventId)
        dao.updateCheckFailure(id, prefs.pairingContext, "queued", "", 0, 0)
        server.enqueue(MockResponse().setResponseCode(404))
        completionWorker(event).doWork()
        server.takeRequest(5, TimeUnit.SECONDS)
        val suspended = dao.getById(id)!!
        assertEquals("failed", suspended.processingStatus)
        assertEquals("event_not_found", suspended.processingError)
        assertNull(suspended.nextCheckAt)
        completionWorker(event).doWork()
        assertNull(server.takeRequest(100, TimeUnit.MILLISECONDS))
        assertTrue(dao.getUnsyncedNotifications(prefs.pairingContext).isEmpty())
    }

    @Test fun partialReorderedBatchOnlyAcceptsMatchedResults() = runBlocking {
        val first = "fictional-partial-${UUID.randomUUID()}"
        val second = "fictional-partial-${UUID.randomUUID()}"
        val firstId = addRow(first)
        val secondId = addRow(second)
        val event = UUID.randomUUID().toString()
        server.enqueue(MockResponse().setBody("""{"ok":false,"results":[{"payload_hash":"$second","event_id":"$event","status":"needs_review","error_code":"uncertain_mapping"}]}"""))
        TestListenableWorkerBuilder<NotificationSyncWorker>(context).build().doWork()
        server.takeRequest(5, TimeUnit.SECONDS)
        assertFalse(dao.getById(firstId)!!.isSynced)
        assertEquals("missing_result", dao.getById(firstId)!!.syncError)
        assertTrue(dao.getById(secondId)!!.isSynced)
        assertEquals("needs_review", dao.getById(secondId)!!.processingStatus)
        assertNull(dao.getById(secondId)!!.nextCheckAt)
    }
}
