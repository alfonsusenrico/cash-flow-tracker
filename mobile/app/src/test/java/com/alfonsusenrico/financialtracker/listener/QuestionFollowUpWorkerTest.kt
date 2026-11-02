package com.alfonsusenrico.financialtracker.listener

import android.Manifest
import android.app.Application
import android.app.NotificationManager
import android.content.Context
import android.util.Log
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.work.Configuration
import androidx.work.Data
import androidx.work.ListenableWorker
import androidx.work.WorkInfo
import androidx.work.WorkManager
import androidx.work.testing.SynchronousExecutor
import androidx.work.testing.TestListenableWorkerBuilder
import androidx.work.testing.WorkManagerTestInitHelper
import com.alfonsusenrico.financialtracker.listener.PurposeFixtures.EVENT
import com.alfonsusenrico.financialtracker.listener.PurposeFixtures.HASH
import com.alfonsusenrico.financialtracker.listener.PurposeFixtures.QUESTION
import com.alfonsusenrico.financialtracker.listener.PurposeFixtures.result
import com.alfonsusenrico.financialtracker.listener.PurposeFixtures.resultJson
import com.alfonsusenrico.financialtracker.listener.PurposeFixtures.sourcePocketJson
import com.alfonsusenrico.financialtracker.listener.data.network.model.IngestResultItem
import com.alfonsusenrico.financialtracker.listener.service.SourcePocketConfirmationNotifier
import com.google.gson.Gson
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.model.RawNotificationEntity
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.data.sync.NotificationCompletionWorker
import com.alfonsusenrico.financialtracker.listener.data.sync.NotificationResults
import com.alfonsusenrico.financialtracker.listener.data.sync.QuestionRefresh
import com.alfonsusenrico.financialtracker.listener.data.sync.SyncScheduler
import com.alfonsusenrico.financialtracker.listener.service.PurposeQuestionNotifier
import kotlinx.coroutines.Dispatchers
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
import org.robolectric.Shadows.shadowOf
import org.robolectric.annotation.Config
import java.util.concurrent.TimeUnit

/** The completion worker as the follow-up check of an open question, against a fictional backend. */
@RunWith(AndroidJUnit4::class)
@Config(sdk = [34], application = TestApplication::class)
class QuestionFollowUpWorkerTest {
    private val context = ApplicationProvider.getApplicationContext<Context>()
    private val manager = context.getSystemService(NotificationManager::class.java)
    private val recordKey = "notification:$EVENT"
    private val slot = recordKey.hashCode() and Int.MAX_VALUE
    private lateinit var db: AppDatabase
    private lateinit var server: MockWebServer
    private lateinit var pairing: String
    private var rowId = 0L

    @Before fun open() = runBlocking {
        WorkManagerTestInitHelper.initializeTestWorkManager(
            context, Configuration.Builder().setMinimumLoggingLevel(Log.DEBUG).setExecutor(SynchronousExecutor()).build()
        )
        shadowOf(context as Application).grantPermissions(Manifest.permission.POST_NOTIFICATIONS)
        server = MockWebServer().apply { start() }
        AppPreferences(context).apply {
            TestEndpoint.set(context, server.url("/").toString())
            apiKey = "fictional-follow-up-key"
            // Follow-ups of a prompted question; light questions are prompted at once with the evening reminder off.
            isEveningReminderEnabled = false
        }
        pairing = AppPreferences(context).pairingContext
        // The worker opens the app's database, so the test uses it too and starts from empty tables.
        db = AppDatabase.getDatabase(context)
        withContext(Dispatchers.IO) { db.clearAllTables() }
        rowId = db.notificationDao().insertOrIgnore(RawNotificationEntity(
            deviceId = "fictional", packageName = "com.bca.mybca.omni.android", appLabel = "myBCA", postTime = 1,
            title = "Financial Diary", payloadHash = HASH
        ))
        val results = NotificationResults(context, db, scheduleFollowUp = { _, _ -> })
        assertTrue(results.consume(db.notificationDao().getById(rowId)!!, result(), pairing))
        results.deliverPendingAlerts(pairing)
        assertEquals(PurposeQuestionNotifier.CHANNEL_ID, shadowOf(manager).getNotification(recordKey, slot).channelId)
        dueNow()
    }

    @After fun close() {
        server.shutdown()
        AppPreferences(context).apiKey = ""
        AppPreferences(context).isEveningReminderEnabled = true
    }

    private suspend fun row() = db.notificationDao().getById(rowId)!!

    private suspend fun dueNow(at: Long = 0, attempt: Int = 0) {
        val stored = row()
        assertEquals(1, db.notificationDao().updateQuestionCheck(rowId, pairing, stored.processingStatus!!, stored.resultJson, at, attempt))
    }

    private suspend fun runWorker(input: String = NotificationCompletionWorker.QUESTION_FOLLOW_UP): ListenableWorker.Result =
        TestListenableWorkerBuilder<NotificationCompletionWorker>(context)
            .setInputData(Data.Builder().putString(NotificationCompletionWorker.EVENT_ID, EVENT).putBoolean(input, true).build())
            .build().doWork()

    private fun followUpStates() = WorkManager.getInstance(context)
        .getWorkInfosForUniqueWork(SyncScheduler.questionFollowUpWork(EVENT)).get().map { it.state }

    private fun closedResult() = MockResponse().setBody(
        """{"ok": true, "result": ${resultJson(question = null, description = "makan kafe senja", reviewStatus = "reviewed")}}"""
    )

    @Test fun closedOnTheServerClearsTheRowAndReplacesThePromptQuietly() = runBlocking {
        server.enqueue(closedResult())
        assertEquals(ListenableWorker.Result.success(), runWorker())

        assertEquals("/api/ingest/notifications/$EVENT/result", server.takeRequest(5, TimeUnit.SECONDS)?.path)
        assertNull(row().purposeQuestionId)
        assertNull(row().nextCheckAt)
        assertEquals("recorded", row().processingStatus)
        val shown = shadowOf(manager).getNotification(recordKey, slot)
        assertEquals("transaction_alerts", shown.channelId)
        assertTrue(followUpStates().none { it == WorkInfo.State.ENQUEUED })
    }

    @Test fun dismissedPromptStaysGoneWhenTheQuestionCloses() = runBlocking {
        PurposeQuestionNotifier.cancel(context, recordKey)
        server.enqueue(closedResult())
        runWorker()
        assertNull(row().purposeQuestionId)
        assertNull(shadowOf(manager).getNotification(recordKey, slot))
    }

    @Test fun stillOpenSchedulesTheNextStep() = runBlocking {
        server.enqueue(MockResponse().setBody("""{"ok": true, "result": ${resultJson()}}"""))
        val before = System.currentTimeMillis()
        runWorker()
        assertEquals(QUESTION, row().purposeQuestionId)
        assertEquals(1, row().checkAttempt)
        val next = row().nextCheckAt!!
        assertTrue(next - before >= QuestionRefresh.delay(1) && next - System.currentTimeMillis() <= QuestionRefresh.delay(1))
        assertEquals(listOf(WorkInfo.State.ENQUEUED), followUpStates())
        assertEquals(PurposeQuestionNotifier.CHANNEL_ID, shadowOf(manager).getNotification(recordKey, slot).channelId)
    }

    @Test fun failedChecksNeverMarkTheRecordFailed() = runBlocking {
        server.enqueue(MockResponse().setResponseCode(503))
        runWorker()
        assertEquals("recorded", row().processingStatus)
        assertEquals(QUESTION, row().purposeQuestionId)
        assertEquals(1, row().checkAttempt)
        assertEquals(listOf(WorkInfo.State.ENQUEUED), followUpStates())

        // A refresh the owner starts (opening the app) is held to the same rule.
        server.enqueue(MockResponse().setResponseCode(401))
        runWorker(NotificationCompletionWorker.REFRESH)
        assertEquals("recorded", row().processingStatus)
        assertNull(row().processingError)
        assertEquals(QUESTION, row().purposeQuestionId)
        assertNull(row().nextCheckAt)
    }

    @Test fun confirmationAnsweredOnTheServerClearsItsNotification() = runBlocking {
        // Start over: the purpose row's delivered alert receipt would otherwise suppress the record alert.
        withContext(Dispatchers.IO) { db.clearAllTables() }
        PurposeQuestionNotifier.cancel(context, recordKey)
        rowId = db.notificationDao().insertOrIgnore(RawNotificationEntity(
            deviceId = "fictional", packageName = "com.jago.digitalBanking", appLabel = "Jago", postTime = 2,
            title = "Transfer keluar", payloadHash = HASH
        ))
        val results = NotificationResults(context, db, scheduleFollowUp = { _, _ -> })
        val pending = Gson().fromJson(sourcePocketJson(), IngestResultItem::class.java)
        assertTrue(results.consume(row(), pending, pairing))
        val channels = { shadowOf(manager).allNotifications.map { it.channelId } }
        assertEquals(listOf(SourcePocketConfirmationNotifier.CHANNEL_ID), channels())
        dueNow()

        server.enqueue(MockResponse().setBody("""{"ok": true, "result": ${resultJson(question = null, description = "Pindah ke Kantong Harian")}}"""))
        assertEquals(ListenableWorker.Result.success(), runWorker())

        assertEquals("recorded", row().processingStatus)
        assertNull(row().nextCheckAt)
        assertEquals(listOf("transaction_alerts"), channels())
    }

    @Test fun refreshFindsTheQuestionClosed() = runBlocking {
        dueNow(at = Long.MAX_VALUE, attempt = 3)
        server.enqueue(closedResult())
        runWorker(NotificationCompletionWorker.REFRESH)
        assertNull(row().purposeQuestionId)
        assertEquals("transaction_alerts", shadowOf(manager).getNotification(recordKey, slot).channelId)
    }
}
