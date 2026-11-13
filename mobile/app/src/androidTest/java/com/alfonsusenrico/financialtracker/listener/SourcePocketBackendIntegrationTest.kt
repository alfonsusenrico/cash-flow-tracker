package com.alfonsusenrico.financialtracker.listener

import android.Manifest
import android.app.KeyguardManager
import android.app.NotificationManager
import android.content.Intent
import android.os.ParcelFileDescriptor
import android.view.accessibility.AccessibilityNodeInfo
import android.view.WindowManager
import androidx.test.platform.app.InstrumentationRegistry
import androidx.work.Data
import androidx.work.WorkManager
import androidx.work.testing.TestListenableWorkerBuilder
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.model.RawNotificationEntity
import com.alfonsusenrico.financialtracker.listener.data.network.ApiClient
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.data.sync.*
import com.alfonsusenrico.financialtracker.listener.service.SourcePocketConfirmationNotifier
import com.google.gson.Gson
import com.google.gson.JsonParser
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.junit.Assert.*
import org.junit.Assume.assumeFalse
import org.junit.Assume.assumeTrue
import org.junit.Test
import java.util.UUID
import java.util.concurrent.TimeUnit

class SourcePocketBackendIntegrationTest {
    private val instrumentation = InstrumentationRegistry.getInstrumentation()
    private val context = instrumentation.targetContext
    private val db = AppDatabase.getDatabase(context)
    private val dao = db.notificationDao()
    private val prefs = AppPreferences(context)
    private val manager = context.getSystemService(NotificationManager::class.java)
    private val phase = context.getSharedPreferences("fictional_pocket_verification", 0)

    private suspend fun requireLocal() {
        assumeTrue(InstrumentationRegistry.getArguments().getString("localBackend") == "true")
        check(context.packageName.endsWith(".verification"))
        assumeFalse(context.getSystemService(KeyguardManager::class.java).isDeviceLocked)
        prefs.didRequestBatteryExempt = true
        prefs.isAutoSyncEnabled = false
        TestEndpoint.set(context, "http://127.0.0.1:18000")
        prefs.apiKey = "fictional-jago-phone-key"
        ParcelFileDescriptor.AutoCloseInputStream(instrumentation.uiAutomation.executeShellCommand(
            "pm grant ${context.packageName} ${Manifest.permission.POST_NOTIFICATIONS}"
        )).use { it.readBytes() }
        val activity = instrumentation.startActivitySync(
            Intent(context, MainActivity::class.java).addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
        )
        instrumentation.runOnMainSync { activity.window.addFlags(WindowManager.LayoutParams.FLAG_KEEP_SCREEN_ON) }
        delay(300)
        clickIfVisible("Lewati & Buka Aplikasi")
    }

    private suspend fun fixture(path: String = "jago-state", post: Boolean = false) = withContext(Dispatchers.IO) {
        val request = Request.Builder().url("http://127.0.0.1:18000/verification/$path")
            .header("Authorization", "Bearer fictional-jago-phone-key")
        if (post) request.post("".toRequestBody())
        OkHttpClient().newCall(request.build()).execute().use { response ->
            check(response.isSuccessful) { "Fictional verification request failed" }
            JsonParser.parseString(response.body!!.string()).asJsonObject
        }
    }

    private suspend fun held(): RawNotificationEntity {
        val id = dao.insertOrIgnore(RawNotificationEntity(deviceId = "fictional-jago", packageName = "com.jago.digitalBanking",
            appLabel = "Bank Jago", bodyText = "You've transferred Rp112.590 to Andra. Need help?",
            postTime = System.currentTimeMillis(), payloadHash = "fictional-jago-${UUID.randomUUID()}", sourceVersion = BuildConfig.VERSION_NAME))
        TestListenableWorkerBuilder<NotificationSyncWorker>(context).build().doWork()
        val row = waitFor(id, "needs_confirmation")
        cacheOptions(row)
        return dao.getById(id)!!
    }

    private suspend fun cacheOptions(row: RawNotificationEntity) {
        val item = SourcePocketQuestions.parse(row.resultJson)!!
        val options = ApiClient(context).createService().getSourcePockets(row.backendEventId!!).body()!!.result!!
        assertTrue(SourcePocketQuestions.validOptions(options, item))
        assertTrue(options.options!!.size >= 2)
        dao.saveSourcePocketOptions(row.id, prefs.pairingContext, row.backendEventId, Gson().toJson(options))
    }

    private suspend fun waitFor(id: Long, status: String): RawNotificationEntity {
        repeat(80) {
            val row = dao.getById(id)!!
            row.backendEventId?.let { event ->
                ApiClient(context).createService().getNotificationResult(event).body()?.result?.let { item ->
                    val results = NotificationResults(context)
                    results.consume(row, item, prefs.pairingContext)
                    results.deliverPendingAlerts()
                }
            }
            val saved = dao.getById(id)!!
            if (saved.processingStatus == status) return saved
            delay(250)
        }
        fail("Fictional pocket event did not reach $status")
        error("unreachable")
    }

    private suspend fun deliver(replyId: String) {
        TestListenableWorkerBuilder<SenderReplyWorker>(context,
            inputData = Data.Builder().putString(SenderReplyWorker.REPLY_ID, replyId).build()).build().doWork()
    }

    private suspend fun click(text: String) {
        repeat(60) {
            clickIfVisible("Lewati & Buka Aplikasi")
            if (clickIfVisible(text)) return
            delay(100)
        }
        fail("Fictional chooser action unavailable: $text")
    }

    private fun clickIfVisible(text: String): Boolean {
        fun find(node: AccessibilityNodeInfo?): AccessibilityNodeInfo? {
            if (node == null) return null
            if (node.text?.contains(text) == true || node.contentDescription?.contains(text) == true) return node
            for (index in 0 until node.childCount) find(node.getChild(index))?.let { return it }
            return null
        }
        var target = find(instrumentation.uiAutomation.rootInActiveWindow)
        while (target != null && !target.isClickable) target = target.parent
        return target?.performAction(AccessibilityNodeInfo.ACTION_CLICK) == true
    }

    @Test fun notificationActionOpensChooserAndRecordsNonMainPocketOnce(): Unit = runBlocking {
        requireLocal()
        val pair = prefs.pairingContext
        val before = fixture().getAsJsonArray("records").size()
        val row = held()
        try {
            val item = SourcePocketQuestions.parse(row.resultJson)!!
            val options = SourcePocketQuestions.cached(row.sourcePocketOptionsJson, item)!!
            assertEquals(3, options.options!!.size)
            assertEquals(before, fixture().getAsJsonArray("records").size())
            val notification = manager.activeNotifications.single { it.tag == "source-pocket:${row.backendEventId}" }.notification
            assertEquals(androidx.core.app.NotificationCompat.VISIBILITY_PRIVATE, notification.visibility)
            assertEquals("Pilih sumber dana", notification.actions.single().title.toString())
            notification.actions.single().actionIntent.send()
            delay(300)
            clickIfVisible("Lewati & Buka Aplikasi")
            click("Bank Jago · Dana Darurat")
            click("Konfirmasi sumber dana")
            val recorded = waitFor(row.id, "recorded")
            val result = Gson().fromJson(recorded.resultJson, com.alfonsusenrico.financialtracker.listener.data.network.model.IngestResultItem::class.java)
            assertEquals("Bank Jago · Dana Darurat", result.source)
            assertEquals(before + 1, fixture().getAsJsonArray("records").size())
            assertEquals(pair, prefs.pairingContext)
        } finally { cleanup(row) }
    }

    @Test fun staleCachedPocketIsRejectedThenInboxAnswerUsesCurrentOptions(): Unit = runBlocking {
        requireLocal()
        val state = fixture()
        val before = state.getAsJsonArray("records").size()
        val row = held()
        val question = SourcePocketQuestions.parse(row.resultJson)!!.confirmation!!.questionId!!
        val extra = state.get("extra_id").asString
        try {
            // No OS alert is needed to answer the durable inbox question.
            SourcePocketConfirmationNotifier.cancel(context, row.backendEventId!!)
            fixture("jago-pocket/$extra/archive", true)
            assertTrue(SourcePocketReplies(context, db) { _, _ -> }.enqueue(row.id, question, extra).queued)
            val reply = db.senderReplyDao().forQuestion(prefs.pairingContext, row.backendEventId, question)!!
            deliver(reply.replyId)
            assertEquals("stale", db.senderReplyDao().get(reply.replyId)!!.state)
            assertNull(dao.getById(row.id)!!.sourcePocketOptionsJson)
            assertEquals(before, fixture().getAsJsonArray("records").size())
            cacheOptions(dao.getById(row.id)!!)
            val main = state.get("main_id").asString
            assertTrue(SourcePocketReplies(context, db) { _, _ -> }.enqueue(row.id, question, main).queued)
            deliver(db.senderReplyDao().forQuestion(prefs.pairingContext, row.backendEventId, question)!!.replyId)
            val recorded = waitFor(row.id, "recorded")
            assertEquals("Bank Jago · Kantong Utama", Gson().fromJson(recorded.resultJson,
                com.alfonsusenrico.financialtracker.listener.data.network.model.IngestResultItem::class.java).source)
            assertEquals(before + 1, fixture().getAsJsonArray("records").size())
        } finally { cleanup(row) }
    }

    @Test fun preparePocketAnswerForOfflineRestart(): Unit = runBlocking {
        requireLocal()
        val state = fixture()
        val row = held()
        WorkManager.getInstance(context).cancelAllWork().result.get(10, TimeUnit.SECONDS)
        val question = SourcePocketQuestions.parse(row.resultJson)!!.confirmation!!.questionId!!
        val account = state.get("emergency_id").asString
        assertTrue(SourcePocketReplies(context, db) { id, _ -> SyncScheduler.enqueueSenderReply(context, id, 300000) }
            .enqueue(row.id, question, account).queued)
        val reply = db.senderReplyDao().forQuestion(prefs.pairingContext, row.backendEventId!!, question)!!
        phase.edit().putString("reply_id", reply.replyId).putString("account_id", account)
            .putString("pairing", prefs.pairingContext).putInt("records_before", state.getAsJsonArray("records").size()).commit()
    }

    @Test fun verifyPocketAnswerPersistsAfterOfflineRestart(): Unit = runBlocking {
        requireLocal()
        val reply = db.senderReplyDao().get(phase.getString("reply_id", null)!!)!!
        assertEquals("source_pocket", reply.answerKind)
        assertEquals(phase.getString("account_id", null), reply.accountId)
        assertEquals(phase.getString("pairing", null), prefs.pairingContext)
        deliver(reply.replyId)
        assertEquals("queued", db.senderReplyDao().get(reply.replyId)!!.state)
        assertEquals("network_unavailable", db.senderReplyDao().get(reply.replyId)!!.errorCode)
        assertEquals("needs_confirmation", dao.getById(reply.rowId)!!.processingStatus)
    }

    @Test fun resumePocketAnswerAfterConnectionRestored(): Unit = runBlocking {
        requireLocal()
        val reply = db.senderReplyDao().get(phase.getString("reply_id", null)!!)!!
        deliver(reply.replyId)
        val recorded = waitFor(reply.rowId, "recorded")
        assertEquals("accepted", db.senderReplyDao().get(reply.replyId)!!.state)
        assertEquals(phase.getInt("records_before", -100) + 1, fixture().getAsJsonArray("records").size())
        assertEquals("Bank Jago · Dana Darurat", Gson().fromJson(recorded.resultJson,
            com.alfonsusenrico.financialtracker.listener.data.network.model.IngestResultItem::class.java).source)
        cleanup(recorded)
        phase.edit().clear().commit()
    }

    private suspend fun cleanup(row: RawNotificationEntity) {
        WorkManager.getInstance(context).cancelAllWork().result.get(10, TimeUnit.SECONDS)
        row.backendEventId?.let { SourcePocketConfirmationNotifier.cancel(context, it) }
        dao.getById(row.id)?.recordKey?.let { manager.cancel(it, it.hashCode() and Int.MAX_VALUE) }
        dao.deleteById(row.id)
    }
}
