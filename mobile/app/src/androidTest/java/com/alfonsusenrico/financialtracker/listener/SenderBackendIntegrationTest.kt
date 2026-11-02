package com.alfonsusenrico.financialtracker.listener

import android.Manifest
import android.app.KeyguardManager
import android.app.NotificationManager
import android.content.Intent
import android.os.Bundle
import android.os.ParcelFileDescriptor
import androidx.core.app.RemoteInput
import androidx.test.platform.app.InstrumentationRegistry
import androidx.work.Data
import androidx.work.WorkManager
import androidx.work.testing.TestListenableWorkerBuilder
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.model.RawNotificationEntity
import com.alfonsusenrico.financialtracker.listener.data.network.ApiClient
import com.alfonsusenrico.financialtracker.listener.data.network.model.IngestResultItem
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.data.sync.*
import com.alfonsusenrico.financialtracker.listener.receiver.SenderReplyReceiver
import com.alfonsusenrico.financialtracker.listener.service.SenderConfirmationNotifier
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
import org.junit.Assume.*
import org.junit.Test
import java.util.UUID
import java.util.concurrent.TimeUnit

class SenderBackendIntegrationTest {
    private val instrumentation = InstrumentationRegistry.getInstrumentation()
    private val context = instrumentation.targetContext
    private val db = AppDatabase.getDatabase(context)
    private val dao = db.notificationDao()
    private val prefs = AppPreferences(context)
    private val manager = context.getSystemService(NotificationManager::class.java)
    private val phase = context.getSharedPreferences("fictional_sender_verification", 0)

    private fun requireLocal() {
        assumeTrue(InstrumentationRegistry.getArguments().getString("localBackend") == "true")
        check(context.packageName.endsWith(".verification"))
        assumeFalse(context.getSystemService(KeyguardManager::class.java).isDeviceLocked)
        prefs.didRequestBatteryExempt = true
        context.startActivity(Intent(context, MainActivity::class.java).addFlags(Intent.FLAG_ACTIVITY_NEW_TASK))
    }

    private fun permission(grant: Boolean) {
        ParcelFileDescriptor.AutoCloseInputStream(instrumentation.uiAutomation.executeShellCommand(
            "pm ${if (grant) "grant" else "revoke"} ${context.packageName} ${Manifest.permission.POST_NOTIFICATIONS}"
        )).use { it.readBytes() }
    }

    private suspend fun fixture(path: String, post: Boolean = false) = withContext(Dispatchers.IO) {
        val request = Request.Builder().url("http://127.0.0.1:18000/verification/$path")
            .header("Authorization", "Bearer fictional-phone-key")
        if (post) request.post("".toRequestBody())
        OkHttpClient().newCall(request.build()).execute().use { response ->
            check(response.isSuccessful) { "Fictional verification endpoint rejected request" }
            JsonParser.parseString(response.body!!.string()).asJsonObject
        }
    }

    private suspend fun add(mask: String, amount: Long = 112590): Long {
        val id = dao.insertOrIgnore(RawNotificationEntity(
            deviceId = "fictional-sender-phone", packageName = "com.bca.mybca.omni.android", appLabel = "myBCA",
            title = "Fictional transfer", bodyText = "You received IDR $amount.00 from $mask at Account Transfer category.",
            postTime = System.currentTimeMillis(), payloadHash = "fictional-sender-${UUID.randomUUID()}",
            sourceVersion = BuildConfig.VERSION_NAME,
            isFinancial = true, eventClass = "income", expectedAmount = amount.toDouble(), expectedDirection = "in"
        ))
        TestListenableWorkerBuilder<NotificationSyncWorker>(context).build().doWork()
        return id
    }

    private suspend fun waitFor(id: Long, status: String, questionType: String? = null): RawNotificationEntity {
        repeat(80) {
            val row = dao.getById(id)!!
            val event = row.backendEventId
            if (event != null) {
                val result = ApiClient(context).createService().getNotificationResult(event).body()?.result
                if (result != null) {
                    val results = NotificationResults(context)
                    results.consume(row, result, prefs.pairingContext)
                    results.deliverPendingAlerts()
                }
            }
            val saved = dao.getById(id)!!
            val item = Gson().fromJson(saved.resultJson, IngestResultItem::class.java)
            if (saved.processingStatus == status && (questionType == null || item?.confirmation?.type == questionType)) return saved
            delay(250)
        }
        val saved = dao.getById(id)!!
        val question = result(saved).confirmation?.questionId
        val reply = question?.let { db.senderReplyDao().forQuestion(saved.pairingContext!!, saved.backendEventId!!, it) }
        fail("Fictional event did not reach $status / $questionType; row=${saved.processingStatus}; reply=${reply?.state}/${reply?.errorCode}")
        error("unreachable")
    }

    private fun result(row: RawNotificationEntity) = Gson().fromJson(row.resultJson, IngestResultItem::class.java)

    private suspend fun notificationAnswer(row: RawNotificationEntity, name: String?) {
        repeat(40) {
            if (manager.activeNotifications.none { it.tag == "sender:${row.backendEventId}" }) delay(100)
        }
        val notification = manager.activeNotifications.single { it.tag == "sender:${row.backendEventId}" }.notification
        val fill = Intent()
        if (name != null) RemoteInput.addResultsToIntent(arrayOf(RemoteInput.Builder(SenderReplyReceiver.INPUT_NAME).build()), fill,
            Bundle().apply { putCharSequence(SenderReplyReceiver.INPUT_NAME, name) })
        notification.actions[if (name == null) 1 else 0].actionIntent.send(context, 0, fill)
        val question = result(row).confirmation!!.questionId!!
        repeat(40) {
            if (db.senderReplyDao().forQuestion(prefs.pairingContext, row.backendEventId!!, question) == null) delay(100)
        }
        assertNotNull("Direct action did not durably enqueue its answer", db.senderReplyDao().forQuestion(prefs.pairingContext, row.backendEventId!!, question))
    }

    @Test fun namedUnknownConcurrentAndSequentialFlowsRecordOnce(): Unit = runBlocking {
        requireLocal()
        prefs.isAutoSyncEnabled = false
        TestEndpoint.set(context, "http://127.0.0.1:18000")
        prefs.apiKey = "fictional-phone-key"
        permission(true)
        WorkManager.getInstance(context).cancelAllWork().result.get(10, TimeUnit.SECONDS)
        val ids = mutableListOf<Long>()
        val before = fixture("sender-state").getAsJsonArray("records").size()
        try {
            val first = add("AN**RA**").also { ids.add(it) }
            val held = waitFor(first, "needs_confirmation", "sender")
            assertEquals(before, fixture("sender-state").getAsJsonArray("records").size())
            assertFalse(SenderReplies(context, db, { _, _ -> fail("Stale reply scheduled") }, { _, _ -> })
                .enqueue(first, held.backendEventId!!, UUID.randomUUID().toString(), prefs.pairingContext, "name", "Andra").queued)
            notificationAnswer(held, "Andra")
            val named = waitFor(first, "recorded")
            assertEquals("Transfer masuk dari Andra", result(named).description)
            assertEquals(112590L, result(named).amount)
            assertEquals("BCA Harian", result(named).target)
            repeat(30) { if (manager.activeNotifications.none { it.tag == named.recordKey }) delay(100) }
            assertEquals(1, manager.activeNotifications.count { it.tag == named.recordKey })
            val future = add("AN**RA**").also { ids.add(it) }
            assertEquals("Transfer masuk dari Andra", result(waitFor(future, "recorded")).description)
            val unknown = add("DE**WI**").also { ids.add(it) }
            notificationAnswer(waitFor(unknown, "needs_confirmation", "sender"), null)
            assertEquals("Transfer masuk", result(waitFor(unknown, "recorded")).description)
            val unknownAgain = add("DE**WI**").also { ids.add(it) }
            waitFor(unknownAgain, "needs_confirmation", "sender")
            // Dismissing the OS alert leaves the durable app question answerable.
            SenderConfirmationNotifier.cancel(context, dao.getById(unknownAgain)!!.backendEventId!!)
            val dismissed = dao.getById(unknownAgain)!!
            assertEquals("needs_confirmation", dismissed.processingStatus)
            assertTrue(SenderReplies(context).enqueue(dismissed.id, dismissed.backendEventId!!,
                result(dismissed).confirmation!!.questionId!!, prefs.pairingContext, "unknown", null).queued)
            waitFor(unknownAgain, "recorded")
            val concurrent = listOf(add("RI**NA**").also { ids.add(it) }, add("MA**YA**").also { ids.add(it) })
            val questions = concurrent.map { waitFor(it, "needs_confirmation", "sender") }
            notificationAnswer(questions[0], "Rina")
            notificationAnswer(questions[1], "Maya")
            assertEquals("Transfer masuk dari Rina", result(waitFor(concurrent[0], "recorded")).description)
            assertEquals("Transfer masuk dari Maya", result(waitFor(concurrent[1], "recorded")).description)
            fixture("sender-account/true", true)
            val sequential = add("LI**NA**").also { ids.add(it) }
            val account = waitFor(sequential, "needs_confirmation", "account")
            val accountQuestion = result(account)
            assertTrue(MappingConfirmation(context).confirm(account.backendEventId!!, accountQuestion.mappingProposal!!.proposed!!.id!!,
                accountQuestion.confirmation!!.questionId).success)
            fixture("sender-account/false", true)
            val sender = waitFor(sequential, "needs_confirmation", "sender")
            assertNotEquals(accountQuestion.confirmation.questionId, result(sender).confirmation!!.questionId)
            notificationAnswer(sender, "Lina")
            waitFor(sequential, "recorded")
            assertEquals(before + 7, fixture("sender-state").getAsJsonArray("records").size())
        } finally {
            fixture("sender-account/false", true)
            permission(true)
            WorkManager.getInstance(context).cancelAllWork().result.get(10, TimeUnit.SECONDS)
            ids.forEach { id -> dao.getById(id)?.let { row ->
                row.backendEventId?.let { SenderConfirmationNotifier.cancel(context, it) }
                row.recordKey?.let { manager.cancel(it, it.hashCode() and Int.MAX_VALUE) }
                dao.deleteById(id)
            } }
        }
    }

    // Revoke permission with adb before instrumentation: Android kills an app when its permission is revoked.
    @Test fun notificationPermissionDeniedUsesDurableAppAnswer(): Unit = runBlocking {
        requireLocal()
        assertFalse(manager.areNotificationsEnabled())
        TestEndpoint.set(context, "http://127.0.0.1:18000")
        prefs.apiKey = "fictional-phone-key"
        val before = fixture("sender-state").getAsJsonArray("records").size()
        val id = add("DI**NA**")
        try {
            val waiting = waitFor(id, "needs_confirmation", "sender")
            assertTrue(manager.activeNotifications.none { it.tag == "sender:${waiting.backendEventId}" })
            assertTrue(SenderReplies(context).enqueue(waiting.id, waiting.backendEventId!!,
                result(waiting).confirmation!!.questionId!!, prefs.pairingContext, "name", "Dina").queued)
            assertEquals("Transfer masuk dari Dina", result(waitFor(id, "recorded")).description)
            assertEquals(before + 1, fixture("sender-state").getAsJsonArray("records").size())
        } finally {
            WorkManager.getInstance(context).cancelAllWork().result.get(10, TimeUnit.SECONDS)
            dao.deleteById(id)
        }
    }

    @Test fun prepareReplyForOfflineRestart(): Unit = runBlocking {
        requireLocal()
        TestEndpoint.set(context, "http://127.0.0.1:18000")
        prefs.apiKey = "fictional-phone-key"
        val id = add("SA**RA**")
        val held = waitFor(id, "needs_confirmation", "sender")
        WorkManager.getInstance(context).cancelAllWork().result.get(10, TimeUnit.SECONDS)
        val before = fixture("sender-state").getAsJsonArray("records").size()
        assertTrue(SenderReplies(context, db, { reply, _ -> SyncScheduler.enqueueSenderReply(context, reply, 300000) }, { _, _ -> })
            .enqueue(id, held.backendEventId!!, result(held).confirmation!!.questionId!!, prefs.pairingContext, "name", "Sara").queued)
        val reply = db.senderReplyDao().forQuestion(prefs.pairingContext, held.backendEventId, result(held).confirmation!!.questionId!!)!!
        phase.edit().putLong("row_id", id).putString("reply_id", reply.replyId).putInt("records_before", before).commit()
    }

    @Test fun verifyOfflineReplyAfterRestart(): Unit = runBlocking {
        requireLocal()
        val reply = db.senderReplyDao().get(phase.getString("reply_id", null)!!)!!
        assertEquals("Sara", reply.name)
        TestListenableWorkerBuilder<SenderReplyWorker>(context, inputData = Data.Builder().putString(SenderReplyWorker.REPLY_ID, reply.replyId).build()).build().doWork()
        assertEquals("queued", db.senderReplyDao().get(reply.replyId)!!.state)
        assertEquals("network_unavailable", db.senderReplyDao().get(reply.replyId)!!.errorCode)
        assertEquals("needs_confirmation", dao.getById(reply.rowId)!!.processingStatus)
    }

    @Test fun resumeOfflineReplyOnRestoredConnection(): Unit = runBlocking {
        requireLocal()
        val reply = db.senderReplyDao().get(phase.getString("reply_id", null)!!)!!
        TestListenableWorkerBuilder<SenderReplyWorker>(context, inputData = Data.Builder().putString(SenderReplyWorker.REPLY_ID, reply.replyId).build()).build().doWork()
        val recorded = waitFor(reply.rowId, "recorded")
        assertEquals("Transfer masuk dari Sara", result(recorded).description)
        assertEquals(phase.getInt("records_before", -100) + 1, fixture("sender-state").getAsJsonArray("records").size())
        assertEquals("accepted", db.senderReplyDao().get(reply.replyId)!!.state)
        WorkManager.getInstance(context).cancelAllWork().result.get(10, TimeUnit.SECONDS)
        dao.deleteById(reply.rowId)
        phase.edit().clear().commit()
        SenderConfirmationNotifier.cancel(context, reply.eventId)
        recorded.recordKey?.let { manager.cancel(it, it.hashCode() and Int.MAX_VALUE) }
    }
}
