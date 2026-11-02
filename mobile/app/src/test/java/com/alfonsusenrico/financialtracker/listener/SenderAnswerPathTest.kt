package com.alfonsusenrico.financialtracker.listener

import android.Manifest
import android.app.Application
import android.app.KeyguardManager
import android.app.Notification
import android.app.NotificationManager
import android.content.Context
import android.content.Intent
import android.os.Bundle
import android.os.Looper
import android.util.Log
import androidx.core.app.RemoteInput
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.work.Configuration
import androidx.work.testing.SynchronousExecutor
import androidx.work.testing.WorkManagerTestInitHelper
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.model.AnswerAttemptEntity
import com.alfonsusenrico.financialtracker.listener.data.model.RawNotificationEntity
import com.alfonsusenrico.financialtracker.listener.data.model.SenderReplyEntity
import com.alfonsusenrico.financialtracker.listener.data.network.model.AccountChoice
import com.alfonsusenrico.financialtracker.listener.data.network.model.ConfirmationQuestion
import com.alfonsusenrico.financialtracker.listener.data.network.model.IngestResultItem
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.data.sync.AnswerAttempts
import com.alfonsusenrico.financialtracker.listener.data.sync.NotificationResults
import com.alfonsusenrico.financialtracker.listener.receiver.SenderReplyReceiver
import com.alfonsusenrico.financialtracker.listener.ui.shell.CompanionViewModel
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.Shadows.shadowOf
import org.robolectric.annotation.Config

/**
 * One sender question answered through each real path: the notification's actions through
 * [SenderReplyReceiver], and Tanya's view model. A second transfer of the same amount, one minute
 * later and already recorded, sits beside it as on the owner's phone.
 */
@RunWith(AndroidJUnit4::class)
@Config(sdk = [34], application = TestApplication::class)
class SenderAnswerPathTest {
    private val context = ApplicationProvider.getApplicationContext<Context>()
    private val manager = context.getSystemService(NotificationManager::class.java)
    private lateinit var db: AppDatabase
    private lateinit var pairing: String
    private var askedRow = 0L
    private var recordedRow = 0L

    private val askedEvent = "00000000-0000-4000-8000-000000000818"
    private val askedQuestion = "00000000-0000-4000-8000-000000001818"
    private val recordedEvent = "00000000-0000-4000-8000-000000000819"
    private val receivingAccount = "00000000-0000-4000-8000-000000002818"

    private fun asked() = IngestResultItem(
        payloadHash = "fictional-818", eventId = askedEvent, status = "needs_confirmation",
        errorCode = "sender_confirmation_required",
        confirmation = ConfirmationQuestion("sender", askedQuestion, "AN**RA**", "bca", AccountChoice(receivingAccount, "BCA Contoh"), 112_590, "IDR")
    )

    private fun recorded() = IngestResultItem(
        payloadHash = "fictional-819", eventId = recordedEvent, status = "recorded", recordKey = "fictional:819",
        type = "income", description = "Transfer dari Contoh", target = "BCA Contoh", amount = 112_590, currency = "IDR"
    )

    @Before fun open() = runBlocking {
        WorkManagerTestInitHelper.initializeTestWorkManager(
            context, Configuration.Builder().setMinimumLoggingLevel(Log.DEBUG).setExecutor(SynchronousExecutor()).build()
        )
        shadowOf(context as Application).grantPermissions(Manifest.permission.POST_NOTIFICATIONS)
        TestEndpoint.set(context, "http://127.0.0.1:1")
        AppPreferences(context).apiKey = "fictional-sender-path-key"
        pairing = AppPreferences(context).pairingContext
        // The receiver opens the app's database, so the test uses it too and starts from empty tables.
        db = AppDatabase.getDatabase(context)
        withContext(Dispatchers.IO) { db.clearAllTables() }
        askedRow = insertRow("fictional-818", 1_000)
        recordedRow = insertRow("fictional-819", 61_000)
        val results = NotificationResults(context, db, scheduleFollowUp = { _, _ -> })
        assertTrue(results.consume(db.notificationDao().getById(askedRow)!!, asked(), pairing))
        assertTrue(results.consume(db.notificationDao().getById(recordedRow)!!, recorded(), pairing))
        results.deliverPendingAlerts(pairing)
    }

    @After fun close() {
        AppPreferences(context).apiKey = ""
    }

    private suspend fun insertRow(hash: String, postTime: Long) = db.notificationDao().insertOrIgnore(
        RawNotificationEntity(
            deviceId = "fictional", packageName = "com.bca.mybca.omni.android", appLabel = "myBCA",
            postTime = postTime, title = "Transfer masuk", payloadHash = hash
        )
    )

    private fun question(): Notification? = manager.activeNotifications.singleOrNull { it.tag == "sender:$askedEvent" }?.notification

    private fun text(notification: Notification?) = notification?.extras?.getCharSequence(Notification.EXTRA_TEXT)?.toString()

    private fun reply(): SenderReplyEntity? = runBlocking { db.senderReplyDao().forQuestion(pairing, askedEvent, askedQuestion) }

    private fun attempts(): List<Pair<String, String>> =
        runBlocking { db.answerAttemptDao().forQuestion(askedEvent, askedQuestion) }.map { it.channel to it.outcome }

    private fun fromNotification(vararg outcomes: String) = outcomes.map { AnswerAttempts.NOTIFICATION to it }

    /** Runs the main looper (broadcasts, view-model work) until [done] holds or two seconds pass. */
    private fun settle(done: () -> Boolean) {
        val until = System.currentTimeMillis() + 2_000
        while (!done() && System.currentTimeMillis() < until) {
            shadowOf(Looper.getMainLooper()).idle()
            Thread.sleep(20)
        }
        shadowOf(Looper.getMainLooper()).idle()
    }

    private fun sendName(notification: Notification, name: String?) {
        val fill = Intent()
        if (name != null) {
            RemoteInput.addResultsToIntent(
                arrayOf(RemoteInput.Builder(SenderReplyReceiver.INPUT_NAME).build()), fill,
                Bundle().apply { putCharSequence(SenderReplyReceiver.INPUT_NAME, name) }
            )
        }
        notification.actions[0].actionIntent.send(context, 0, fill)
    }

    @Test fun recordedNeighbourLeavesTheQuestionAndItsOwnActions() {
        val shown = question()
        assertNotNull("The question is still on screen after the neighbour was recorded", shown)
        assertEquals(2, shown!!.actions.size)
        val alert = manager.activeNotifications.single { it.tag == "fictional:819" }.notification
        assertTrue(alert.actions.isNullOrEmpty())
        assertNotEquals(alert.contentIntent, shown.contentIntent)
    }

    @Test fun notificationNameIsStoredForItsOwnQuestion() {
        sendName(question()!!, "Nama Contoh")
        settle { reply() != null }
        val stored = reply()
        assertNotNull("The inline reply was not stored", stored)
        assertEquals(askedRow, stored!!.rowId)
        assertEquals("name", stored.action)
        assertEquals("Nama Contoh", stored.name)
        assertEquals("Balasan tersimpan; menunggu server.", text(question()))
        assertEquals(fromNotification(AnswerAttempts.RECEIVED, AnswerAttempts.QUEUED), attempts())
        val recorded = runBlocking { db.answerAttemptDao().forQuestion(askedEvent, askedQuestion) }
        assertTrue("The attempt record never holds the name", recorded.none { "Nama Contoh" in it.toString() })
        assertTrue(recorded.all { it.rowId == askedRow && it.appVersion == BuildConfig.VERSION_NAME })
    }

    @Test fun notificationUnknownIsStoredForItsOwnQuestion() {
        question()!!.actions[1].actionIntent.send()
        settle { reply() != null }
        assertEquals("unknown", reply()?.action)
        assertNull(reply()?.name)
    }

    @Test fun tanyaNameIsStoredForItsOwnQuestion() {
        val viewModel = CompanionViewModel(context as Application, db)
        var outcome: Pair<Boolean, String?>? = null
        viewModel.confirmSender(askedRow, askedQuestion, "Nama Contoh") { ok, error -> outcome = ok to error }
        settle { outcome != null }
        assertEquals(true to null, outcome)
        assertEquals(askedRow, reply()?.rowId)
        assertEquals("Nama Contoh", reply()?.name)
        assertEquals(listOf(AnswerAttempts.APP to AnswerAttempts.QUEUED), attempts())
    }

    @Test fun blankNotificationReplyLeavesTheQuestionAnswerable() {
        sendName(question()!!, " ")
        settle { text(question()) != null && question()?.actions.isNullOrEmpty() }
        assertNull(reply())
        assertEquals("The blank reply must stay correctable from the notification", 2, question()?.actions?.size ?: 0)
        assertEquals("Isi nama pengirim, maksimal 80 karakter.", text(question()))
        assertEquals(fromNotification(AnswerAttempts.RECEIVED, AnswerAttempts.INVALID_NAME), attempts())
    }

    @Test fun missingReplyResultsLeaveTheQuestionAnswerable() {
        sendName(question()!!, null)
        settle { question()?.actions.isNullOrEmpty() }
        assertNull(reply())
        assertEquals("Missing input must stay correctable from the notification", 2, question()?.actions?.size ?: 0)
        assertEquals(fromNotification(AnswerAttempts.RECEIVED, AnswerAttempts.MISSING_INPUT), attempts())
    }

    @Test fun laterLookupOffersARefusedQuestionAgain() = runBlocking {
        sendName(question()!!, " ")
        settle { question()?.actions.isNullOrEmpty() }
        val results = NotificationResults(context, db, scheduleFollowUp = { _, _ -> })
        assertTrue(results.consume(db.notificationDao().getById(askedRow)!!, asked(), pairing))
        assertEquals("A later lookup must still offer the unanswered question", 2, question()?.actions?.size ?: 0)
    }

    @Test fun lockedDeviceLeavesTheQuestionAnswerable() {
        shadowOf(context.getSystemService(KeyguardManager::class.java)).setIsDeviceLocked(true)
        sendName(question()!!, "Nama Contoh")
        settle { question()?.actions.isNullOrEmpty() }
        assertNull(reply())
        assertEquals("A refused attempt must leave the question answerable", 2, question()?.actions?.size ?: 0)
        assertEquals("Buka kunci lalu balas lagi.", text(question()))
        assertEquals(fromNotification(AnswerAttempts.RECEIVED, AnswerAttempts.LOCKED), attempts())
    }

    @Test fun refusedAttemptAfterAStoredAnswerOnlyReportsIt() {
        sendName(question()!!, "Nama Contoh")
        settle { reply() != null }
        SenderReplyReceiver.intent(context, askedRow, askedEvent, askedQuestion, pairing, "unknown").let(context::sendBroadcast)
        settle { attempts().size == 4 }
        assertEquals("Nama Contoh", reply()?.name)
        assertTrue("A stored answer is not offered again", question()?.actions.isNullOrEmpty())
        assertEquals("Balasan sebelumnya sudah tersimpan. Tunggu hasil server.", text(question()))
        assertEquals(
            fromNotification(AnswerAttempts.RECEIVED, AnswerAttempts.QUEUED, AnswerAttempts.RECEIVED, AnswerAttempts.ALREADY_ANSWERED),
            attempts()
        )
    }

    @Test fun malformedActionIsRecordedWithoutIdentifiers() = runBlocking {
        context.sendBroadcast(Intent(context, SenderReplyReceiver::class.java).setAction(SenderReplyReceiver.ACTION_REPLY))
        var all = emptyList<AnswerAttemptEntity>()
        settle { all = runBlocking { db.answerAttemptDao().recentOutcomesFlow().first() }; all.isNotEmpty() }
        assertEquals(AnswerAttempts.MALFORMED, all.single().outcome)
        assertNull(all.single().eventId)
        assertNull(reply())
        assertEquals(2, question()?.actions?.size)
    }

    @Test fun attemptRecordKeepsOnlyTheNewestTwoHundred() = runBlocking {
        repeat(205) { AnswerAttempts.record(db, AnswerAttempts.APP, AnswerAttempts.SENDER, askedRow, askedEvent, askedQuestion, AnswerAttempts.QUEUED, now = it.toLong()) }
        val kept = db.answerAttemptDao().forQuestion(askedEvent, askedQuestion)
        assertEquals(200, kept.size)
        assertEquals(5L, kept.first().createdAt)
    }
}
