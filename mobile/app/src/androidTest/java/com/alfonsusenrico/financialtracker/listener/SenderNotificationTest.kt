package com.alfonsusenrico.financialtracker.listener

import android.app.KeyguardManager
import android.app.NotificationManager
import android.content.Intent
import android.os.Build
import androidx.core.app.RemoteInput
import androidx.test.platform.app.InstrumentationRegistry
import androidx.work.WorkManager
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.model.RawNotificationEntity
import com.alfonsusenrico.financialtracker.listener.data.network.model.*
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.data.sync.NotificationResults
import com.alfonsusenrico.financialtracker.listener.receiver.SenderReplyReceiver
import com.alfonsusenrico.financialtracker.listener.service.SenderConfirmationNotifier
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.Assert.*
import org.junit.Assume.assumeFalse
import org.junit.Test
import java.util.UUID
import java.util.concurrent.TimeUnit

class SenderNotificationTest {
    private val instrumentation = InstrumentationRegistry.getInstrumentation()
    private val context = instrumentation.targetContext
    private val db = AppDatabase.getDatabase(context)
    private val prefs = AppPreferences(context)
    private fun uuid() = UUID.randomUUID().toString()

    @Test fun restrictedActionsRouteTwoNamesToTheirOwnQuestionsAndRequireUnlock() = runBlocking {
        check(context.packageName.endsWith(".verification"))
        assumeFalse(context.getSystemService(KeyguardManager::class.java).isDeviceLocked)
        if (Build.VERSION.SDK_INT >= 33) instrumentation.uiAutomation.executeShellCommand("pm grant ${context.packageName} android.permission.POST_NOTIFICATIONS").close()
        TestEndpoint.set(context, "http://127.0.0.1:1")
        prefs.apiKey = "fictional-direct-reply-key"
        val manager = context.getSystemService(NotificationManager::class.java)
        val rows = mutableListOf<RawNotificationEntity>()
        val questions = mutableListOf<IngestResultItem>()
        try {
            repeat(2) {
                val event = uuid()
                val id = db.notificationDao().insertOrIgnore(RawNotificationEntity(deviceId = "fictional", packageName = "com.bca", appLabel = "BCA",
                    postTime = 1000, title = "Fictional transfer", payloadHash = event))
                val item = IngestResultItem(payloadHash = event, eventId = event, status = "needs_confirmation",
                    confirmation = ConfirmationQuestion("sender", uuid(), "AN**RA**", "bca", AccountChoice(uuid(), "BCA"), 112590, "IDR"))
                assertTrue(NotificationResults(context).consume(db.notificationDao().getById(id)!!, item, prefs.pairingContext))
                rows.add(db.notificationDao().getById(id)!!)
                questions.add(item)
            }
            repeat(30) {
                if (questions.any { item -> manager.activeNotifications.none { it.tag == "sender:${item.eventId}" } }) delay(100)
            }
            val notifications = questions.map { item -> manager.activeNotifications.single { it.tag == "sender:${item.eventId}" }.notification }
            assertNotEquals(notifications[0].actions[0].actionIntent, notifications[1].actions[0].actionIntent)
            notifications.forEach {
                assertEquals(android.app.Notification.VISIBILITY_PRIVATE, it.visibility)
                assertNotNull(it.publicVersion)
                assertTrue(it.publicVersion.actions.isNullOrEmpty())
                assertEquals(context.packageName, it.actions[0].actionIntent.creatorPackage)
                if (Build.VERSION.SDK_INT >= 31) it.actions.forEach { action -> assertTrue(action.isAuthenticationRequired) }
            }
            notifications.forEachIndexed { index, notification ->
                val fill = Intent()
                val remote = RemoteInput.Builder(SenderReplyReceiver.INPUT_NAME).build()
                RemoteInput.addResultsToIntent(arrayOf(remote), fill, android.os.Bundle().apply {
                    putCharSequence(SenderReplyReceiver.INPUT_NAME, if (index == 0) "Andra" else "Dewi")
                })
                notification.actions[0].actionIntent.send(context, 0, fill)
            }
            repeat(40) {
                if (questions.all { db.senderReplyDao().forQuestion(prefs.pairingContext, it.eventId!!, it.confirmation!!.questionId!!) != null }) return@repeat
                delay(100)
            }
            questions.forEachIndexed { index, item ->
                val reply = db.senderReplyDao().forQuestion(prefs.pairingContext, item.eventId!!, item.confirmation!!.questionId!!)!!
                assertEquals(rows[index].id, reply.rowId)
                assertEquals(if (index == 0) "Andra" else "Dewi", reply.name)
                assertEquals("needs_confirmation", db.notificationDao().getById(reply.rowId)!!.processingStatus)
            }
            // Reusing an old explicit action after the question changes cannot replace the pending answer.
            val staleIntent = SenderReplyReceiver.intent(context, rows[0].id, questions[0].eventId!!, uuid(), prefs.pairingContext, "unknown")
            context.sendBroadcast(staleIntent)
            delay(200)
            assertEquals("Andra", db.senderReplyDao().forQuestion(prefs.pairingContext, questions[0].eventId!!, questions[0].confirmation!!.questionId!!)!!.name)
        } finally {
            WorkManager.getInstance(context).cancelAllWork().result.get(10, TimeUnit.SECONDS)
            questions.forEach { SenderConfirmationNotifier.cancel(context, it.eventId!!) }
            rows.forEach { db.notificationDao().deleteById(it.id) }
            prefs.apiKey = ""
        }
    }
}
