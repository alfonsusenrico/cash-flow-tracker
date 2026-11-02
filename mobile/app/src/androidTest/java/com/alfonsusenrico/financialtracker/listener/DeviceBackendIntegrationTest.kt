package com.alfonsusenrico.financialtracker.listener

import android.Manifest
import android.os.ParcelFileDescriptor
import android.content.pm.PackageManager
import android.app.NotificationManager
import android.content.Context
import androidx.test.platform.app.InstrumentationRegistry
import androidx.work.WorkManager
import androidx.work.testing.TestListenableWorkerBuilder
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.model.RawNotificationEntity
import com.alfonsusenrico.financialtracker.listener.data.network.ApiClient
import com.alfonsusenrico.financialtracker.listener.data.network.model.TransactionUpdateRequest
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.data.sync.NotificationSyncWorker
import com.alfonsusenrico.financialtracker.listener.data.sync.NotificationResults
import com.alfonsusenrico.financialtracker.listener.service.TransactionNotifier
import com.alfonsusenrico.financialtracker.listener.service.NotificationProcessor
import com.google.gson.Gson
import com.alfonsusenrico.financialtracker.listener.data.network.model.IngestResultItem
import kotlinx.coroutines.runBlocking
import org.junit.Assert.*
import org.junit.Assume.assumeTrue
import org.junit.Test
import java.util.UUID
import java.util.concurrent.TimeUnit

class DeviceBackendIntegrationTest {
    @Test fun fictionalPhoneToBackendCompletionAlertAndEdit() = runBlocking {
        assumeTrue(InstrumentationRegistry.getArguments().getString("localBackend") == "true")
        val context = InstrumentationRegistry.getInstrumentation().targetContext
        check(context.packageName.endsWith(".verification"))
        val automation = InstrumentationRegistry.getInstrumentation().uiAutomation
        fun permission(grant: Boolean) {
            val descriptor = automation.executeShellCommand("pm ${if (grant) "grant" else "revoke"} ${context.packageName} ${Manifest.permission.POST_NOTIFICATIONS}")
            ParcelFileDescriptor.AutoCloseInputStream(descriptor).use { it.readBytes() }
            assertEquals(if (grant) PackageManager.PERMISSION_GRANTED else PackageManager.PERMISSION_DENIED,
                context.checkSelfPermission(Manifest.permission.POST_NOTIFICATIONS))
        }
        val dao = AppDatabase.getDatabase(context).notificationDao()
        val prefs = AppPreferences(context)
        val manager = context.getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager
        prefs.isAutoSyncEnabled = false
        TestEndpoint.set(context, "http://127.0.0.1:18000")
        prefs.apiKey = "fictional-phone-key"
        WorkManager.getInstance(context).cancelAllWork().result.get(10, TimeUnit.SECONDS)
        val ids = mutableListOf<Long>()
        val tags = mutableSetOf<String>()
        suspend fun add(body: String, second: Int): Long {
            val summary = NotificationProcessor.extractDetectionSummary("com.jago.digitalbanking", "Transfer berhasil", body, null)
            val id = dao.insertOrIgnore(RawNotificationEntity(
                deviceId = "fictional-phone", packageName = "com.jago.digitalbanking", appLabel = "Bank Jago",
                title = "Transfer berhasil", bodyText = body, postTime = 1790676900000L + second * 1000,
                payloadHash = "fictional-phone-${UUID.randomUUID()}", isFinancial = true,
                eventClass = summary.eventClass, expectedAmount = summary.expectedAmount,
                expectedDirection = summary.expectedDirection, expectedCounterparty = summary.expectedCounterparty
            ))
            ids.add(id)
            return id
        }
        suspend fun upload() {
            TestListenableWorkerBuilder<NotificationSyncWorker>(context).build().doWork()
            WorkManager.getInstance(context).cancelAllWork().result.get(10, TimeUnit.SECONDS)
        }
        try {
            permission(false)
            val expenseId = add("You've paid Rp125.000 to Kedai Awan", 27)
            upload()
            val recorded = dao.getById(expenseId)!!
            assertEquals("recorded", recorded.processingStatus)
            assertNotNull(recorded.editableTransactionId)
            tags.add(recorded.recordKey!!)
            assertEquals(1, dao.getPendingAlerts(prefs.pairingContext).size)
            permission(true)
            val item = Gson().fromJson(recorded.resultJson, IngestResultItem::class.java)
            // Simulate process interruption after OS submission and before receipt persistence.
            assertTrue("Android notifications must be enabled", manager.areNotificationsEnabled())
            assertTrue(TransactionNotifier.notifyRecorded(context, item))
            NotificationResults(context).deliverPendingAlerts()
            assertEquals(1, manager.activeNotifications.count { it.tag == recorded.recordKey })
            val edit = ApiClient(context).createService().updateTransaction(recorded.editableTransactionId!!,
                TransactionUpdateRequest(amount = 126000, notes = "Fictional corrected expense"))
            assertTrue(edit.isSuccessful && edit.body()?.ok == true)
            val firstId = add(
                "Rp500.000 has been moved from your Kantong Utama Pocket to your Dana Darurat Pocket.",
                1
            )
            upload()
            val first = dao.getById(firstId)!!
            assertEquals("recorded", first.processingStatus)
            assertEquals("internal_movement", Gson().fromJson(first.resultJson, IngestResultItem::class.java).type)
            assertNull(first.editableTransactionId)
            tags.add(first.recordKey!!)
            assertEquals(1, manager.activeNotifications.count { it.tag == first.recordKey })
            manager.cancel(first.recordKey, first.recordKey.hashCode() and 0x7fffffff)
            NotificationResults(context).consume(first, Gson().fromJson(first.resultJson, IngestResultItem::class.java), prefs.pairingContext)
            NotificationResults(context).deliverPendingAlerts()
            assertEquals(0, manager.activeNotifications.count { it.tag == first.recordKey })
            assertTrue(dao.getUnsyncedNotifications(prefs.pairingContext).isEmpty())
        } finally {
            WorkManager.getInstance(context).cancelAllWork().result.get(10, TimeUnit.SECONDS)
            ids.forEach { dao.deleteById(it) }
            tags.forEach { manager.cancel(it, it.hashCode() and 0x7fffffff) }
            prefs.apiKey = ""
        }
    }
}
