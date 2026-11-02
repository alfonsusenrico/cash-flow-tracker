package com.alfonsusenrico.financialtracker.listener

import android.app.Application
import android.content.Context
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.service.CapturedNotification
import com.alfonsusenrico.financialtracker.listener.service.NotificationProcessor
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.annotation.Config

/** End to end through the processor: classification happens before storage and nothing is sent. */
@RunWith(AndroidJUnit4::class)
@Config(sdk = [34], application = com.alfonsusenrico.financialtracker.listener.TestApplication::class)
class CaptureProcessorTest {

    private fun captured(key: String, title: String, body: String, postTime: Long, ongoing: Boolean = false) =
        CapturedNotification(
            packageName = "com.gojek.gopay", notificationKey = key, notificationId = 7, notificationWhen = postTime,
            channelId = "promotional_notifications", category = null, title = title, bodyText = body, bigText = null,
            subText = null, summaryText = null, postTime = postTime, rawExtrasJson = null, isOngoing = ongoing
        )

    @Test fun captureClassifiesStoresAndCountsDrops() = runBlocking {
        val context = ApplicationProvider.getApplicationContext<Context>()
        val processor = NotificationProcessor(context)
        val prefs = AppPreferences(context)
        val dao = AppDatabase.getDatabase(context).notificationDao()
        val t0 = 1_800_000_000_000L

        processor.capture(captured("pay", "Pembayaran berhasil", "Kamu baru aja bayar Rp45.000 ke Contoh pake GoPay", t0))
        processor.capture(captured("promo", "Diskon 50% GoFood", "Klaim vouchernya hari ini!", t0 + 1_000))
        processor.capture(captured("otp", "GoPay", "Kode OTP 123456 untuk login", t0 + 2_000))
        processor.capture(captured("ongoing", "Transfer berjalan", "Mengirim Rp100.000", t0 + 3_000, ongoing = true))

        val sent = dao.getUnsyncedNotifications(prefs.pairingContext)
        assertEquals(listOf("pay"), sent.map { it.notificationKey })
        assertEquals("recognised_transaction", sent.single().captureReason)
        val kept = dao.getLocalOnlyFlow().first()
        assertEquals(listOf("promo"), kept.map { it.notificationKey })
        assertEquals("promotional", kept.single().captureReason)
        assertEquals(2, prefs.droppedCount)
        assertTrue(prefs.lastCaptureAt > 0)

        prefs.recordCaptureFailure(now = t0)
        assertEquals(1, prefs.captureFailureCount)
        assertEquals(t0, prefs.lastCaptureFailureAt)
    }
}
