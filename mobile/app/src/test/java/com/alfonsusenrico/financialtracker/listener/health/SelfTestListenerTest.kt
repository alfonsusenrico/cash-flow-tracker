package com.alfonsusenrico.financialtracker.listener.health

import android.app.Notification
import android.content.Context
import android.os.Bundle
import android.os.Process
import android.service.notification.StatusBarNotification
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import com.alfonsusenrico.financialtracker.listener.TestApplication
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.health.SelfTestCatches
import com.alfonsusenrico.financialtracker.listener.data.health.SelfTestMarker
import com.alfonsusenrico.financialtracker.listener.service.FinancialNotificationListenerService
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.Robolectric
import org.robolectric.annotation.Config

/** The real listener service: the self-test's notification is answered and dropped, a bank's is stored. */
@RunWith(AndroidJUnit4::class)
@Config(sdk = [34], application = TestApplication::class)
class SelfTestListenerTest {
    private val context = ApplicationProvider.getApplicationContext<Context>()

    @Suppress("DEPRECATION")
    private fun posted(pkg: String, tag: String?, extras: Bundle, title: String, text: String, id: Int) = StatusBarNotification(
        pkg, pkg, id, tag, Process.myUid(), 0, 0,
        Notification.Builder(context, "fictional").setContentTitle(title).setContentText(text).addExtras(extras).build(),
        Process.myUserHandle(), 1_793_600_000_000L + id
    )

    @Test fun theSelfTestIsCaughtButNeverStoredWhileABankNotificationIs() = runBlocking {
        val db = AppDatabase.getDatabase(context)
        withContext(Dispatchers.IO) { db.clearAllTables() }
        val service = Robolectric.setupService(FinancialNotificationListenerService::class.java)
        val token = "00000000-0000-4000-8000-00000000c0de"
        val waiting = SelfTestCatches.expect(token)

        service.onNotificationPosted(posted(
            context.packageName, SelfTestMarker.TAG, Bundle().apply { putString(SelfTestMarker.EXTRA_TOKEN, token) },
            "Tes Cash Flow Tracker", "Ini notifikasi tes, bukan transaksi. Nggak bakal kecatat.", SelfTestMarker.NOTIFICATION_ID
        ))
        assertTrue("the listener answered the waiting test", waiting.isCompleted)

        // A synthetic bank notification takes the normal path, which proves the queue was running.
        service.onNotificationPosted(posted("com.gojek.app", null, Bundle(), "Pembayaran berhasil", "Kamu bayar Rp27.000 ke Kopi Contoh", 77))
        suspend fun packages() = withContext(Dispatchers.IO) {
            db.query("SELECT package_name FROM raw_notifications", null).use { cursor ->
                generateSequence { if (cursor.moveToNext()) cursor.getString(0) else null }.toList()
            }
        }
        val everything = withTimeout(10_000) {
            var rows = packages()
            while (rows.isEmpty()) {
                delay(50)
                rows = packages()
            }
            rows
        }
        assertEquals("the self-test left no row of any outcome", listOf("com.gojek.app"), everything)
    }
}
