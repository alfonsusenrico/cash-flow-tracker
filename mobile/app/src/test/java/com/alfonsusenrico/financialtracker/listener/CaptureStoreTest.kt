package com.alfonsusenrico.financialtracker.listener

import android.app.Application
import android.content.Context
import androidx.room.Room
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import com.alfonsusenrico.financialtracker.listener.data.capture.CaptureFingerprint
import com.alfonsusenrico.financialtracker.listener.data.capture.CaptureStore
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.model.RawNotificationEntity
import com.alfonsusenrico.financialtracker.listener.service.CaptureOutcome
import com.alfonsusenrico.financialtracker.listener.service.CaptureReason
import com.alfonsusenrico.financialtracker.listener.service.NotificationProcessor
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.annotation.Config

@RunWith(AndroidJUnit4::class)
@Config(sdk = [34], application = com.alfonsusenrico.financialtracker.listener.TestApplication::class)
class CaptureStoreTest {

    private lateinit var db: AppDatabase
    private lateinit var store: CaptureStore
    private val dao get() = db.notificationDao()

    private val payment = "Kamu baru aja bayar Rp45.000 ke Contoh pake GoPay"
    private val t0 = 1_800_000_000_000L

    @Before fun open() {
        val context = ApplicationProvider.getApplicationContext<Context>()
        db = Room.inMemoryDatabaseBuilder(context, AppDatabase::class.java).build()
        store = CaptureStore(db)
    }

    @After fun close() = db.close()

    private fun row(
        packageName: String = "com.gojek.gopay",
        key: String? = "0|$packageName|7|null|10",
        title: String? = "Pembayaran berhasil",
        body: String? = payment,
        postTime: Long = t0,
        `when`: Long? = postTime - 50,
        outcome: CaptureOutcome = CaptureOutcome.SEND,
        reason: String = CaptureReason.RECOGNISED_TRANSACTION
    ) = RawNotificationEntity(
        deviceId = "fictional-device",
        packageName = packageName,
        appLabel = "GoPay",
        notificationKey = key,
        title = title,
        bodyText = body,
        postTime = postTime,
        payloadHash = NotificationProcessor.computePayloadHash(packageName, title, body, postTime),
        captureOutcome = outcome.code,
        captureReason = reason,
        notificationWhen = `when`,
        contentFingerprint = CaptureFingerprint.content(title, body, null),
        normalizedFingerprint = CaptureFingerprint.normalized(title, body, null)
    )

    private suspend fun sendRows() = dao.getUnsyncedNotifications("fictional-pairing", 500)

    @Test fun repostOfTheSamePostingUpdatesTheRow() = runBlocking {
        val first = store.store(row(postTime = t0), isGroupSummary = false) as CaptureStore.Result.Inserted
        val again = store.store(row(postTime = t0 + 30_000, `when` = t0 - 50), isGroupSummary = false)

        assertEquals(CaptureStore.Result.Reposted(first.id), again)
        val stored = dao.getById(first.id)!!
        assertEquals(1, stored.repostCount)
        assertEquals(t0 + 30_000, stored.lastPostedAt)
        assertEquals(t0, stored.postTime)
        assertEquals(NotificationProcessor.computePayloadHash("com.gojek.gopay", "Pembayaran berhasil", payment, t0), stored.payloadHash)
        assertEquals(1, sendRows().size)
    }

    @Test fun identicalPaymentsAtDifferentTimesStaySeparate() = runBlocking {
        val spent = "You spent IDR 25,000.00 at Groceries."
        val key = "0|com.bca.mybca.omni.android|1|null|10"
        val first = store.store(
            row("com.bca.mybca.omni.android", key, "Financial Diary", spent, t0, t0), false
        )
        val second = store.store(
            row("com.bca.mybca.omni.android", key, "Financial Diary", spent, t0 + 600_000, t0 + 600_000), false
        )
        assertTrue(first is CaptureStore.Result.Inserted)
        assertTrue(second is CaptureStore.Result.Inserted)
        val rows = sendRows()
        assertEquals(2, rows.size)
        assertNotEquals(rows[0].payloadHash, rows[1].payloadHash)
    }

    @Test fun rescanOfARowWithoutPostingTimeIsDeduplicatedByHash() = runBlocking {
        store.store(row(`when` = null), false)
        assertEquals(CaptureStore.Result.AlreadyStored, store.store(row(`when` = null), false))
        assertEquals(1, sendRows().size)
    }

    @Test fun gojekAndGopayReportingOnePaymentAreSentOnce() = runBlocking {
        val original = store.store(row("com.gojek.gopay", postTime = t0), false) as CaptureStore.Result.Inserted
        val duplicate = store.store(row("com.gojek.app", postTime = t0 + 20_000), false) as CaptureStore.Result.Inserted

        assertEquals(CaptureOutcome.DROP.code, duplicate.outcome)
        val dropped = dao.getById(duplicate.id)!!
        assertEquals(CaptureReason.DUPLICATE_SOURCE, dropped.captureReason)
        assertEquals(original.id, dropped.duplicateOfId)
        assertEquals(listOf(original.id), sendRows().map { it.id })
    }

    @Test fun sameTextOutsideTheWindowIsKeptTwice() = runBlocking {
        store.store(row("com.gojek.gopay", postTime = t0), false)
        store.store(row("com.gojek.app", postTime = t0 + 5 * 60_000), false)
        assertEquals(2, sendRows().size)
    }

    @Test fun normalisedTextMatchesAcrossCaseAndSpacing() = runBlocking {
        store.store(row("com.gojek.gopay", postTime = t0), false)
        val other = store.store(
            row("com.gojek.app", title = "PEMBAYARAN  berhasil", body = " $payment ", postTime = t0 + 5_000), false
        ) as CaptureStore.Result.Inserted
        assertEquals(CaptureOutcome.DROP.code, other.outcome)
    }

    @Test fun twoPaymentsReportedByBothAppsPairOneToOne() = runBlocking {
        store.store(row("com.gojek.gopay", key = "a1", postTime = t0), false)
        store.store(row("com.gojek.app", key = "b1", postTime = t0 + 5_000), false)
        store.store(row("com.gojek.gopay", key = "a2", postTime = t0 + 10_000), false)
        store.store(row("com.gojek.app", key = "b2", postTime = t0 + 15_000), false)
        assertEquals(listOf("a1", "a2"), sendRows().map { it.notificationKey })
    }

    @Test fun aRowAbsorbsOnlyOneDuplicate() = runBlocking {
        // The GoPay app missed the second payment; it must not be absorbed by the first one's row.
        store.store(row("com.gojek.gopay", key = "a1", postTime = t0), false)
        store.store(row("com.gojek.app", key = "b1", postTime = t0 + 5_000), false)
        store.store(row("com.gojek.app", key = "b2", postTime = t0 + 10_000), false)
        assertEquals(listOf("a1", "b2"), sendRows().map { it.notificationKey })
    }

    @Test fun sameAppRepeatsAreNotCrossAppDuplicates() = runBlocking {
        store.store(row("com.gojek.gopay", key = "a1", postTime = t0, `when` = t0), false)
        store.store(row("com.gojek.gopay", key = "a2", postTime = t0 + 10_000, `when` = t0 + 10_000), false)
        assertEquals(2, sendRows().size)
    }

    @Test fun groupSummaryRestatingAChildIsDropped() = runBlocking {
        val child = store.store(row(key = "child", postTime = t0), false) as CaptureStore.Result.Inserted
        val summary = store.store(row(key = "summary", postTime = t0 + 1_000), true) as CaptureStore.Result.Inserted
        val stored = dao.getById(summary.id)!!
        assertEquals(CaptureOutcome.DROP.code, stored.captureOutcome)
        assertEquals(CaptureReason.GROUP_SUMMARY, stored.captureReason)
        assertEquals(child.id, stored.duplicateOfId)
        assertEquals(1, sendRows().size)
    }

    @Test fun keptRowsStayOffTheFeedUntilTheOwnerSendsThem() = runBlocking {
        val kept = store.store(
            row(title = "Diskon 50% GoFood", body = "Klaim vouchernya!", outcome = CaptureOutcome.KEEP_LOCAL,
                reason = CaptureReason.PROMOTIONAL), false
        ) as CaptureStore.Result.Inserted

        assertTrue(sendRows().isEmpty())
        assertTrue(dao.getAllNotificationsFlow().first().isEmpty())
        assertEquals(0, dao.countUnsubmitted("fictional-pairing"))
        assertEquals(listOf(kept.id), dao.getLocalOnlyFlow().first().map { it.id })

        assertEquals(1, dao.sendLocal(kept.id))
        val sent = dao.getById(kept.id)!!
        assertEquals(CaptureOutcome.SEND.code, sent.captureOutcome)
        assertEquals(CaptureReason.OWNER_SENT, sent.captureReason)
        assertEquals(listOf(kept.id), sendRows().map { it.id })
        assertTrue(dao.getLocalOnlyFlow().first().isEmpty())
        assertEquals(0, dao.sendLocal(kept.id))
    }

    @Test fun keptAndDroppedRowsExpireAfterSevenDays() = runBlocking {
        val day = 24L * 60 * 60 * 1000
        store.store(row(key = "old-kept", body = "Klaim voucher", postTime = t0 - 8 * day,
            outcome = CaptureOutcome.KEEP_LOCAL, reason = CaptureReason.PROMOTIONAL), false)
        store.store(row(key = "new-kept", body = "Klaim bonus", postTime = t0 - day,
            outcome = CaptureOutcome.KEEP_LOCAL, reason = CaptureReason.PROMOTIONAL), false)
        store.store(row("com.gojek.gopay", key = "old-send", postTime = t0 - 8 * day), false)
        val duplicate = store.store(row("com.gojek.app", key = "old-dup", postTime = t0 - 8 * day + 1_000), false)
            as CaptureStore.Result.Inserted
        assertEquals(CaptureOutcome.DROP.code, duplicate.outcome)

        assertEquals(2, dao.pruneLocalOnlyBefore(t0 - 7 * day))
        assertEquals(listOf("new-kept"), dao.getLocalOnlyFlow().first().map { it.notificationKey })
        assertEquals(listOf("old-send"), sendRows().map { it.notificationKey })
        assertNull(dao.getById(duplicate.id))
    }
}
