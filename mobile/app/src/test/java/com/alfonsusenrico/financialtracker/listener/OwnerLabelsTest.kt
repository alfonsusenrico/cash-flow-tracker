package com.alfonsusenrico.financialtracker.listener

import android.app.Application
import android.content.Context
import androidx.room.Room
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import com.alfonsusenrico.financialtracker.listener.data.capture.OwnerLabels
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.model.RawNotificationEntity
import com.alfonsusenrico.financialtracker.listener.service.CaptureOutcome
import com.alfonsusenrico.financialtracker.listener.service.CaptureReason
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.annotation.Config

/** The inbox sheet's labels: what the owner changes is stored, every other captured label stays. */
@RunWith(AndroidJUnit4::class)
@Config(sdk = [34], application = com.alfonsusenrico.financialtracker.listener.TestApplication::class)
class OwnerLabelsTest {

    private lateinit var db: AppDatabase
    private lateinit var labels: OwnerLabels
    private val dao get() = db.notificationDao()

    @Before fun open() {
        val context = ApplicationProvider.getApplicationContext<Context>()
        db = Room.inMemoryDatabaseBuilder(context, AppDatabase::class.java).build()
        labels = OwnerLabels(dao, now = { 5_000L })
    }

    @After fun close() = db.close()

    private suspend fun insert(
        eventClass: String,
        direction: String,
        counterparty: String,
        synced: Boolean = false,
        outcome: CaptureOutcome = CaptureOutcome.SEND
    ): Long = dao.insertOrIgnore(
        RawNotificationEntity(
            deviceId = "fictional-device", packageName = "com.jago.digitalBanking", appLabel = "Bank Jago",
            title = "Fictional pocket move", bodyText = "Rp500.000 moved from Contoh Pocket",
            postTime = 1_000L, payloadHash = "fictional-$eventClass-$synced-$outcome", isSynced = synced,
            captureOutcome = outcome.code, isFinancial = true, eventClass = eventClass, expectedAmount = 500_000.0,
            expectedDirection = direction, expectedCounterparty = counterparty, labelNotes = "Internal Movement"
        )
    )

    @Test fun sendingAnUnsentPocketMoveKeepsItsCounterpartyAndDirection() = runBlocking {
        val id = insert("transfer", "internal", "Contoh Pocket → Kantong Utama")

        assertTrue(labels.labelForSend(id, 500_000, "transfer", "Pindah ke kantong utama"))

        val row = dao.getById(id)!!
        assertEquals("Contoh Pocket → Kantong Utama", row.expectedCounterparty)
        assertEquals("internal", row.expectedDirection)
        assertEquals("transfer", row.eventClass)
        assertEquals("Pindah ke kantong utama", row.labelNotes)
        assertEquals(5_000L, row.labelledAt)
    }

    @Test fun changingTheTypeSetsItsDirectionButKeepsTheCounterparty() = runBlocking {
        val id = insert("expense", "out", "Kedai Contoh")

        assertTrue(labels.labelForSend(id, 42_000, "income", null))

        val row = dao.getById(id)!!
        assertEquals("income", row.eventClass)
        assertEquals("in", row.expectedDirection)
        assertEquals(42_000.0, row.expectedAmount!!, 0.0)
        assertEquals("Kedai Contoh", row.expectedCounterparty)
        assertTrue(row.isFinancial == true)
    }

    @Test fun labellingAKeptRowJoinsTheSendPipeline() = runBlocking {
        val id = insert("expense", "out", "Kedai Contoh", outcome = CaptureOutcome.KEEP_LOCAL)

        assertTrue(labels.labelForSend(id, 15_000, "expense", null))

        val row = dao.getById(id)!!
        assertEquals(CaptureOutcome.SEND.code, row.captureOutcome)
        assertEquals(CaptureReason.OWNER_SENT, row.captureReason)
    }

    @Test fun aSentRowIsNotRelabelledForSending() = runBlocking {
        val id = insert("expense", "out", "Kedai Contoh", synced = true)

        assertFalse(labels.labelForSend(id, 99_000, "income", "changed"))

        val row = dao.getById(id)!!
        assertEquals("expense", row.eventClass)
        assertEquals(500_000.0, row.expectedAmount!!, 0.0)
        assertEquals("Internal Movement", row.labelNotes)
    }

    @Test fun anAcceptedLedgerEditKeepsTheCapturedCounterparty() = runBlocking {
        val id = insert("expense", "out", "Kedai Contoh", synced = true)

        labels.labelEdited(id, 47_500, "expense", "Makan siang")

        val row = dao.getById(id)!!
        assertEquals("Kedai Contoh", row.expectedCounterparty)
        assertEquals(47_500.0, row.expectedAmount!!, 0.0)
        assertEquals("out", row.expectedDirection)
        assertEquals("Makan siang", row.labelNotes)
    }

    @Test fun onlyExpenseAndIncomeChooseADirection() {
        assertEquals("out", OwnerLabels.directionFor("expense", "internal"))
        assertEquals("in", OwnerLabels.directionFor("income", null))
        assertEquals("internal", OwnerLabels.directionFor("transfer", "internal"))
        assertEquals(null, OwnerLabels.directionFor("transfer", null))
    }
}
