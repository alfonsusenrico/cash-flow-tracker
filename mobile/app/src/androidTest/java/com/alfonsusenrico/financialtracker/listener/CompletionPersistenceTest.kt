package com.alfonsusenrico.financialtracker.listener

import androidx.room.Room
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.model.RawNotificationEntity
import com.alfonsusenrico.financialtracker.listener.data.network.model.IngestResultItem
import com.alfonsusenrico.financialtracker.listener.data.sync.NotificationResults
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(AndroidJUnit4::class)
class CompletionPersistenceTest {
    private val context = InstrumentationRegistry.getInstrumentation().targetContext
    private lateinit var db: AppDatabase
    private lateinit var results: NotificationResults
    private val deliveries = mutableListOf<Pair<IngestResultItem, Boolean>>()
    private var permitted = true

    @Before fun setup() {
        db = Room.inMemoryDatabaseBuilder(context, AppDatabase::class.java).build()
        results = NotificationResults(context, db) { item, activeOnly ->
            if (permitted) deliveries.add(item to activeOnly)
            permitted
        }
    }
    @After fun cleanup() = db.close()

    private fun row(hash: String) = RawNotificationEntity(
        deviceId = "fictional-device", packageName = "com.jago.digitalbanking", appLabel = "Jago",
        postTime = 1000, payloadHash = hash, isFinancial = true, expectedAmount = 125000.0,
        eventClass = "expense", title = "Fictional notification"
    )
    private fun item(hash: String, event: String, status: String = "queued") = IngestResultItem(
        payloadHash = hash, eventId = event, status = status,
        recordKey = if (status == "recorded") "movement:fictional" else null,
        type = if (status == "recorded") "internal_movement" else null,
        description = if (status == "recorded") "Pindah saldo" else null,
        source = "Jago", target = "BCA", amount = 125000, currency = "IDR"
    )

    @Test fun queuedCompletionIsPersistedAndDeliveredOnceAcrossTwoEvents() = runBlocking {
        val dao = db.notificationDao()
        val firstId = dao.insertOrIgnore(row("first"))
        val secondId = dao.insertOrIgnore(row("second"))
        val firstEvent = "00000000-0000-4000-8000-000000000001"
        val secondEvent = "00000000-0000-4000-8000-000000000002"
        assertTrue(results.consume(dao.getById(firstId)!!, item("first", firstEvent), "pair"))
        assertTrue(dao.getById(firstId)!!.isSynced)
        assertEquals(1, dao.getDueCompletion("pair", Long.MAX_VALUE).size)
        assertEquals(1, dao.getUnsyncedNotifications("pair").size)
        results.deliverPendingAlerts("pair")
        assertTrue(deliveries.isEmpty())
        assertTrue(results.consume(dao.getById(firstId)!!, item("first", firstEvent, "recorded"), "pair"))
        results.deliverPendingAlerts("pair")
        assertEquals(1, deliveries.size)
        assertFalse(deliveries.single().second)
        assertTrue(results.consume(dao.getById(secondId)!!, item("second", secondEvent, "recorded"), "pair"))
        results.deliverPendingAlerts("pair")
        assertEquals(1, deliveries.count { !it.second })
        assertTrue(deliveries.last().second)
        assertNull(dao.getById(firstId)!!.editableTransactionId)
    }

    @Test fun permissionDenialKeepsAlertPendingUntilPermittedWithoutResubmission() = runBlocking {
        val dao = db.notificationDao()
        val id = dao.insertOrIgnore(row("permission"))
        val event = "00000000-0000-4000-8000-000000000003"
        results.consume(dao.getById(id)!!, item("permission", event, "recorded"), "pair")
        permitted = false
        results.deliverPendingAlerts("pair")
        assertEquals(1, dao.getPendingAlerts("pair").size)
        permitted = true
        results.deliverPendingAlerts("pair")
        assertTrue(dao.getPendingAlerts("pair").isEmpty())
        assertTrue(dao.getUnsyncedNotifications("pair").isEmpty())
    }

    @Test fun foreignPairingAndMismatchedEventCannotOverwriteAcceptedState() = runBlocking {
        val dao = db.notificationDao()
        val id = dao.insertOrIgnore(row("isolation"))
        val event = "00000000-0000-4000-8000-000000000004"
        results.consume(dao.getById(id)!!, item("isolation", event), "original")
        assertFalse(results.consume(dao.getById(id)!!, item("isolation", event, "recorded"), "foreign"))
        assertFalse(results.consume(dao.getById(id)!!, item("wrong-hash", event, "recorded"), "original"))
        assertTrue(dao.getDueCompletion("foreign", Long.MAX_VALUE).isEmpty())
        assertEquals("queued", dao.getById(id)!!.processingStatus)
    }

    @Test fun malformedRecordedResultIsAcceptedButNeverAlertsOrExposesEditId() = runBlocking {
        val dao = db.notificationDao()
        val id = dao.insertOrIgnore(row("malformed"))
        val event = "00000000-0000-4000-8000-000000000005"
        results.consume(dao.getById(id)!!, item("malformed", event, "recorded").copy(amount = null), "pair")
        assertEquals("invalid_recorded_result", dao.getById(id)!!.processingError)
        results.deliverPendingAlerts("pair")
        assertTrue(deliveries.isEmpty())
        assertTrue(dao.getUnsyncedNotifications("pair").isEmpty())
    }

    @Test fun retentionKeepsSixtyQueuedAndReviewEventsAcrossRestart() = runBlocking {
        val dao = db.notificationDao()
        repeat(60) { index ->
            val id = dao.insertOrIgnore(row("retention-$index"))
            val event = "00000000-0000-4000-8000-${(index + 1).toString().padStart(12, '0')}"
            results.consume(dao.getById(id)!!, item("retention-$index", event), "pair")
        }
        dao.pruneResolvedKeepLatest(15)
        assertEquals(50, dao.getDueCompletion("pair", Long.MAX_VALUE).size)
        assertEquals(60, dao.getDueCompletion("pair", Long.MAX_VALUE, 100).size)
        val resumed = NotificationResults(context, db) { _, _ -> fail("Queued event alerted"); false }
        resumed.deliverPendingAlerts("pair")
        assertTrue(dao.getUnsyncedNotifications("pair").isEmpty())
    }

    @Test fun confirmationIsStoredPostedOnceAndClearedWhenResolved() = runBlocking {
        val posted = mutableListOf<String>()
        val cleared = mutableListOf<String>()
        val confirming = NotificationResults(
            context, db,
            postConfirmation = { _, item -> posted.add(item.eventId!!); true },
            clearConfirmation = { cleared.add(it) }
        ) { _, _ -> true }
        val dao = db.notificationDao()
        val id = dao.insertOrIgnore(row("confirm-hash"))
        val event = "00000000-0000-4000-8000-000000000077"
        val waiting = item("confirm-hash", event, "needs_confirmation").copy(
            errorCode = "mapping_confirmation_required",
            mappingProposal = com.alfonsusenrico.financialtracker.listener.data.network.model.MappingProposal(
                name = "GoPay Tabungan", role = "target", confidence = 0.6,
                proposed = com.alfonsusenrico.financialtracker.listener.data.network.model.AccountChoice(
                    "00000000-0000-4000-8000-000000000005", "GoPay"
                )
            )
        )
        assertTrue(confirming.consume(dao.getById(id)!!, waiting, "pair"))
        assertTrue(confirming.consume(dao.getById(id)!!, waiting, "pair"))
        val saved = dao.getById(id)!!
        assertEquals("needs_confirmation", saved.processingStatus)
        assertEquals("mapping_confirmation_required", saved.processingError)
        assertNull(saved.nextCheckAt)
        assertTrue(saved.resultJson!!.contains("GoPay Tabungan"))
        assertEquals(listOf(event), posted)
        assertTrue(cleared.isEmpty())

        assertTrue(confirming.consume(dao.getById(id)!!, item("confirm-hash", event, "queued"), "pair"))
        assertEquals(listOf(event), cleared)
        assertEquals("queued", dao.getById(id)!!.processingStatus)
    }

    @Test fun confirmationWithoutValidProposalIsRejected() = runBlocking {
        val dao = db.notificationDao()
        val id = dao.insertOrIgnore(row("bad-confirm"))
        val invalid = item("bad-confirm", "00000000-0000-4000-8000-000000000078", "needs_confirmation")
        assertFalse(results.consume(dao.getById(id)!!, invalid, "pair"))
        assertEquals("invalid_result", dao.getById(id)!!.syncError)
    }

    @Test fun ignoredRowCanBeResubmittedOnceAsNew() = runBlocking {
        val dao = db.notificationDao()
        val id = dao.insertOrIgnore(row("ignored-hash"))
        assertTrue(results.consume(dao.getById(id)!!, item("ignored-hash", "ignored", "ignored").copy(eventId = null), "pair"))
        assertEquals("ignored", dao.getById(id)!!.processingStatus)
        assertTrue(dao.getUnsyncedNotifications("pair").isEmpty())
        assertEquals(1, dao.resubmitIgnored(id))
        val reset = dao.getById(id)!!
        assertFalse(reset.isSynced)
        assertNull(reset.processingStatus)
        assertEquals(listOf(id), dao.getUnsyncedNotifications("pair").map { it.id })
        assertEquals(0, dao.resubmitIgnored(id))
    }

    @Test fun deletedRowLeavesListsAndQueuesAndBlocksRecapture() = runBlocking {
        val dao = db.notificationDao()
        val pending = dao.insertOrIgnore(row("dismiss-pending"))
        val ignored = dao.insertOrIgnore(row("dismiss-ignored"))
        assertTrue(results.consume(dao.getById(ignored)!!, item("dismiss-ignored", "x", "ignored").copy(eventId = null), "pair"))
        assertEquals(1, dao.dismiss(pending))
        assertEquals(0, dao.dismiss(pending))
        assertTrue(dao.getUnsyncedNotifications("pair").none { it.id == pending })
        assertEquals(0, dao.countUnsubmitted("pair"))
        assertEquals(-1L, dao.insertOrIgnore(row("dismiss-pending")))
        assertEquals(1, dao.dismissAllIgnored())
        assertEquals("dismissed", dao.getById(ignored)!!.processingStatus)
        assertTrue(dao.getIgnoredNotificationsFlow().first().isEmpty())
    }

    @Test fun undoRestoresDeletedRowAndItsPollSchedule() = runBlocking {
        val dao = db.notificationDao()
        val id = dao.insertOrIgnore(row("undo-queued"))
        assertTrue(results.consume(dao.getById(id)!!, item("undo-queued", "00000000-0000-4000-8000-000000000009"), "pair"))
        val before = dao.getById(id)!!
        assertEquals(1, dao.dismiss(id))
        assertEquals(1, dao.restoreDismissed(id, before.processingStatus, before.nextCheckAt))
        val restored = dao.getById(id)!!
        assertEquals("queued", restored.processingStatus)
        assertEquals(before.nextCheckAt, restored.nextCheckAt)
        assertEquals(1, dao.getDueCompletion("pair", Long.MAX_VALUE).size)
        assertEquals(0, dao.restoreDismissed(id, "ignored", null))
    }
}
