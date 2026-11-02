package com.alfonsusenrico.financialtracker.listener

import androidx.room.Room
import androidx.test.platform.app.InstrumentationRegistry
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.model.RawNotificationEntity
import com.alfonsusenrico.financialtracker.listener.data.network.model.*
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.data.sync.*
import kotlinx.coroutines.runBlocking
import org.junit.Assert.*
import org.junit.Test
import java.util.UUID

class SenderReplyPersistenceTest {
    private val context = InstrumentationRegistry.getInstrumentation().targetContext
    private val prefs = AppPreferences(context)
    private fun uuid() = UUID.randomUUID().toString()

    @Test fun duplicateOfflineReplySurvivesDatabaseReopenAndPruning() = runBlocking {
        check(context.packageName.endsWith(".verification"))
        prefs.apiKey = "fictional-test-key"
        val database = "fictional_sender_${uuid()}"
        var db = Room.databaseBuilder(context, AppDatabase::class.java, database).build()
        val event = uuid()
        val question = uuid()
        val pair = prefs.pairingContext
        val scheduled = mutableListOf<String>()
        try {
            val rowId = db.notificationDao().insertOrIgnore(RawNotificationEntity(deviceId = "fictional", packageName = "com.bca", appLabel = "BCA", postTime = 1,
                title = "Fictional transfer", payloadHash = "fictional-$event"))
            val item = IngestResultItem(payloadHash = "fictional-$event", eventId = event, status = "needs_confirmation",
                confirmation = ConfirmationQuestion("sender", question, "AN**RA**", "bca", AccountChoice(uuid(), "BCA"), 112590, "IDR"))
            val results = NotificationResults(context, db, postConfirmation = { _, _ -> false })
            assertTrue(results.consume(db.notificationDao().getById(rowId)!!, item, pair))
            val store = SenderReplies(context, db, { id, _ -> scheduled.add(id) }, { _, _ -> })
            assertFalse(store.enqueue(rowId, event, question, pair, "name", " ").queued)
            assertFalse(store.enqueue(rowId, event, uuid(), pair, "name", "Andra").queued)
            assertTrue(store.enqueue(rowId, event, question, pair, "name", "Andra").queued)
            assertFalse(store.enqueue(rowId, event, question, pair, "name", "Different name").queued)
            val persisted = db.senderReplyDao().forQuestion(pair, event, question)!!
            assertEquals("Andra", persisted.name)
            assertEquals(1, scheduled.distinct().size)
            db.notificationDao().pruneResolvedKeepLatest(0)
            db.notificationDao().pruneIgnoredBefore(Long.MAX_VALUE)
            assertNotNull(db.notificationDao().getById(rowId))
            db.close()
            db = Room.databaseBuilder(context, AppDatabase::class.java, database).build()
            assertEquals(persisted, db.senderReplyDao().get(persisted.replyId))
            SenderReplies(context, db, { id, _ -> scheduled.add(id) }, { _, _ -> }).recover()
            assertEquals(persisted.replyId, scheduled.last())
            prefs.apiKey = "fictional-different-pairing"
            assertFalse(SenderReplies(context, db, { _, _ -> fail("Foreign pairing scheduled") }, { _, _ -> })
                .enqueue(rowId, event, question, pair, "unknown", null).queued)
            db.senderReplyDao().accept(persisted.replyId)
            assertNull(db.senderReplyDao().get(persisted.replyId)!!.name)
        } finally { db.close(); context.deleteDatabase(database); prefs.apiKey = "" }
    }

    @Test fun sequentialQuestionsHaveDistinctAlertsAndDismissalDoesNotDropQuestion() = runBlocking {
        val db = Room.inMemoryDatabaseBuilder(context, AppDatabase::class.java).build()
        try {
            val event = uuid()
            val posted = mutableListOf<String>()
            val results = NotificationResults(context, db, postConfirmation = { _, item -> posted.add(item.confirmation!!.questionId!!); true })
            val id = db.notificationDao().insertOrIgnore(RawNotificationEntity(deviceId = "fictional", packageName = "com.bca", appLabel = "BCA", postTime = 1,
                title = "Fictional", payloadHash = event))
            val account = IngestResultItem(payloadHash = event, eventId = event, status = "needs_confirmation",
                mappingProposal = MappingProposal("BCA", "observed", 0.7, AccountChoice(uuid(), "BCA")),
                confirmation = ConfirmationQuestion("account", uuid()))
            results.consume(db.notificationDao().getById(id)!!, account, "pair")
            results.consume(db.notificationDao().getById(id)!!, account, "pair")
            val sender = account.copy(mappingProposal = null, confirmation = ConfirmationQuestion("sender", uuid(), "AN**RA**", "bca", AccountChoice(uuid(), "BCA"), 112590, "IDR"))
            results.consume(db.notificationDao().getById(id)!!, sender, "pair")
            results.consume(db.notificationDao().getById(id)!!, sender, "pair")
            assertEquals(2, posted.size)
            assertEquals(2, posted.distinct().size)
            db.notificationDao().dismiss(id)
            db.notificationDao().pruneIgnoredBefore(Long.MAX_VALUE)
            db.notificationDao().pruneResolvedKeepLatest(0)
            assertNotNull(db.notificationDao().getById(id))
            assertFalse(results.consume(db.notificationDao().getById(id)!!, sender.copy(confirmation = sender.confirmation!!.copy(type = "future")), "pair"))
        } finally { db.close() }
    }
}
