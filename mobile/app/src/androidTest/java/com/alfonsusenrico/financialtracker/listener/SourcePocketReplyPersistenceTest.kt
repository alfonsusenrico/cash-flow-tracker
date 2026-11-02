package com.alfonsusenrico.financialtracker.listener

import androidx.room.Room
import androidx.test.platform.app.InstrumentationRegistry
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.model.RawNotificationEntity
import com.alfonsusenrico.financialtracker.listener.data.network.model.*
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.data.sync.*
import com.google.gson.Gson
import kotlinx.coroutines.runBlocking
import org.junit.Assert.*
import org.junit.Test
import java.util.UUID

class SourcePocketReplyPersistenceTest {
    private val context = InstrumentationRegistry.getInstrumentation().targetContext
    private fun uuid() = UUID.randomUUID().toString()

    @Test fun selectedPocketReplySurvivesReopenPruningAndPairingChange() = runBlocking {
        check(context.packageName.endsWith(".verification"))
        val prefs = AppPreferences(context)
        prefs.apiKey = "fictional-pocket-key"
        val name = "fictional_pocket_${uuid()}"
        var db = Room.databaseBuilder(context, AppDatabase::class.java, name).build()
        try {
            val event = uuid()
            val question = uuid()
            val account = uuid()
            val pair = prefs.pairingContext
            val scheduled = mutableListOf<String>()
            val item = IngestResultItem(payloadHash = event, eventId = event, status = "needs_confirmation",
                confirmation = ConfirmationQuestion("source_pocket", question, institution = "jago", amount = 112590,
                    currency = "IDR", jagoAccount = AccountChoice(uuid(), "Bank Jago")))
            val id = db.notificationDao().insertOrIgnore(RawNotificationEntity(deviceId = "fictional", packageName = "com.jago.digitalbanking",
                appLabel = "Jago", postTime = 1, title = "Fictional", payloadHash = event, isSynced = true,
                backendEventId = event, pairingContext = pair, processingStatus = "needs_confirmation", resultJson = Gson().toJson(item)))
            val options = SourcePocketOptions(event, question, listOf(AccountChoice(account, "Dana Darurat")))
            db.notificationDao().saveSourcePocketOptions(id, pair, event, Gson().toJson(options))
            val replies = SourcePocketReplies(context, db) { reply, _ -> scheduled.add(reply) }
            assertFalse(replies.enqueue(id, uuid(), account).queued)
            assertFalse(replies.enqueue(id, question, uuid()).queued)
            assertTrue(replies.enqueue(id, question, account).queued)
            assertTrue(replies.enqueue(id, question, account).queued)
            assertEquals(1, scheduled.distinct().size)
            val persisted = db.senderReplyDao().forQuestion(pair, event, question)!!
            assertEquals("source_pocket", persisted.answerKind)
            assertEquals(account, persisted.accountId)
            db.notificationDao().dismiss(id)
            db.notificationDao().pruneIgnoredBefore(Long.MAX_VALUE)
            db.notificationDao().pruneResolvedKeepLatest(0)
            assertNotNull(db.notificationDao().getById(id))
            db.close()
            db = Room.databaseBuilder(context, AppDatabase::class.java, name).build()
            assertEquals(persisted, db.senderReplyDao().get(persisted.replyId))
            SenderReplies(context, db, { reply, _ -> scheduled.add(reply) }, { _, _ -> }).recover()
            assertEquals(persisted.replyId, scheduled.last())
            prefs.apiKey = "fictional-other-pairing"
            assertFalse(SourcePocketReplies(context, db) { _, _ -> fail("Foreign pairing scheduled") }.enqueue(id, question, account).queued)
        } finally {
            db.close()
            context.deleteDatabase(name)
            prefs.apiKey = ""
        }
    }
}
