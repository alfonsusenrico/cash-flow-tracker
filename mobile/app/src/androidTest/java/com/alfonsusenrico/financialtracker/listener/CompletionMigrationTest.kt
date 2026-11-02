package com.alfonsusenrico.financialtracker.listener

import androidx.room.Room
import androidx.room.testing.MigrationTestHelper
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import kotlinx.coroutines.runBlocking
import org.junit.Assert.*
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(AndroidJUnit4::class)
class CompletionMigrationTest {
    @get:Rule val helper = MigrationTestHelper(
        InstrumentationRegistry.getInstrumentation(), AppDatabase::class.java
    )

    @Test fun populatedVersionOnePreservesHistoricalRowsWithoutReplay() = verifyUpgrade(1)
    @Test fun populatedVersionTwoPreservesReferencesWithoutRetroactiveAlert() = verifyUpgrade(2)
    @Test fun populatedVersionThreePreservesQuestionsAndReceipts() = verifyUpgrade(3)
    @Test fun populatedVersionFourPreservesPendingSenderReply() = verifyUpgrade(4)
    @Test fun populatedVersionFiveBecomesSendRows() = verifyUpgrade(5)

    private fun verifyUpgrade(version: Int) = runBlocking {
        val name = "fictional_migration_v$version"
        helper.createDatabase(name, version).apply {
            execSQL("""INSERT INTO raw_notifications
                (id, device_id, package_name, app_label, post_time, captured_at, payload_hash,
                 is_synced, title, body_text, is_financial, expected_amount, event_class)
                VALUES (1, 'fictional-device', 'com.jago.digitalbanking', 'Bank Jago',
                        1000, 2000, 'migration-fictional', 1, 'Jago', 'Fictional payment', 1, 125000, 'expense')""")
            if (version == 2) execSQL("UPDATE raw_notifications SET server_transaction_id = 'notification:fictional' WHERE id = 1")
            if (version >= 3) {
                execSQL("UPDATE raw_notifications SET processing_status = 'needs_confirmation', pairing_context = 'fictional-pairing' WHERE id = 1")
                execSQL("INSERT INTO alert_receipts VALUES ('fictional-pairing', 'confirm:fictional', 2000)")
            }
            if (version == 4) {
                execSQL("INSERT INTO sender_replies VALUES ('fictional-reply',1,'fictional-pairing','fictional-event','fictional-question','name','Andra','queued',2,3000,NULL)")
            }
            close()
        }
        helper.runMigrationsAndValidate(name, 6, true, *AppDatabase.ALL_MIGRATIONS).close()
        val context = InstrumentationRegistry.getInstrumentation().targetContext
        val db = Room.databaseBuilder(context, AppDatabase::class.java, name)
            .addMigrations(*AppDatabase.ALL_MIGRATIONS).build()
        try {
            val row = db.notificationDao().getById(1)!!
            assertEquals("Fictional payment", row.bodyText)
            assertEquals("migration-fictional", row.payloadHash)
            assertEquals("send", row.captureOutcome)
            assertEquals(0, row.repostCount)
            assertTrue(row.isSynced)
            assertNull(row.backendEventId)
            if (version >= 3) {
                assertEquals("needs_confirmation", row.processingStatus)
                assertEquals("fictional-pairing", row.pairingContext)
                assertEquals(2000L, db.notificationDao().getAlertReceipt("fictional-pairing", "confirm:fictional")!!.deliveredAt)
            } else assertNull(row.processingStatus)
            if (version == 4) {
                val reply = db.senderReplyDao().get("fictional-reply")!!
                assertEquals("sender", reply.answerKind)
                assertNull(reply.accountId)
                assertEquals("Andra", reply.name)
                assertEquals("queued", reply.state)
                assertEquals(2, reply.attempt)
            }
            assertNull(row.editableTransactionId)
            if (version == 2) assertEquals("notification:fictional", row.serverTransactionId)
            assertTrue(db.notificationDao().getUnsyncedNotifications("new-pairing").isEmpty())
            assertTrue(db.notificationDao().getDueCompletion("new-pairing", Long.MAX_VALUE).isEmpty())
            assertTrue(db.notificationDao().getPendingAlerts("new-pairing").isEmpty())
        } finally {
            db.close()
            context.deleteDatabase(name)
        }
    }
}
