package com.alfonsusenrico.financialtracker.listener

import android.app.Application
import android.content.Context
import android.database.sqlite.SQLiteDatabase
import androidx.room.Room
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.model.SenderReplyEntity
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import org.json.JSONObject
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.annotation.Config
import java.io.File

@RunWith(AndroidJUnit4::class)
@Config(sdk = [34], application = com.alfonsusenrico.financialtracker.listener.TestApplication::class)
class PurposeMigrationTest {

    private companion object {
        // Gradle runs unit tests from the module directory.
        const val SCHEMA_DIR = "schemas/com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase"
        const val PAIRING = "00000000-0000-4000-8000-000000000301"
        const val EVENT = "00000000-0000-4000-8000-000000000302"
        const val QUESTION = "00000000-0000-4000-8000-000000000303"
    }

    /**
     * Builds a version 6 database from its exported schema (Robolectric cannot read schemas as assets);
     * opening it through Room then validates the migrated tables against the version 7 entities.
     */
    private fun createVersionSix(context: Context, name: String): SQLiteDatabase {
        val schema = JSONObject(File(SCHEMA_DIR, "6.json").readText()).getJSONObject("database")
        val file = context.getDatabasePath(name).apply { parentFile?.mkdirs() }
        val db = SQLiteDatabase.openOrCreateDatabase(file, null)
        val entities = schema.getJSONArray("entities")
        for (i in 0 until entities.length()) {
            val entity = entities.getJSONObject(i)
            val table = entity.getString("tableName")
            db.execSQL(entity.getString("createSql").replace("\${TABLE_NAME}", table))
            val indices = entity.optJSONArray("indices") ?: continue
            for (j in 0 until indices.length()) {
                db.execSQL(indices.getJSONObject(j).getString("createSql").replace("\${TABLE_NAME}", table))
            }
        }
        val setup = schema.getJSONArray("setupQueries")
        for (i in 0 until setup.length()) db.execSQL(setup.getString(i))
        db.version = 6
        return db
    }

    @Test fun populatedVersionSixKeepsRowsAndRepliesAndGainsEmptyPurposeFields() = runBlocking {
        val name = "fictional_purpose_migration_v6"
        val context = ApplicationProvider.getApplicationContext<Context>()
        createVersionSix(context, name).apply {
            execSQL("""INSERT INTO raw_notifications
                (id, device_id, package_name, app_label, post_time, captured_at, payload_hash, is_synced, title,
                 processing_status, pairing_context, backend_event_id, record_key, check_attempt, capture_outcome, repost_count)
                VALUES (1, 'fictional-device', 'com.bca', 'BCA', 1000, 2000, 'fictional-recorded', 1, 'Transfer',
                        'needs_confirmation', '$PAIRING', '$EVENT', NULL, 0, 'send', 0)""")
            execSQL("""INSERT INTO sender_replies
                (reply_id, row_id, pairing_context, event_id, question_id, action, name, state, attempt,
                 next_attempt_at, error_code, answer_kind, account_id)
                VALUES ('00000000-0000-4000-8000-000000000304', 1, '$PAIRING', '$EVENT',
                        '00000000-0000-4000-8000-000000000305', 'name', 'Fiktif', 'queued', 2, 5000, NULL, 'sender', NULL)""")
            close()
        }

        val db = Room.databaseBuilder(context, AppDatabase::class.java, name).addMigrations(*AppDatabase.ALL_MIGRATIONS).build()
        try {
            val row = db.notificationDao().getById(1)!!
            assertEquals("fictional-recorded", row.payloadHash)
            assertEquals("needs_confirmation", row.processingStatus)
            assertNull(row.purposeQuestionId)
            assertNull(row.purposeQuestionError)
            val sender = db.senderReplyDao().get("00000000-0000-4000-8000-000000000304")!!
            assertEquals("Fiktif", sender.name)
            assertEquals(2, sender.attempt)
            assertNull(sender.suggestionId)
            assertNull(sender.answerText)
            assertEquals(listOf(sender.replyId), db.senderReplyDao().recoverable(PAIRING).map { it.replyId })

            db.openHelper.writableDatabase.execSQL(
                "UPDATE raw_notifications SET processing_status = 'recorded', purpose_question_id = '$QUESTION' WHERE id = 1"
            )
            assertEquals(listOf(1L), db.notificationDao().getNeedsAnswerFlow().first().map { it.id })
            db.senderReplyDao().insert(SenderReplyEntity(
                replyId = "00000000-0000-4000-8000-000000000306", rowId = 1, pairingContext = PAIRING, eventId = EVENT,
                questionId = QUESTION, action = "text", name = null, answerKind = "purpose", answerText = "sewa lapangan"
            ))
            assertTrue(db.notificationDao().getNeedsAnswerFlow().first().isEmpty())
            assertEquals("sewa lapangan", db.senderReplyDao().forQuestion(PAIRING, EVENT, QUESTION)!!.answerText)
        } finally {
            db.close()
            context.deleteDatabase(name)
        }
    }
}
