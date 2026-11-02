package com.alfonsusenrico.financialtracker.listener

import android.content.Context
import android.database.sqlite.SQLiteDatabase
import androidx.room.Room
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.sync.AnswerAttempts
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import org.json.JSONObject
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.annotation.Config
import java.io.File

/** Version 8 → 9 adds the answer-attempt record and changes nothing that existed before. */
@RunWith(AndroidJUnit4::class)
@Config(sdk = [34], application = TestApplication::class)
class AnswerAttemptMigrationTest {

    private companion object {
        const val SCHEMA_DIR = "schemas/com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase"
        const val PAIRING = "00000000-0000-4000-8000-000000000501"
        const val EVENT = "00000000-0000-4000-8000-000000000502"
        const val QUESTION = "00000000-0000-4000-8000-000000000503"
        const val REPLY = "00000000-0000-4000-8000-000000000504"
    }

    /** A version 8 database built from its exported schema, as the 2.3.1 app left it. */
    private fun createVersionEight(context: Context, name: String): SQLiteDatabase {
        val schema = JSONObject(File(SCHEMA_DIR, "8.json").readText()).getJSONObject("database")
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
        db.version = 8
        return db
    }

    private fun SQLiteDatabase.tableSql(): Map<String, String> =
        rawQuery("SELECT name, sql FROM sqlite_master WHERE type IN ('table', 'index') AND name NOT LIKE 'sqlite_%' AND name != 'room_master_table'", null)
            .use { cursor -> buildMap { while (cursor.moveToNext()) put(cursor.getString(0), cursor.getString(1).orEmpty()) } }

    @Test fun populatedVersionEightKeepsEveryRowAndGainsAnEmptyAttemptRecord() = runBlocking {
        val name = "fictional_attempt_migration_v8"
        val context = ApplicationProvider.getApplicationContext<Context>()
        val before = createVersionEight(context, name).run {
            execSQL("""INSERT INTO raw_notifications
                (id, device_id, package_name, app_label, post_time, captured_at, payload_hash, is_synced, title,
                 processing_status, processing_error, pairing_context, backend_event_id, check_attempt, next_check_at,
                 capture_outcome, repost_count, result_json)
                VALUES (1, 'fictional-device', 'com.bca.mybca.omni.android', 'myBCA', 1000, 2000, 'fictional-asked', 1,
                        'Transfer masuk', 'needs_confirmation', 'sender_confirmation_required', '$PAIRING', '$EVENT', 5,
                        9000, 'send', 0, '{"status":"needs_confirmation"}')""")
            execSQL("""INSERT INTO sender_replies
                (reply_id, row_id, pairing_context, event_id, question_id, action, name, state, attempt, next_attempt_at, answer_kind)
                VALUES ('$REPLY', 1, '$PAIRING', '$EVENT', '$QUESTION', 'name', NULL, 'accepted', 1, 0, 'sender')""")
            execSQL("INSERT INTO alert_receipts (pairing_context, record_key, delivered_at) VALUES ('$PAIRING', 'confirm:$EVENT:$QUESTION', 4000)")
            tableSql().also { close() }
        }

        val db = Room.databaseBuilder(context, AppDatabase::class.java, name).addMigrations(*AppDatabase.ALL_MIGRATIONS).build()
        try {
            val row = db.notificationDao().getById(1)!!
            assertEquals("needs_confirmation", row.processingStatus)
            assertEquals(5, row.checkAttempt)
            assertEquals(9000L, row.nextCheckAt)
            assertEquals("accepted", db.senderReplyDao().get(REPLY)!!.state)
            db.openHelper.readableDatabase.query("SELECT delivered_at FROM alert_receipts").use {
                assertTrue(it.moveToFirst())
                assertEquals(4000L, it.getLong(0))
            }
            assertTrue(db.answerAttemptDao().recentOutcomesFlow().first().isEmpty())
            AnswerAttempts.record(db, AnswerAttempts.NOTIFICATION, AnswerAttempts.SENDER, 1, EVENT, QUESTION, AnswerAttempts.LOCKED)
            assertEquals(AnswerAttempts.LOCKED, db.answerAttemptDao().forQuestion(EVENT, QUESTION).single().outcome)

            val after = db.openHelper.readableDatabase.query(
                "SELECT name, sql FROM sqlite_master WHERE type IN ('table', 'index') AND name NOT LIKE 'sqlite_%' AND name != 'room_master_table'"
            ).use { cursor -> buildMap { while (cursor.moveToNext()) put(cursor.getString(0), cursor.getString(1).orEmpty()) } }
            assertEquals(before, after.filterKeys { it in before })
            assertEquals(setOf("answer_attempts", "index_answer_attempts_event_id_question_id"), after.keys - before.keys)
        } finally {
            db.close()
            context.deleteDatabase(name)
        }
    }
}
