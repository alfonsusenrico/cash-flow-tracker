package com.alfonsusenrico.financialtracker.listener

import android.content.Context
import android.database.sqlite.SQLiteDatabase
import androidx.room.Room
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.model.TextCaptureEntity
import com.alfonsusenrico.financialtracker.listener.data.model.TextCaptureState
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import org.json.JSONObject
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.annotation.Config
import java.io.File

/** Version 7 → 8 adds the typed captures table and changes nothing that existed before. */
@RunWith(AndroidJUnit4::class)
@Config(sdk = [34], application = TestApplication::class)
class TextCaptureMigrationTest {

    private companion object {
        const val SCHEMA_DIR = "schemas/com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase"
        const val PAIRING = "00000000-0000-4000-8000-000000000401"
        const val EVENT = "00000000-0000-4000-8000-000000000402"
    }

    /** A version 7 database built from its exported schema, as the 2.2.0 app left it. */
    private fun createVersionSeven(context: Context, name: String): SQLiteDatabase {
        val schema = JSONObject(File(SCHEMA_DIR, "7.json").readText()).getJSONObject("database")
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
        db.version = 7
        return db
    }

    private fun SQLiteDatabase.tableSql(): Map<String, String> =
        rawQuery("SELECT name, sql FROM sqlite_master WHERE type IN ('table', 'index') AND name NOT LIKE 'sqlite_%' AND name != 'room_master_table'", null)
            .use { cursor -> buildMap { while (cursor.moveToNext()) put(cursor.getString(0), cursor.getString(1).orEmpty()) } }

    @Test fun populatedVersionSevenKeepsEveryRowAndGainsAnEmptyCaptureTable() = runBlocking {
        val name = "fictional_capture_migration_v7"
        val context = ApplicationProvider.getApplicationContext<Context>()
        val before = createVersionSeven(context, name).run {
            execSQL("""INSERT INTO raw_notifications
                (id, device_id, package_name, app_label, post_time, captured_at, payload_hash, is_synced, title,
                 processing_status, pairing_context, backend_event_id, record_key, check_attempt, capture_outcome,
                 repost_count, purpose_question_id, result_json)
                VALUES (1, 'fictional-device', 'com.bca', 'BCA', 1000, 2000, 'fictional-recorded', 1, 'Transfer',
                        'recorded', '$PAIRING', '$EVENT', 'fictional:1', 0, 'send', 0, NULL, '{"status":"recorded"}')""")
            execSQL("""INSERT INTO raw_notifications
                (id, device_id, package_name, app_label, post_time, captured_at, payload_hash, is_synced, check_attempt,
                 capture_outcome, capture_reason, repost_count)
                VALUES (2, 'fictional-device', 'com.gojek.gopay', 'GoPay', 3000, 3000, 'fictional-kept', 0, 0, 'keep_local', 'promotional', 0)""")
            execSQL("""INSERT INTO sender_replies
                (reply_id, row_id, pairing_context, event_id, question_id, action, name, state, attempt, next_attempt_at,
                 answer_kind, answer_text)
                VALUES ('00000000-0000-4000-8000-000000000403', 1, '$PAIRING', '$EVENT',
                        '00000000-0000-4000-8000-000000000404', 'text', NULL, 'queued', 1, 9000, 'purpose', 'sewa lapangan')""")
            execSQL("INSERT INTO alert_receipts (pairing_context, record_key, delivered_at) VALUES ('$PAIRING', 'fictional:1', 4000)")
            tableSql().also { close() }
        }

        val db = Room.databaseBuilder(context, AppDatabase::class.java, name).addMigrations(*AppDatabase.ALL_MIGRATIONS).build()
        try {
            val dao = db.notificationDao()
            assertEquals("fictional-recorded", dao.getById(1)!!.payloadHash)
            assertEquals("""{"status":"recorded"}""", dao.getById(1)!!.resultJson)
            assertEquals("keep_local", dao.getById(2)!!.captureOutcome)
            assertEquals("sewa lapangan", db.senderReplyDao().get("00000000-0000-4000-8000-000000000403")!!.answerText)
            db.openHelper.readableDatabase.query("SELECT delivered_at FROM alert_receipts").use {
                assertTrue(it.moveToFirst())
                assertEquals(4000L, it.getLong(0))
            }
            val captures = db.textCaptureDao()
            assertTrue(captures.observeVisible().first().isEmpty())
            val id = captures.insert(
                TextCaptureEntity(clientCaptureId = "00000000-0000-4000-8000-000000000405", text = "kopi 25rb",
                    deviceTime = "2027-01-15T15:00:00+07:00", createdAt = 5000)
            )
            assertEquals(TextCaptureState.QUEUED, captures.getById(id)!!.state)

            // Only the tables of versions 8 and 9 and their indices were added; every existing definition is unchanged.
            val after = db.openHelper.readableDatabase.query(
                "SELECT name, sql FROM sqlite_master WHERE type IN ('table', 'index') AND name NOT LIKE 'sqlite_%' AND name != 'room_master_table'"
            ).use { cursor -> buildMap { while (cursor.moveToNext()) put(cursor.getString(0), cursor.getString(1).orEmpty()) } }
            assertEquals(before, after.filterKeys { it in before })
            assertEquals(
                setOf(
                    "text_captures", "index_text_captures_client_capture_id", "index_text_captures_state",
                    "answer_attempts", "index_answer_attempts_event_id_question_id"
                ),
                after.keys - before.keys
            )
        } finally {
            db.close()
            context.deleteDatabase(name)
        }
    }

    @Test fun aRepeatedClientCaptureIdIsRefused() = runBlocking {
        val context = ApplicationProvider.getApplicationContext<Context>()
        val db = Room.inMemoryDatabaseBuilder(context, AppDatabase::class.java).build()
        try {
            val capture = TextCaptureEntity(clientCaptureId = "00000000-0000-4000-8000-000000000406", text = "kopi 25rb",
                deviceTime = "2027-01-15T15:00:00+07:00", createdAt = 5000)
            db.textCaptureDao().insert(capture)
            val refused = runCatching { db.textCaptureDao().insert(capture) }
            assertTrue(refused.isFailure)
        } finally {
            db.close()
        }
    }
}
