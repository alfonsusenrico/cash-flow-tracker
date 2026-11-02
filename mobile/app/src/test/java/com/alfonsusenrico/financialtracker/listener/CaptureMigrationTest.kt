package com.alfonsusenrico.financialtracker.listener

import android.app.Application
import android.content.Context
import android.database.sqlite.SQLiteDatabase
import androidx.room.Room
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.json.JSONObject
import org.junit.Assert.assertNull
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.annotation.Config
import java.io.File

@RunWith(AndroidJUnit4::class)
@Config(sdk = [34], application = com.alfonsusenrico.financialtracker.listener.TestApplication::class)
class CaptureMigrationTest {

    private companion object {
        // Gradle runs unit tests from the module directory.
        const val SCHEMA_DIR = "schemas/com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase"
    }

    /**
     * Creates a version 5 database from its exported schema. Robolectric cannot read the schemas as
     * assets, so this replaces MigrationTestHelper.createDatabase; Room's own open-time validation
     * then compares the migrated tables with the version 6 entities, as runMigrationsAndValidate does.
     */
    private fun createVersionFive(context: Context, name: String): SQLiteDatabase {
        val schema = JSONObject(File(SCHEMA_DIR, "5.json").readText()).getJSONObject("database")
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
        db.version = 5
        return db
    }

    @Test fun populatedVersionFiveBecomesSendRows() = runBlocking {
        val name = "fictional_capture_migration_v5"
        val context = ApplicationProvider.getApplicationContext<Context>()
        createVersionFive(context, name).apply {
            execSQL("""INSERT INTO raw_notifications
                (id, device_id, package_name, app_label, notification_key, post_time, captured_at, payload_hash,
                 is_synced, title, body_text, check_attempt)
                VALUES (1, 'fictional-device', 'com.gojek.gopay', 'GoPay', 'fictional-key', 1000, 2000,
                        'fictional-unsynced', 0, 'Pembayaran berhasil', 'Kamu baru aja bayar Rp45.000 ke Contoh pake GoPay', 0)""")
            execSQL("""INSERT INTO raw_notifications
                (id, device_id, package_name, app_label, post_time, captured_at, payload_hash, is_synced,
                 title, processing_status, pairing_context, check_attempt)
                VALUES (2, 'fictional-device', 'com.jago.digitalbanking', 'Bank Jago', 3000, 4000,
                        'fictional-recorded', 1, 'Jago', 'recorded', 'fictional-pairing', 0)""")
            close()
        }

        val db = Room.databaseBuilder(context, AppDatabase::class.java, name).addMigrations(*AppDatabase.ALL_MIGRATIONS).build()
        try {
            val dao = db.notificationDao()
            val unsynced = dao.getById(1)!!
            assertEquals("send", unsynced.captureOutcome)
            assertNull(unsynced.captureReason)
            assertNull(unsynced.notificationWhen)
            assertEquals(0, unsynced.repostCount)
            assertNull(unsynced.duplicateOfId)
            assertEquals("fictional-unsynced", unsynced.payloadHash)
            assertEquals(listOf(1L), dao.getUnsyncedNotifications("fictional-pairing").map { it.id })
            assertEquals("recorded", dao.getById(2)!!.processingStatus)
            assertEquals(2, dao.getTotalCountFlow().first())
            assertEquals(0, dao.getLocalOnlyFlow().first().size)
        } finally {
            db.close()
            context.deleteDatabase(name)
        }
    }
}
