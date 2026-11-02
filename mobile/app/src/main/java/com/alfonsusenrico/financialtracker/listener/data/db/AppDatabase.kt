package com.alfonsusenrico.financialtracker.listener.data.db

import android.content.Context
import androidx.room.Database
import androidx.room.Room
import androidx.room.RoomDatabase
import androidx.room.migration.Migration
import androidx.sqlite.db.SupportSQLiteDatabase
import com.alfonsusenrico.financialtracker.listener.data.model.AlertReceiptEntity
import com.alfonsusenrico.financialtracker.listener.data.model.AnswerAttemptEntity
import com.alfonsusenrico.financialtracker.listener.data.model.RawNotificationEntity
import com.alfonsusenrico.financialtracker.listener.data.model.SenderReplyEntity
import com.alfonsusenrico.financialtracker.listener.data.model.TextCaptureEntity

@Database(
    entities = [
        RawNotificationEntity::class, AlertReceiptEntity::class, SenderReplyEntity::class, TextCaptureEntity::class,
        AnswerAttemptEntity::class
    ],
    version = 9,
    exportSchema = true
)
abstract class AppDatabase : RoomDatabase() {

    abstract fun notificationDao(): NotificationDao
    abstract fun senderReplyDao(): SenderReplyDao
    abstract fun textCaptureDao(): TextCaptureDao
    abstract fun answerAttemptDao(): AnswerAttemptDao

    companion object {
        /** Adds the answer-attempt record and nothing else; every existing table and row stays as it is. */
        val MIGRATION_8_9 = object : Migration(8, 9) {
            override fun migrate(db: SupportSQLiteDatabase) {
                db.execSQL(CREATE_ANSWER_ATTEMPTS)
                db.execSQL(CREATE_ANSWER_ATTEMPTS_INDEX)
            }
        }
        // Exactly the exported schema 9, so Room's validation accepts a migrated database.
        private const val CREATE_ANSWER_ATTEMPTS = "CREATE TABLE IF NOT EXISTS `answer_attempts` (" +
            "`id` INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, `created_at` INTEGER NOT NULL, `channel` TEXT NOT NULL, " +
            "`answer_kind` TEXT NOT NULL, `row_id` INTEGER, `event_id` TEXT, `question_id` TEXT, `outcome` TEXT NOT NULL, " +
            "`app_version` TEXT NOT NULL)"
        private const val CREATE_ANSWER_ATTEMPTS_INDEX =
            "CREATE INDEX IF NOT EXISTS `index_answer_attempts_event_id_question_id` ON `answer_attempts` (`event_id`, `question_id`)"
        /** Adds the typed captures and nothing else; every existing table and row stays as it is. */
        val MIGRATION_7_8 = object : Migration(7, 8) {
            override fun migrate(db: SupportSQLiteDatabase) {
                db.execSQL(CREATE_TEXT_CAPTURES)
                db.execSQL("CREATE UNIQUE INDEX IF NOT EXISTS `index_text_captures_client_capture_id` ON `text_captures` (`client_capture_id`)")
                db.execSQL("CREATE INDEX IF NOT EXISTS `index_text_captures_state` ON `text_captures` (`state`)")
            }
        }
        // Exactly the exported schema 8, so Room's validation accepts a migrated database.
        private const val CREATE_TEXT_CAPTURES = "CREATE TABLE IF NOT EXISTS `text_captures` (" +
            "`id` INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, `client_capture_id` TEXT NOT NULL, `text` TEXT NOT NULL, " +
            "`device_time` TEXT NOT NULL, `created_at` INTEGER NOT NULL, `state` TEXT NOT NULL, `server_id` TEXT, " +
            "`pairing_context` TEXT, `question_json` TEXT, `summary_json` TEXT, `transaction_id` TEXT, `undo_until` TEXT, " +
            "`undo_deadline` INTEGER, `pending_answer_json` TEXT, `attempts` INTEGER NOT NULL, `claimed_at` INTEGER, " +
            "`error_code` TEXT, `edited` INTEGER NOT NULL)"
        val MIGRATION_6_7 = object : Migration(6, 7) {
            override fun migrate(db: SupportSQLiteDatabase) {
                db.execSQL("ALTER TABLE raw_notifications ADD COLUMN purpose_question_id TEXT")
                db.execSQL("ALTER TABLE raw_notifications ADD COLUMN purpose_question_error TEXT")
                db.execSQL("ALTER TABLE sender_replies ADD COLUMN suggestion_id TEXT")
                db.execSQL("ALTER TABLE sender_replies ADD COLUMN answer_text TEXT")
            }
        }
        val MIGRATION_5_6 = object : Migration(5, 6) {
            override fun migrate(db: SupportSQLiteDatabase) {
                // Rows captured before classification were already in the send pipeline.
                db.execSQL("ALTER TABLE raw_notifications ADD COLUMN capture_outcome TEXT NOT NULL DEFAULT 'send'")
                db.execSQL("ALTER TABLE raw_notifications ADD COLUMN capture_reason TEXT")
                db.execSQL("ALTER TABLE raw_notifications ADD COLUMN notification_when INTEGER")
                db.execSQL("ALTER TABLE raw_notifications ADD COLUMN content_fingerprint TEXT")
                db.execSQL("ALTER TABLE raw_notifications ADD COLUMN normalized_fingerprint TEXT")
                db.execSQL("ALTER TABLE raw_notifications ADD COLUMN last_posted_at INTEGER")
                db.execSQL("ALTER TABLE raw_notifications ADD COLUMN repost_count INTEGER NOT NULL DEFAULT 0")
                db.execSQL("ALTER TABLE raw_notifications ADD COLUMN duplicate_of_id INTEGER")
                db.execSQL("CREATE INDEX IF NOT EXISTS index_raw_notifications_notification_key ON raw_notifications(notification_key)")
                db.execSQL("CREATE INDEX IF NOT EXISTS index_raw_notifications_normalized_fingerprint ON raw_notifications(normalized_fingerprint)")
                db.execSQL("CREATE INDEX IF NOT EXISTS index_raw_notifications_capture_outcome ON raw_notifications(capture_outcome)")
            }
        }
        val MIGRATION_4_5 = object : Migration(4, 5) {
            override fun migrate(db: SupportSQLiteDatabase) {
                db.execSQL("ALTER TABLE sender_replies ADD COLUMN answer_kind TEXT NOT NULL DEFAULT 'sender'")
                db.execSQL("ALTER TABLE sender_replies ADD COLUMN account_id TEXT")
                db.execSQL("ALTER TABLE raw_notifications ADD COLUMN source_pocket_options_json TEXT")
            }
        }
        val MIGRATION_3_4 = object : Migration(3, 4) {
            override fun migrate(db: SupportSQLiteDatabase) {
                db.execSQL("""CREATE TABLE sender_replies (
                    reply_id TEXT NOT NULL PRIMARY KEY, row_id INTEGER NOT NULL,
                    pairing_context TEXT NOT NULL, event_id TEXT NOT NULL, question_id TEXT NOT NULL,
                    action TEXT NOT NULL, name TEXT, state TEXT NOT NULL, attempt INTEGER NOT NULL,
                    next_attempt_at INTEGER NOT NULL, error_code TEXT,
                    FOREIGN KEY(row_id) REFERENCES raw_notifications(id) ON UPDATE NO ACTION ON DELETE CASCADE
                )""")
                db.execSQL("CREATE INDEX index_sender_replies_row_id ON sender_replies(row_id)")
                db.execSQL("CREATE UNIQUE INDEX index_sender_replies_pairing_context_event_id_question_id ON sender_replies(pairing_context, event_id, question_id)")
            }
        }
        val MIGRATION_1_3 = object : Migration(1, 3) {
            override fun migrate(db: SupportSQLiteDatabase) {
                db.execSQL("ALTER TABLE raw_notifications ADD COLUMN server_transaction_id TEXT")
                addCompletionColumns(db)
            }
        }

        val MIGRATION_2_3 = object : Migration(2, 3) {
            override fun migrate(db: SupportSQLiteDatabase) = addCompletionColumns(db)
        }

        private fun addCompletionColumns(db: SupportSQLiteDatabase) {
            db.execSQL("ALTER TABLE raw_notifications ADD COLUMN backend_event_id TEXT")
            db.execSQL("ALTER TABLE raw_notifications ADD COLUMN processing_status TEXT")
            db.execSQL("ALTER TABLE raw_notifications ADD COLUMN processing_error TEXT")
            db.execSQL("ALTER TABLE raw_notifications ADD COLUMN record_key TEXT")
            db.execSQL("ALTER TABLE raw_notifications ADD COLUMN editable_transaction_id TEXT")
            db.execSQL("ALTER TABLE raw_notifications ADD COLUMN pairing_context TEXT")
            db.execSQL("ALTER TABLE raw_notifications ADD COLUMN next_check_at INTEGER")
            db.execSQL("ALTER TABLE raw_notifications ADD COLUMN check_attempt INTEGER NOT NULL DEFAULT 0")
            db.execSQL("ALTER TABLE raw_notifications ADD COLUMN result_json TEXT")
            db.execSQL("""CREATE TABLE IF NOT EXISTS alert_receipts (
                pairing_context TEXT NOT NULL,
                record_key TEXT NOT NULL,
                delivered_at INTEGER,
                PRIMARY KEY(pairing_context, record_key)
            )""")
        }

        val ALL_MIGRATIONS = arrayOf(MIGRATION_1_3, MIGRATION_2_3, MIGRATION_3_4, MIGRATION_4_5, MIGRATION_5_6, MIGRATION_6_7, MIGRATION_7_8, MIGRATION_8_9)

        @Volatile
        private var INSTANCE: AppDatabase? = null

        fun getDatabase(context: Context): AppDatabase {
            return INSTANCE ?: synchronized(this) {
                val instance = Room.databaseBuilder(
                    context.applicationContext,
                    AppDatabase::class.java,
                    "financial_tracker_listener.db"
                )
                    .addMigrations(*ALL_MIGRATIONS)
                    .build()
                INSTANCE = instance
                instance
            }
        }
    }
}
