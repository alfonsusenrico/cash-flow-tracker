package com.alfonsusenrico.financialtracker.listener

import android.content.Context
import androidx.room.Room
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import com.alfonsusenrico.financialtracker.listener.PurposeFixtures.HASH
import com.alfonsusenrico.financialtracker.listener.PurposeFixtures.resultJson
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.model.RawNotificationEntity
import com.alfonsusenrico.financialtracker.listener.data.network.model.IngestResultItem
import com.alfonsusenrico.financialtracker.listener.data.network.model.RecordedHabit
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.data.sync.NotificationResults
import com.alfonsusenrico.financialtracker.listener.ui.model.RowPresentation
import com.google.gson.Gson
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.annotation.Config

/** A habit the tracker reports with a recorded result is kept with the row's stored result, without a schema change. */
@RunWith(AndroidJUnit4::class)
@Config(sdk = [34], application = TestApplication::class)
class HabitResultTest {
    private val context = ApplicationProvider.getApplicationContext<Context>()
    private lateinit var db: AppDatabase
    private lateinit var pairing: String
    private var rowId = 0L

    private val results get() = NotificationResults(
        context, db, postConfirmation = { _, _ -> true }, clearConfirmation = {}, postPurpose = { _, _, _ -> true },
        scheduleFollowUp = { _, _ -> }
    ) { _, _ -> true }

    @Before fun open() = runBlocking {
        AppPreferences(context).apiKey = "fictional-habit-key"
        pairing = AppPreferences(context).pairingContext
        db = Room.inMemoryDatabaseBuilder(context, AppDatabase::class.java).build()
        rowId = db.notificationDao().insertOrIgnore(RawNotificationEntity(
            deviceId = "fictional", packageName = "com.gojek.gopay", appLabel = "GoPay", postTime = 1,
            title = "Pembayaran berhasil", payloadHash = HASH
        ))
    }

    @After fun close() {
        db.close()
        AppPreferences(context).apiKey = ""
    }

    private suspend fun row() = db.notificationDao().getById(rowId)!!

    private suspend fun consume(json: String) {
        assertTrue(results.consume(row(), Gson().fromJson(json, IngestResultItem::class.java), pairing))
    }

    private fun withHabit(json: String, habit: String) = json.trimEnd().removeSuffix("}") + """, "habit": $habit}"""

    @Test fun aHabitRecordedResultIsStoredAndShown() = runBlocking {
        consume(withHabit(resultJson(question = null, reviewStatus = "reviewed"),
            """{"label": "Dicatat dari kebiasaan: Makan & Minum", "basis": "3 dari 3 jawaban sebelumnya"}"""))
        val stored = row()
        assertTrue(stored.resultJson!!.contains("3 dari 3 jawaban sebelumnya"))
        assertEquals(
            RecordedHabit("Dicatat dari kebiasaan: Makan & Minum", "3 dari 3 jawaban sebelumnya"),
            RowPresentation.of(stored).habit
        )
    }

    @Test fun anOlderTrackerLeavesTheRowWithoutAHabit() = runBlocking {
        consume(resultJson(question = null, reviewStatus = "reviewed"))
        assertNull(RowPresentation.of(row()).habit)
    }

    @Test fun aLaterResultWithoutTheHabitRemovesTheTag() = runBlocking {
        consume(withHabit(resultJson(question = null, reviewStatus = "reviewed"),
            """{"label": "Dicatat dari kebiasaan: Makan & Minum", "basis": "3 dari 3 jawaban sebelumnya"}"""))
        // The owner changed the category on the web, so the tracker no longer reports a habit.
        consume(resultJson(question = null, reviewStatus = "reviewed"))
        assertNull(RowPresentation.of(row()).habit)
    }
}
