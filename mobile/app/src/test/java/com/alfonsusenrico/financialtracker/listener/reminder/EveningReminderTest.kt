package com.alfonsusenrico.financialtracker.listener.reminder

import android.content.Context
import androidx.room.Room
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import com.alfonsusenrico.financialtracker.listener.PurposeFixtures
import com.alfonsusenrico.financialtracker.listener.TestApplication
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.model.RawNotificationEntity
import com.alfonsusenrico.financialtracker.listener.data.network.model.IngestResultItem
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.data.reminder.EveningReminder
import com.alfonsusenrico.financialtracker.listener.data.reminder.EveningReminderNotifier
import com.alfonsusenrico.financialtracker.listener.data.reminder.EveningReminderWorker
import com.alfonsusenrico.financialtracker.listener.data.reminder.QuestionUrgency
import com.alfonsusenrico.financialtracker.listener.data.reminder.ReminderScheduler
import com.alfonsusenrico.financialtracker.listener.data.sync.NotificationResults
import com.alfonsusenrico.financialtracker.listener.ui.UiFixtures
import com.alfonsusenrico.financialtracker.listener.ui.model.OpenQuestions
import com.alfonsusenrico.financialtracker.listener.ui.model.QuestionKind
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.annotation.Config
import java.time.LocalDateTime
import java.time.ZoneId

/** Light questions wait for one evening reminder; urgent ones still interrupt. Scheduling runs on a fake. */
@RunWith(AndroidJUnit4::class)
@Config(sdk = [34], application = TestApplication::class)
class EveningReminderTest {
    private val context = ApplicationProvider.getApplicationContext<Context>()
    private val prefs get() = AppPreferences(context)
    private val zone = ZoneId.of("Asia/Jakarta")
    private fun at(day: Int, hour: Int, minute: Int = 0) = LocalDateTime.of(2026, 11, day, hour, minute).atZone(zone).toInstant().toEpochMilli()

    private class FakeScheduler : ReminderScheduler {
        val planned = mutableListOf<Pair<Long, Boolean>>()
        var cancelled = 0
        override fun schedule(delayMillis: Long, fromReminder: Boolean) { planned += delayMillis to fromReminder }
        override fun cancel() { cancelled++ }
    }

    private lateinit var db: AppDatabase

    @Before fun open() {
        db = Room.inMemoryDatabaseBuilder(context, AppDatabase::class.java).allowMainThreadQueries().build()
    }

    @After fun close() {
        db.close()
        prefs.isEveningReminderEnabled = true
        prefs.eveningReminderMinute = AppPreferences.DEFAULT_REMINDER_MINUTE
    }

    @Test fun theReminderIsOnByDefaultAtEightInTheEvening() {
        assertTrue(prefs.isEveningReminderEnabled)
        assertEquals(20 * 60, prefs.eveningReminderMinute)
        assertEquals("20.00", EveningReminder.label(prefs.eveningReminderMinute))
    }

    @Test fun theNextReminderIsTonightOrTomorrow() {
        assertEquals(at(2, 20), EveningReminder.nextAt(at(2, 14, 5), 20 * 60, zone))
        assertEquals(at(3, 20), EveningReminder.nextAt(at(2, 20), 20 * 60, zone))
        assertEquals(at(3, 20), EveningReminder.nextAt(at(2, 21, 30), 20 * 60, zone))
        assertEquals(at(3, 7, 15), EveningReminder.nextAt(at(2, 21, 30), 7 * 60 + 15, zone))
    }

    @Test fun schedulingFollowsTheSettingOnAFakeScheduler() {
        val scheduler = FakeScheduler()
        val reminder = EveningReminder(prefs, scheduler, zone = { zone }, now = { at(2, 14, 5) })
        reminder.reschedule()
        assertEquals(listOf((at(2, 20) - at(2, 14, 5)) to false), scheduler.planned)

        reminder.setTime(21 * 60 + 30)
        assertEquals(21 * 60 + 30, prefs.eveningReminderMinute)
        assertEquals((at(2, 21, 30) - at(2, 14, 5)) to false, scheduler.planned.last())

        reminder.setEnabled(false)
        assertFalse(prefs.isEveningReminderEnabled)
        assertEquals(1, scheduler.cancelled)
        assertEquals(2, scheduler.planned.size)

        reminder.setEnabled(true)
        assertEquals(3, scheduler.planned.size)
        // After it fires, the reminder plans tomorrow's itself without cancelling the running work.
        EveningReminder(prefs, scheduler, zone = { zone }, now = { at(2, 21, 30) }).reschedule(fromReminder = true)
        assertEquals((at(3, 21, 30) - at(2, 21, 30)) to true, scheduler.planned.last())
    }

    @Test fun anOutOfRangeStoredTimeFallsBackToEightPm() {
        context.getSharedPreferences("financial_tracker_prefs", Context.MODE_PRIVATE).edit().putInt("evening_reminder_minute", 5000).commit()
        assertEquals(20 * 60, prefs.eveningReminderMinute)
    }

    @Test fun onlyPurposeQuestionsAreLightAndUrgentOnesComeFirst() {
        assertTrue(QuestionUrgency.isLight(QuestionKind.PURPOSE))
        listOf(QuestionKind.SENDER, QuestionKind.SOURCE_POCKET, QuestionKind.MAPPING).forEach { assertFalse(QuestionUrgency.isLight(it)) }
        val ordered = QuestionUrgency.ordered(listOf(UiFixtures.purpose(), UiFixtures.sender(), UiFixtures.mapping()), OpenQuestions::kindOf)
        assertEquals(listOf(UiFixtures.sender().id, UiFixtures.mapping().id, UiFixtures.purpose().id), ordered.map { it.id })
    }

    @Test fun theSummaryCountsWaitingLightQuestionsOnly() = runBlocking {
        listOf(UiFixtures.purpose(), UiFixtures.sender(), UiFixtures.recorded(20, "Kopi Contoh", 32_000)).forEach { db.notificationDao().insertOrIgnore(it) }
        assertEquals(1, EveningReminderWorker.lightQuestionCount(db))
        assertEquals("Ada 3 pertanyaan ringan hari ini", EveningReminderNotifier.title(3))
    }

    private suspend fun consume(batch: Boolean): Pair<Int, Int> {
        var purposePrompts = 0
        var recordAlerts = 0
        val results = NotificationResults(
            context, db, postConfirmation = { _, _ -> true }, clearConfirmation = {},
            postPurpose = { _, _, _ -> purposePrompts++; true }, scheduleFollowUp = { _, _ -> },
            batchLightQuestions = { batch }
        ) { _: IngestResultItem, _: Boolean -> recordAlerts++; true }
        val pairing = prefs.pairingContext
        val id = db.notificationDao().insertOrIgnore(RawNotificationEntity(
            deviceId = "fictional", packageName = "com.bca.mybca.omni.android", appLabel = "myBCA", postTime = 1,
            title = "Financial Diary", payloadHash = PurposeFixtures.HASH
        ))
        assertTrue(results.consume(db.notificationDao().getById(id)!!, PurposeFixtures.result(), pairing))
        results.deliverPendingAlerts(pairing)
        return purposePrompts to recordAlerts
    }

    @Test fun withTheReminderOnAPurposeQuestionAlertsAsTheRecordAndWaitsInTheApp() = runBlocking {
        assertEquals(0 to 1, consume(batch = true))
        // The question still waits under "Perlu dijawab" and in the question sheet.
        assertEquals(1, EveningReminderWorker.lightQuestionCount(db))
    }

    @Test fun withTheReminderOffAPurposeQuestionIsPromptedAtOnce() = runBlocking {
        assertEquals(1 to 0, consume(batch = false))
    }

    @Test fun theDefaultFollowsTheSetting() {
        assertTrue(QuestionUrgency.batched(prefs))
        prefs.isEveningReminderEnabled = false
        assertFalse(QuestionUrgency.batched(prefs))
    }
}
