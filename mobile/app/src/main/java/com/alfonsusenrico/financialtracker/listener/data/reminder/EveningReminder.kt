package com.alfonsusenrico.financialtracker.listener.data.reminder

import android.content.Context
import androidx.work.CoroutineWorker
import androidx.work.ExistingWorkPolicy
import androidx.work.OneTimeWorkRequestBuilder
import androidx.work.WorkManager
import androidx.work.WorkerParameters
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.ui.model.OpenQuestions
import com.alfonsusenrico.financialtracker.listener.ui.model.QuestionKind
import kotlinx.coroutines.flow.first
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime
import java.util.concurrent.TimeUnit

/**
 * Light questions wait for one evening reminder (owner decisions 2 and 3 of 2026-11-02). Purpose questions
 * ("Apa ini?", including habit guesses) are light; sender, source-pocket and account questions are urgent
 * and still interrupt at once. With the reminder turned off, light questions are prompted at once again.
 */
object QuestionUrgency {
    fun isLight(kind: QuestionKind?): Boolean = kind == QuestionKind.PURPOSE

    /** Whether a light question waits for the evening instead of its own prompt. */
    fun batched(prefs: AppPreferences): Boolean = prefs.isEveningReminderEnabled

    /** Urgent questions first, each group newest first, as the hub and the question sheet list them. */
    fun <T> ordered(questions: List<T>, kind: (T) -> QuestionKind?): List<T> =
        questions.sortedBy { if (isLight(kind(it))) 1 else 0 }
}

/** Plans the reminder's next run; WorkManager in the app, a fake in tests. */
interface ReminderScheduler {
    fun schedule(delayMillis: Long, fromReminder: Boolean)
    fun cancel()
}

class WorkManagerReminderScheduler(private val context: Context) : ReminderScheduler {
    override fun schedule(delayMillis: Long, fromReminder: Boolean) {
        val request = OneTimeWorkRequestBuilder<EveningReminderWorker>()
            .setInitialDelay(delayMillis.coerceAtLeast(0), TimeUnit.MILLISECONDS)
            .build()
        // The reminder plans its own successor; replacing would cancel the run that is planning it.
        WorkManager.getInstance(context).enqueueUniqueWork(
            WORK_NAME, if (fromReminder) ExistingWorkPolicy.APPEND_OR_REPLACE else ExistingWorkPolicy.REPLACE, request
        )
    }

    override fun cancel() {
        WorkManager.getInstance(context).cancelUniqueWork(WORK_NAME)
    }

    companion object {
        const val WORK_NAME = "evening_question_reminder"
    }
}

class EveningReminder(
    private val prefs: AppPreferences,
    private val scheduler: ReminderScheduler,
    private val zone: () -> ZoneId = ZoneId::systemDefault,
    private val now: () -> Long = System::currentTimeMillis
) {
    constructor(context: Context) : this(AppPreferences(context), WorkManagerReminderScheduler(context.applicationContext))

    /** Plans the next reminder from the current setting, or cancels it when the reminder is off. */
    fun reschedule(fromReminder: Boolean = false) {
        if (!prefs.isEveningReminderEnabled) {
            scheduler.cancel()
            return
        }
        val current = now()
        scheduler.schedule(nextAt(current, prefs.eveningReminderMinute, zone()) - current, fromReminder)
    }

    fun setEnabled(enabled: Boolean) {
        prefs.isEveningReminderEnabled = enabled
        reschedule()
    }

    fun setTime(minuteOfDay: Int) {
        prefs.eveningReminderMinute = minuteOfDay
        reschedule()
    }

    companion object {
        /** The next local time at [minuteOfDay] strictly after [nowMillis]: today if it is still ahead, else tomorrow. */
        fun nextAt(nowMillis: Long, minuteOfDay: Int, zone: ZoneId): Long {
            val now = Instant.ofEpochMilli(nowMillis).atZone(zone)
            val today: ZonedDateTime = now.toLocalDate().atStartOfDay(zone).plusMinutes(minuteOfDay.toLong())
            val next = if (today.isAfter(now)) today else today.plusDays(1)
            return next.toInstant().toEpochMilli()
        }

        fun label(minuteOfDay: Int): String = "%02d.%02d".format(minuteOfDay / 60, minuteOfDay % 60)
    }
}

/** At the reminder time: one summary when light questions wait, then the next evening is planned. */
class EveningReminderWorker(context: Context, params: WorkerParameters) : CoroutineWorker(context, params) {
    override suspend fun doWork(): Result {
        val prefs = AppPreferences(applicationContext)
        if (prefs.isEveningReminderEnabled) {
            val waiting = lightQuestionCount(AppDatabase.getDatabase(applicationContext))
            if (waiting > 0) EveningReminderNotifier.post(applicationContext, waiting)
        }
        EveningReminder(applicationContext).reschedule(fromReminder = true)
        return Result.success()
    }

    companion object {
        suspend fun lightQuestionCount(db: AppDatabase): Int {
            val dao = db.notificationDao()
            val open = OpenQuestions.open(dao.getAllNotificationsFlow().first(), db.senderReplyDao().pendingFlow().first(),
                dao.getNeedsAnswerFlow().first())
            return open.count { QuestionUrgency.isLight(OpenQuestions.kindOf(it)) }
        }
    }
}
