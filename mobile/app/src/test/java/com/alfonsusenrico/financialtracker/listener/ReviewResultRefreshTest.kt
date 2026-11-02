package com.alfonsusenrico.financialtracker.listener

import android.app.Application
import android.content.Context
import androidx.room.Room
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.model.RawNotificationEntity
import com.alfonsusenrico.financialtracker.listener.data.model.SenderReplyEntity
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.data.sync.ReviewResultRefresh
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.annotation.Config
import java.util.concurrent.TimeUnit

/** Which settled review rows the foreground check re-reads. */
@RunWith(AndroidJUnit4::class)
@Config(sdk = [34], application = TestApplication::class)
class ReviewResultRefreshTest {
    private val context = ApplicationProvider.getApplicationContext<Context>()
    private lateinit var db: AppDatabase
    private lateinit var pairing: String
    private val now = 1_800_000_000_000L
    private val refreshed = mutableListOf<String>()

    @Before fun open() {
        AppPreferences(context).apply {
            TestEndpoint.set(context, "https://tracker.example.invalid/")
            apiKey = "fictional-review-key"
        }
        pairing = AppPreferences(context).pairingContext
        db = Room.inMemoryDatabaseBuilder(context, AppDatabase::class.java).allowMainThreadQueries().build()
    }

    @After fun close() {
        db.close()
        AppPreferences(context).apiKey = ""
    }

    private fun event(n: Int) = "00000000-0000-4000-8000-%012d".format(n)

    private suspend fun store(
        n: Int, status: String, capturedAt: Long = now - TimeUnit.DAYS.toMillis(3), pairingContext: String = pairing,
        eventId: String? = event(n)
    ): Long {
        val dao = db.notificationDao()
        val id = dao.insertOrIgnore(RawNotificationEntity(
            deviceId = "fictional", packageName = "com.gojek.app", appLabel = "Gojek", postTime = n.toLong(),
            capturedAt = capturedAt, payloadHash = "fictional-review-$n"
        ))
        dao.saveCompletion(
            id = id, accepted = true, syncedAt = 1, eventId = eventId, status = status,
            errorCode = if (status in ReviewResultRefresh.STATUSES) "fictional_noise" else null, recordKey = null,
            transactionId = null, pairingContext = pairingContext, nextCheckAt = null, checkAttempt = 0, resultJson = null
        )
        return id
    }

    private suspend fun refresh() = ReviewResultRefresh.refresh(context, db.notificationDao(), now) { refreshed.add(it) }

    @Test fun picksRecentReviewAndFailedRowsOfThisPairingOnly() = runBlocking {
        store(1, "needs_review")
        store(2, "failed")
        store(3, "recorded")
        store(4, "ignored")
        store(5, "queued")
        store(6, "needs_confirmation")
        store(7, "needs_review", capturedAt = now - TimeUnit.DAYS.toMillis(31))
        store(8, "needs_review", pairingContext = "00000000-0000-4000-8000-000000000998")
        store(9, "needs_review", eventId = null)
        store(10, "needs_review", capturedAt = now - ReviewResultRefresh.WINDOW_MILLIS)

        assertEquals(3, refresh())
        assertEquals(setOf(event(1), event(2), event(10)), refreshed.toSet())
    }

    @Test fun leavesOwnerLabelledRowsAndRowsWithUnsentRepliesAlone() = runBlocking {
        val labelled = store(1, "needs_review")
        db.notificationDao().saveOwnerLabel(labelled, "expense", 25_000.0, "out", null, now)
        val replying = store(2, "needs_review")
        db.senderReplyDao().insert(SenderReplyEntity(
            replyId = "00000000-0000-4000-8000-000000000901", rowId = replying, pairingContext = pairing,
            eventId = event(2), questionId = "00000000-0000-4000-8000-000000000902", action = "name", name = "Contoh"
        ))
        val answered = store(3, "failed")
        db.senderReplyDao().insert(SenderReplyEntity(
            replyId = "00000000-0000-4000-8000-000000000903", rowId = answered, pairingContext = pairing,
            eventId = event(3), questionId = "00000000-0000-4000-8000-000000000904", action = "name", name = "Contoh",
            state = "accepted"
        ))

        assertEquals(1, refresh())
        assertEquals(listOf(event(3)), refreshed)
    }

    @Test fun readsAtMostFiftyNewestFirst() = runBlocking {
        repeat(55) { store(it + 1, "needs_review") }
        assertEquals(ReviewResultRefresh.LIMIT, refresh())
        assertEquals(event(55), refreshed.first())
        assertEquals(event(6), refreshed.last())
    }

    @Test fun nothingIsReadWithoutCredentials() = runBlocking {
        store(1, "needs_review")
        AppPreferences(context).apiKey = ""
        assertEquals(0, refresh())
        assertTrue(refreshed.isEmpty())
    }
}
