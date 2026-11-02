package com.alfonsusenrico.financialtracker.listener

import android.app.Application
import android.content.Context
import android.util.Log
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.work.Configuration
import androidx.work.WorkInfo
import androidx.work.WorkManager
import androidx.work.testing.SynchronousExecutor
import androidx.work.testing.WorkManagerTestInitHelper
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.data.sync.AutoSync
import com.alfonsusenrico.financialtracker.listener.data.sync.NotificationSyncWorker
import com.alfonsusenrico.financialtracker.listener.data.sync.SyncScheduler
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.annotation.Config

/** The auto-sync switch: off stops every automatic send, on resumes them. */
@RunWith(AndroidJUnit4::class)
@Config(sdk = [34], application = TestApplication::class)
class AutoSyncTest {

    private val context = ApplicationProvider.getApplicationContext<Context>()
    private val prefs = AppPreferences(context)
    private val directStarts = mutableListOf<Context>()
    private lateinit var workManager: WorkManager

    @Before fun setUp() {
        WorkManagerTestInitHelper.initializeTestWorkManager(
            context,
            Configuration.Builder().setMinimumLoggingLevel(Log.DEBUG).setExecutor(SynchronousExecutor()).build()
        )
        workManager = WorkManager.getInstance(context)
        TestEndpoint.set(context, "https://tracker.example.invalid")
        prefs.apiKey = "fictional-auto-sync-key"
    }

    @After fun tearDown() {
        prefs.isAutoSyncEnabled = true
        prefs.apiKey = ""
    }

    private fun states(name: String): List<WorkInfo.State> =
        workManager.getWorkInfosForUniqueWork(name).get().map { it.state }

    private fun active(name: String) = states(name).any { !it.isFinished }

    @Test fun turningAutoSyncOffCancelsPeriodicAndQueuedSends() {
        AutoSync.setEnabled(context, true)
        assertTrue(active(NotificationSyncWorker.WORK_NAME))
        assertTrue(active(SyncScheduler.IMMEDIATE_SYNC_WORK))

        AutoSync.setEnabled(context, false)

        assertFalse(prefs.isAutoSyncEnabled)
        assertEquals(listOf(WorkInfo.State.CANCELLED), states(NotificationSyncWorker.WORK_NAME))
        assertFalse(active(SyncScheduler.IMMEDIATE_SYNC_WORK))
    }

    @Test fun theSwitchAlsoGovernsQueuedTypedCaptures() {
        AutoSync.setEnabled(context, true)
        assertTrue(active(SyncScheduler.TEXT_CAPTURE_WORK))

        AutoSync.setEnabled(context, false)
        assertFalse(active(SyncScheduler.TEXT_CAPTURE_WORK))

        // "Kirim" is the owner's action and queues a resend whatever the switch says.
        SyncScheduler.enqueueTextCaptures(context)
        assertTrue(active(SyncScheduler.TEXT_CAPTURE_WORK))
    }

    @Test fun captureAndReconnectDoNotSendWhileAutoSyncIsOff() {
        AutoSync.setEnabled(context, false)

        assertFalse(AutoSync.sendPending(context) { directStarts.add(it) })

        assertTrue(directStarts.isEmpty())
        assertFalse(active(SyncScheduler.IMMEDIATE_SYNC_WORK))
    }

    @Test fun captureAndReconnectSendWhileAutoSyncIsOn() {
        prefs.isAutoSyncEnabled = true

        assertTrue(AutoSync.sendPending(context) { directStarts.add(it) })

        assertEquals(1, directStarts.size)
        assertTrue(active(SyncScheduler.IMMEDIATE_SYNC_WORK))
    }

    @Test fun nothingIsSentBeforeTheAccountIsConnected() {
        prefs.isAutoSyncEnabled = true
        prefs.apiKey = ""

        assertFalse(AutoSync.sendPending(context) { directStarts.add(it) })

        assertTrue(directStarts.isEmpty())
    }

    @Test fun appStartCancelsPeriodicWorkLeftFromAnOlderBuild() {
        SyncScheduler.schedulePeriodicSync(context)
        prefs.isAutoSyncEnabled = false

        AutoSync.restore(context)

        assertEquals(listOf(WorkInfo.State.CANCELLED), states(NotificationSyncWorker.WORK_NAME))
    }

    @Test fun appStartSchedulesPeriodicWorkWhenAutoSyncIsOn() {
        prefs.isAutoSyncEnabled = true

        AutoSync.restore(context)

        assertTrue(active(NotificationSyncWorker.WORK_NAME))
    }

    @Test fun ownerSyncNowStillRunsWhileAutoSyncIsOff() {
        AutoSync.setEnabled(context, false)

        SyncScheduler.enqueueImmediateSync(context)

        assertTrue(active(SyncScheduler.IMMEDIATE_SYNC_WORK))
    }
}
