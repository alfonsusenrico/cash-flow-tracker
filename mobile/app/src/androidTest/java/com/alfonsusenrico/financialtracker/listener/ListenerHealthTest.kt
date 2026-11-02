package com.alfonsusenrico.financialtracker.listener

import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(AndroidJUnit4::class)
class ListenerHealthTest {
    @Test fun recordsConnectDisconnectCaptureAndDirectSync() {
        val prefs = AppPreferences(InstrumentationRegistry.getInstrumentation().targetContext)
        val before = prefs.listenerDisconnectCount
        prefs.recordListenerConnected(1_000L)
        prefs.recordListenerDisconnected(2_000L)
        prefs.recordListenerDisconnected(3_000L)
        prefs.recordCapture(4_000L)
        prefs.recordDirectSync("submitted", 5_000L)
        assertEquals(1_000L, prefs.listenerConnectedAt)
        assertEquals(3_000L, prefs.listenerDisconnectedAt)
        assertEquals(before + 2, prefs.listenerDisconnectCount)
        assertEquals(4_000L, prefs.lastCaptureAt)
        assertEquals(5_000L, prefs.lastDirectSyncAt)
        assertEquals("submitted", prefs.lastDirectSyncOutcome)
    }
}
