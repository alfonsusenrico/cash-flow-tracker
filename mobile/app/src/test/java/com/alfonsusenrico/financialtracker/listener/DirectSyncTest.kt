package com.alfonsusenrico.financialtracker.listener

import com.alfonsusenrico.financialtracker.listener.data.sync.DirectSync
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class DirectSyncTest {
    @Test
    fun resultChecksEscalateAndStayBounded() {
        val delays = DirectSync.POLL_DELAYS_SECONDS
        assertEquals(listOf(5L, 10L, 20L, 40L, 60L), delays)
        assertTrue(delays.zipWithNext().all { (a, b) -> b > a })
        assertTrue(delays.sum() <= 150)
    }
}
