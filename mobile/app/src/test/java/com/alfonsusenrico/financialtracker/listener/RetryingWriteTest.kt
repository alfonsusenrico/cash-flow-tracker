package com.alfonsusenrico.financialtracker.listener

import com.alfonsusenrico.financialtracker.listener.data.capture.retryingWrite
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.test.runTest
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class RetryingWriteTest {

    @Test fun transientFailureIsRetried() = runTest {
        var attempts = 0
        var failures = 0
        val result = retryingWrite(listOf(200L, 800L), onFailure = { failures++ }) {
            attempts++
            if (attempts < 2) throw IllegalStateException("database locked")
            "stored"
        }
        assertEquals("stored", result)
        assertEquals(2, attempts)
        assertEquals(0, failures)
    }

    @Test fun persistentFailureIsReportedOnce() = runTest {
        var attempts = 0
        val reported = mutableListOf<Exception>()
        val result = retryingWrite<String>(listOf(200L, 800L), onFailure = { reported += it }) {
            attempts++
            throw IllegalStateException("disk full")
        }
        assertNull(result)
        assertEquals(3, attempts)
        assertEquals(1, reported.size)
    }

    @Test fun cancellationIsNotSwallowed() = runTest {
        var reported = false
        val thrown = runCatching {
            retryingWrite<Unit>(listOf(200L), onFailure = { reported = true }) { throw CancellationException("stop") }
        }.exceptionOrNull()
        assertTrue(thrown is CancellationException)
        assertTrue(!reported)
    }
}
