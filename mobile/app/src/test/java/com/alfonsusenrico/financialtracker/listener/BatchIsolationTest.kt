package com.alfonsusenrico.financialtracker.listener

import com.alfonsusenrico.financialtracker.listener.data.network.model.BatchIngestResponse
import com.alfonsusenrico.financialtracker.listener.data.sync.BatchReply
import com.alfonsusenrico.financialtracker.listener.data.sync.submitIsolating
import kotlinx.coroutines.test.runTest
import org.junit.Assert.assertEquals
import org.junit.Test

class BatchIsolationTest {

    private val accepted = BatchReply.Accepted(BatchIngestResponse(ok = true))

    /** A fake tracker that rejects any request containing [bad] with 422. */
    private class FakeTracker(private val bad: Set<String>, private val perRow: Map<String, BatchReply> = emptyMap()) {
        val requests = mutableListOf<List<String>>()
        fun reply(rows: List<String>, accepted: BatchReply): BatchReply {
            requests += rows
            if (rows.size == 1) perRow[rows[0]]?.let { return it }
            return if (rows.any { it in bad }) BatchReply.Rejected(422) else accepted
        }
    }

    @Test fun oneBadRowOnlyRejectsItself() = runTest {
        val tracker = FakeTracker(bad = setOf("b"))
        val chunks = submitIsolating(listOf("a", "b", "c")) { tracker.reply(it, accepted) }
        assertEquals(listOf(listOf("a", "b", "c"), listOf("a"), listOf("b"), listOf("c")), tracker.requests)
        assertEquals(listOf(listOf("a"), listOf("b"), listOf("c")), chunks.map { it.rows })
        assertEquals(listOf(accepted, BatchReply.Rejected(422), accepted), chunks.map { it.reply })
    }

    @Test fun serverFailureDuringIsolationLeavesRemainingRowsPending() = runTest {
        val tracker = FakeTracker(bad = setOf("a"), perRow = mapOf("b" to BatchReply.Rejected(503)))
        val chunks = submitIsolating(listOf("a", "b", "c")) { tracker.reply(it, accepted) }
        assertEquals(listOf(listOf("a"), listOf("b", "c")), chunks.map { it.rows })
        assertEquals(listOf(BatchReply.Rejected(422), BatchReply.Rejected(503)), chunks.map { it.reply })
        assertEquals(3, tracker.requests.size)
    }

    @Test fun authorisationFailureDuringIsolationStopsTheRest() = runTest {
        val tracker = FakeTracker(bad = setOf("c"), perRow = mapOf("a" to BatchReply.Rejected(401)))
        val chunks = submitIsolating(listOf("a", "b", "c")) { tracker.reply(it, accepted) }
        assertEquals(listOf(listOf("a", "b", "c")), chunks.map { it.rows })
        assertEquals(BatchReply.Rejected(401), chunks.single().reply)
    }

    @Test fun networkLossDuringIsolationLeavesRowsPending() = runTest {
        val tracker = FakeTracker(bad = setOf("a"), perRow = mapOf("b" to BatchReply.Unreachable))
        val chunks = submitIsolating(listOf("a", "b")) { tracker.reply(it, accepted) }
        assertEquals(listOf(BatchReply.Rejected(422), BatchReply.Unreachable), chunks.map { it.reply })
    }

    @Test fun singleRowRejectionIsNotRetried() = runTest {
        val tracker = FakeTracker(bad = setOf("a"))
        val chunks = submitIsolating(listOf("a")) { tracker.reply(it, accepted) }
        assertEquals(1, tracker.requests.size)
        assertEquals(BatchReply.Rejected(422), chunks.single().reply)
    }

    @Test fun serverErrorForTheBatchIsNotSplit() = runTest {
        val chunks = submitIsolating(listOf("a", "b")) { BatchReply.Rejected(500) }
        assertEquals(listOf(listOf("a", "b")), chunks.map { it.rows })
    }
}
