package com.alfonsusenrico.financialtracker.listener.data.sync

import com.alfonsusenrico.financialtracker.listener.data.network.model.BatchIngestResponse

/** What the tracker answered to one ingestion request. */
internal sealed interface BatchReply {
    data class Accepted(val body: BatchIngestResponse) : BatchReply
    data class Rejected(val httpStatus: Int) : BatchReply
    data object Unreachable : BatchReply
}

/** A 400, 404 or 422 can be caused by a single row, so the batch is worth splitting. */
internal fun BatchReply.rejectsRows(): Boolean =
    this is BatchReply.Rejected && httpStatus in setOf(400, 404, 422)

internal data class BatchChunk<T>(val rows: List<T>, val reply: BatchReply)

/**
 * Sends [rows] as one batch. When the batch is rejected as a request error, each row is resent on
 * its own so only the rows that fail alone are rejected. A reply that is neither accepted nor a
 * request error (network, server or authorisation) stops the per-row pass and is attributed to every
 * row not yet sent, which leaves them pending for the existing retry handling.
 */
internal suspend fun <T> submitIsolating(rows: List<T>, send: suspend (List<T>) -> BatchReply): List<BatchChunk<T>> {
    val reply = send(rows)
    if (!reply.rejectsRows() || rows.size <= 1) return listOf(BatchChunk(rows, reply))
    val chunks = mutableListOf<BatchChunk<T>>()
    for ((index, row) in rows.withIndex()) {
        val single = send(listOf(row))
        if (single is BatchReply.Accepted || single.rejectsRows()) {
            chunks += BatchChunk(listOf(row), single)
        } else {
            chunks += BatchChunk(rows.subList(index, rows.size), single)
            break
        }
    }
    return chunks
}
