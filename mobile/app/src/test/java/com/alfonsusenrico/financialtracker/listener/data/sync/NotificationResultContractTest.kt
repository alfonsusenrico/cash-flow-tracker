package com.alfonsusenrico.financialtracker.listener.data.sync

import com.alfonsusenrico.financialtracker.listener.data.network.model.IngestResultItem
import com.alfonsusenrico.financialtracker.listener.data.network.model.NotificationResultResponse
import com.google.gson.Gson
import org.junit.Assert.*
import org.junit.Test

class NotificationResultContractTest {
    private val recorded = IngestResultItem(
        payloadHash = "fictional-hash", eventId = "00000000-0000-4000-8000-000000000001",
        status = "recorded", recordKey = "notification:fictional", type = "expense",
        description = "Kedai Awan", source = "Bank Jago", amount = 125000, currency = "IDR"
    )

    @Test fun expenseRequiresCompleteCommittedResult() {
        assertTrue(completeRecordedResult(recorded))
        listOf(
            recorded.copy(status = "queued"), recorded.copy(recordKey = null),
            recorded.copy(description = ""), recorded.copy(amount = 0),
            recorded.copy(amount = null), recorded.copy(currency = null),
            recorded.copy(currency = "USD"), recorded.copy(source = null),
            recorded.copy(type = "unknown")
        ).forEach { assertFalse(completeRecordedResult(it)) }
    }

    @Test fun incomeUsesReceivingEndpointAndMovementRequiresBoth() {
        assertFalse(completeRecordedResult(recorded.copy(type = "income")))
        assertTrue(completeRecordedResult(recorded.copy(type = "income", source = null, target = "GoPay")))
        assertFalse(completeRecordedResult(recorded.copy(type = "internal_movement")))
        assertTrue(completeRecordedResult(recorded.copy(type = "internal_movement", target = "BCA")))
    }

    @Test fun missingOptionalFieldsNeverBecomeFabricatedRecordedValues() {
        val result = Gson().fromJson(
            """{"ok":true,"result":{"payload_hash":"fictional-hash","status":"queued","event_id":"00000000-0000-4000-8000-000000000001"}}""",
            NotificationResultResponse::class.java
        ).result!!
        assertEquals("queued", result.status)
        assertNull(result.amount)
        assertNull(result.transactionId)
        assertNull(result.currency)
        assertFalse(completeRecordedResult(result))
    }

    @Test fun uuidValidationRejectsLogicalKeysAndAbbreviatedUuid() {
        assertTrue(validUuid(recorded.eventId))
        listOf(null, "", "notification:00000000-0000-4000-8000-000000000001", "1-1-1-1-1")
            .forEach { assertFalse(validUuid(it)) }
    }

    @Test fun completionDelayIncreasesAndRemainsBounded() {
        assertEquals(10_000L, nextCheckDelay(0))
        assertEquals(20_000L, nextCheckDelay(1))
        assertTrue(nextCheckDelay(100) <= 900_000L)
    }
}
