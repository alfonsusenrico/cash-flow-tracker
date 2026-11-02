package com.alfonsusenrico.financialtracker.listener.ui

import com.alfonsusenrico.financialtracker.listener.data.network.model.LedgerCashFlowSummary
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class LedgerSummaryTest {
    private val valid = LedgerCashFlowSummary(
        ok = true, period = "today", timezone = "Asia/Jakarta", currency = "IDR",
        start = "2026-10-29T17:00:00+00:00", end = "2026-10-30T03:00:00+00:00",
        generatedAt = "2026-10-30T03:00:00+00:00", inflow = 500, outflow = 200, net = 300,
        grossOutflow = 250, reimbursementOffset = 50
    )

    @Test fun validatesAmountsDatesAndPeriod() {
        assertTrue(valid.isComplete())
        assertFalse(valid.copy(net = 1).isComplete())
        assertFalse(valid.copy(outflow = null).isComplete())
        assertFalse(valid.copy(timezone = "invalid").isComplete())
        assertFalse(valid.copy(start = "bad").isComplete())
        assertFalse(valid.copy(period = "month").isComplete())
        assertFalse(valid.copy(end = "2026-10-29T00:00:00+00:00").isComplete())
        assertFalse(valid.copy(generatedAt = null).isComplete())
        assertFalse(valid.copy(currency = "USD").isComplete())
        assertFalse(valid.copy(ok = false).isComplete())
        assertFalse(valid.copy(inflow = -1, net = -201).isComplete())
    }

    @Test fun requiresTheReimbursementOffsetToExplainTheOutflow() {
        assertTrue(valid.copy(grossOutflow = 200, reimbursementOffset = 0).isComplete())
        assertFalse(valid.copy(reimbursementOffset = 10).isComplete())
        assertFalse(valid.copy(grossOutflow = null).isComplete())
        assertFalse(valid.copy(grossOutflow = 150, reimbursementOffset = -50).isComplete())
    }
}
