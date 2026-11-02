package com.alfonsusenrico.financialtracker.listener

import com.alfonsusenrico.financialtracker.listener.data.network.model.IngestResultItem
import com.alfonsusenrico.financialtracker.listener.service.MappingConfirmationNotifier
import com.google.gson.Gson
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class MappingConfirmationTest {

    private val json = """
        {"payload_hash": "fictional-hash", "event_id": "00000000-0000-4000-8000-000000000042",
         "status": "needs_confirmation", "error_code": "mapping_confirmation_required",
         "mapping_proposal": {"name": "GoPay Tabungan", "role": "target", "confidence": 0.72,
           "proposed": {"id": "00000000-0000-4000-8000-000000000005", "label": "GoPay"},
           "alternatives": [{"id": "00000000-0000-4000-8000-000000000004", "label": "Bank Jago · Dana Darurat"}]}}
    """.trimIndent()

    private fun item(): IngestResultItem = Gson().fromJson(json, IngestResultItem::class.java)

    @Test
    fun parsesBackendProposal() {
        val proposal = item().mappingProposal!!
        assertEquals("GoPay Tabungan", proposal.name)
        assertEquals("GoPay", proposal.proposed?.label)
        assertEquals(listOf("Bank Jago · Dana Darurat"), proposal.alternatives?.map { it.label })
        assertTrue(MappingConfirmationNotifier.validProposal(item()))
    }

    @Test
    fun notificationNamesAmountBankNameProposalAndConfidence() {
        val content = MappingConfirmationNotifier.content(item(), 1_250_000.0)
        assertEquals("Konfirmasi rekening • Rp 1.250.000", content.title)
        assertEquals("“GoPay Tabungan” → GoPay? (yakin 72%)", content.text)
        assertEquals("Ya, GoPay", content.confirmLabel)
        assertEquals("Konfirmasi rekening", MappingConfirmationNotifier.content(item(), null).title)
    }

    @Test
    fun rejectsIncompleteOrWrongStateProposals() {
        val base = item()
        val proposal = base.mappingProposal!!
        listOf(
            base.copy(status = "needs_review"),
            base.copy(eventId = "not-a-uuid"),
            base.copy(mappingProposal = null),
            base.copy(mappingProposal = proposal.copy(proposed = proposal.proposed?.copy(id = "x"))),
            base.copy(mappingProposal = proposal.copy(proposed = proposal.proposed?.copy(label = " "))),
            base.copy(mappingProposal = proposal.copy(name = "")),
            base.copy(mappingProposal = proposal.copy(confidence = 1.4)),
            base.copy(mappingProposal = proposal.copy(confidence = null))
        ).forEach { assertFalse(MappingConfirmationNotifier.validProposal(it)) }
    }
}
