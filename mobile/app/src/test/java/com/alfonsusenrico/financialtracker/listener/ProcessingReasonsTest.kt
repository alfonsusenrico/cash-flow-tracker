package com.alfonsusenrico.financialtracker.listener

import com.alfonsusenrico.financialtracker.listener.data.network.model.NotificationResolveRequest
import com.alfonsusenrico.financialtracker.listener.ui.components.ProcessingReasons
import com.google.gson.Gson
import com.google.gson.JsonParser
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class ProcessingReasonsTest {

    // error_code values the backend can emit (notification_evidence, notification_resolution,
    // notification_application, notification_interpretation, notification_processing,
    // openai_notification_provider) plus the companion's own completion codes.
    private val documentedCodes = listOf(
        "message_too_large", "unsafe_instructions", "timestamp_timezone_required", "unsupported_currency",
        "ambiguous_amount", "missing_amount", "settlement_unproven", "wording_unconfirmed", "conflicting_mobile_amount",
        "conflicting_mobile_direction", "direction_unproven", "invalid_amount", "fractional_amount",
        "uncertain_institution_mapping", "invalid_default_pocket", "ambiguous_main_pocket", "main_pocket_unconfigured",
        "uncertain_pocket_mapping", "uncertain_category_mapping", "movement_category_missing", "same_movement_endpoint",
        "incomplete_movement", "conflicting_movement_endpoints", "conflicting_observed_account",
        "invalid_account_reference", "invalid_category_reference", "invalid_candidate_reference",
        "invalid_current_category", "invalid_current_account", "invalid_current_reference",
        "ambiguous_existing_movement", "movement_role_consumed", "counterpart_changed", "ambiguous_movement",
        "model_uncertain", "conflicting_direction", "conflicting_amount", "fabricated_amount_evidence",
        "fabricated_direction_evidence", "fabricated_endpoint_evidence", "income_kakeibo_not_permitted",
        "provider_invalid_output", "invalid_interpretation", "incomplete_context",
        "provider_timeout", "provider_unavailable", "database_unavailable", "provider_access_denied",
        "provider_credential_missing", "provider_model_not_permitted", "provider_reasoning_not_permitted",
        "provider_request_rejected", "ai_disabled", "processor_configuration_invalid", "worker_attempts_exhausted",
        "unsupported_broker_event", "conflicting_trade_price", "conflicting_trade_quantity", "invalid_trade_amount",
        "broker_funding_missing", "ambiguous_broker_position", "broker_position_missing", "legacy_review_required",
        "auth_rejected", "invalid_request", "server_unavailable", "network_unavailable", "missing_result",
        "result_mismatch", "invalid_result", "missing_event_id", "invalid_recorded_result", "event_not_found"
    )

    @Test
    fun everyDocumentedCodeHasAnIndonesianMessage() {
        val missing = documentedCodes.filterNot(ProcessingReasons::hasMessage)
        assertTrue("Missing messages: $missing", missing.isEmpty())
    }

    @Test
    fun unregisteredPocketsAskForRegistrationNotManualRecording() {
        val message = ProcessingReasons.describe("uncertain_pocket_mapping")!!
        assertTrue(message.contains("Rekening & Kantong"))
        assertTrue(message.contains("Coba Proses Ulang"))
        assertFalse(message.contains("catat manual"))
    }

    @Test
    fun unknownCodesFallBackToTheRawCode() {
        assertEquals("Perlu diperiksa (kode: brand_new_code).", ProcessingReasons.describe("brand_new_code"))
        assertEquals(null, ProcessingReasons.describe(null))
    }

    @Test
    fun onlyServerSideFailuresAreRetriedAutomatically() {
        assertTrue(ProcessingReasons.isServerRetrying("provider_unavailable"))
        assertFalse(ProcessingReasons.isServerRetrying("uncertain_category_mapping"))
        assertFalse(ProcessingReasons.isServerRetrying("auth_rejected"))
        assertFalse(ProcessingReasons.isServerRetrying(null))
    }

    @Test
    fun resolveRequestUsesBackendFieldNames() {
        val json = JsonParser.parseString(
            Gson().toJson(NotificationResolveRequest("expense", "acc-1", "cat-1", 125000L, null))
        ).asJsonObject
        assertEquals(setOf("type", "account_id", "category_id", "amount"), json.keySet())
        assertEquals(125000L, json.get("amount").asLong)
    }
}
