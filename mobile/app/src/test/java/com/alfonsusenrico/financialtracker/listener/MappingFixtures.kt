package com.alfonsusenrico.financialtracker.listener

import com.alfonsusenrico.financialtracker.listener.data.network.model.IngestResultItem
import com.google.gson.Gson
import okhttp3.mockwebserver.MockResponse

/** A fictional account proposal awaiting confirmation, and the server's recorded answer to it. */
object MappingFixtures {
    const val EVENT = "00000000-0000-4000-8000-000000000601"
    const val QUESTION = "00000000-0000-4000-8000-000000000602"
    const val ACCOUNT = "00000000-0000-4000-8000-000000000603"
    const val HASH = "fictional-mapping-hash"
    const val TAG = "confirm:$EVENT"

    fun proposal(): IngestResultItem = Gson().fromJson(
        """{"payload_hash": "$HASH", "event_id": "$EVENT", "status": "needs_confirmation",
            "error_code": "mapping_confirmation_required",
            "confirmation": {"type": "account", "question_id": "$QUESTION"},
            "mapping_proposal": {"name": "BCA ••0002", "role": "target", "confidence": 0.72,
              "proposed": {"id": "$ACCOUNT", "label": "BCA Tabungan Contoh"}}}""",
        IngestResultItem::class.java
    )

    fun recorded(): MockResponse = MockResponse().setBody(
        """{"ok": true, "result": {"payload_hash": "$HASH", "event_id": "$EVENT", "status": "recorded",
            "record_key": "notification:$EVENT", "type": "income", "description": "Transfer masuk",
            "target": "BCA Tabungan Contoh", "amount": 300000, "currency": "IDR"}}"""
    )
}
