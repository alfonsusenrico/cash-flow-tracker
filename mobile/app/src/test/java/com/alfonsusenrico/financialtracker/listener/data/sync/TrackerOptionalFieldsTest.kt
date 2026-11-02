package com.alfonsusenrico.financialtracker.listener.data.sync

import com.alfonsusenrico.financialtracker.listener.data.network.model.IngestResultItem
import com.alfonsusenrico.financialtracker.listener.data.network.model.NotificationResultResponse
import com.alfonsusenrico.financialtracker.listener.data.network.model.RecordedHabit
import com.alfonsusenrico.financialtracker.listener.ui.model.detailLine
import com.google.gson.Gson
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

/** The tracker's optional `habit` result object, with and without it. */
class TrackerOptionalFieldsTest {
    private val gson = Gson()

    private fun result(extra: String = ""): IngestResultItem = gson.fromJson(
        """{"ok":true,"result":{"payload_hash":"fictional-hash","event_id":"00000000-0000-4000-8000-000000000001",
           "status":"recorded","record_key":"notification:fictional","type":"expense","description":"Kedai Contoh",
           "source":"GoPay Contoh","amount":32000,"currency":"IDR"$extra}}""",
        NotificationResultResponse::class.java
    ).result!!

    @Test fun habitIsReadWhenTheTrackerSendsIt() {
        val item = result(""","habit":{"label":"Dicatat dari kebiasaan: Makan & Minum","basis":"3 dari 3 jawaban sebelumnya"}""")
        assertEquals(RecordedHabit("Dicatat dari kebiasaan: Makan & Minum", "3 dari 3 jawaban sebelumnya"), item.habit!!.shown())
        assertTrue(completeRecordedResult(item))
    }

    @Test fun anOlderTrackerSendsNoHabit() {
        val item = result()
        assertNull(item.habit)
        assertTrue(completeRecordedResult(item))
    }

    @Test fun anIncompleteHabitIsNotShownAndDoesNotSpoilTheResult() {
        listOf(
            ""","habit":{"label":"Dicatat dari kebiasaan: Makan & Minum"}""",
            ""","habit":{"basis":"3 dari 3 jawaban sebelumnya"}""",
            ""","habit":{"label":"  ","basis":"3 dari 3 jawaban sebelumnya"}""",
            ""","habit":{"label":"Dicatat dari kebiasaan","basis":"${"x".repeat(201)}"}""",
            ""","habit":null"""
        ).forEach { extra ->
            val item = result(extra)
            assertNull(extra, item.habit?.shown())
            assertTrue(extra, completeRecordedResult(item))
        }
    }

    @Test fun habitSurvivesTheStoredResultJson() {
        val item = result(""","habit":{"label":"Dicatat dari kebiasaan: Makan & Minum","basis":"3 dari 3 jawaban sebelumnya"}""")
        val stored = gson.fromJson(gson.toJson(item), IngestResultItem::class.java)
        assertEquals(item.habit, stored.habit)
        assertFalse(gson.toJson(result()).contains("habit"))
    }

    @Test fun detailLineKeepsTheTrackersLabelAndAddsTheBasis() {
        assertEquals(
            "Dicatat dari kebiasaan: Makan & Minum · 3 dari 3 jawaban sebelumnya",
            RecordedHabit("Dicatat dari kebiasaan: Makan & Minum", "3 dari 3 jawaban sebelumnya").detailLine()
        )
        // The tracker's label for a transaction without a category.
        assertEquals(
            "Dicatat dari kebiasaan · 2 dari 3 jawaban sebelumnya",
            RecordedHabit("Dicatat dari kebiasaan", "2 dari 3 jawaban sebelumnya").detailLine()
        )
        assertEquals(
            "Dicatat dari kebiasaan: Parkir · 4 dari 5 jawaban sebelumnya",
            RecordedHabit("Parkir", "4 dari 5 jawaban sebelumnya").detailLine()
        )
    }
}
