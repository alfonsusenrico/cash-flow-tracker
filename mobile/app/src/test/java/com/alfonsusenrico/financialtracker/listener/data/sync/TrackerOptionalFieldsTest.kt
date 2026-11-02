package com.alfonsusenrico.financialtracker.listener.data.sync

import com.alfonsusenrico.financialtracker.listener.data.network.model.IngestResultItem
import com.alfonsusenrico.financialtracker.listener.data.network.model.LedgerCashFlowSummary
import com.alfonsusenrico.financialtracker.listener.data.network.model.NotificationResultResponse
import com.alfonsusenrico.financialtracker.listener.data.network.model.RecordedHabit
import com.alfonsusenrico.financialtracker.listener.data.network.model.TopCategory
import com.alfonsusenrico.financialtracker.listener.ui.model.detailLine
import com.google.gson.Gson
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

/** The tracker's optional `habit` result object and `top_categories` summary list, with and without them. */
class TrackerOptionalFieldsTest {
    private val gson = Gson()

    private fun result(extra: String = ""): IngestResultItem = gson.fromJson(
        """{"ok":true,"result":{"payload_hash":"fictional-hash","event_id":"00000000-0000-4000-8000-000000000001",
           "status":"recorded","record_key":"notification:fictional","type":"expense","description":"Kedai Contoh",
           "source":"GoPay Contoh","amount":32000,"currency":"IDR"$extra}}""",
        NotificationResultResponse::class.java
    ).result!!

    private fun summary(extra: String = ""): LedgerCashFlowSummary = gson.fromJson(
        """{"ok":true,"period":"today","timezone":"Asia/Jakarta","currency":"IDR",
           "start":"2026-10-30T17:00:00+00:00","end":"2026-10-31T03:00:00+00:00",
           "generated_at":"2026-10-31T03:00:01+00:00","inflow":0,"outflow":160000,"net":-160000,
           "gross_outflow":160000,"reimbursement_offset":0$extra}""",
        LedgerCashFlowSummary::class.java
    )

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

    @Test fun topCategoriesAreReadWhenTheTrackerSendsThem() {
        val parsed = summary(
            ""","top_categories":[
                {"category_id":"00000000-0000-4000-8000-000000000201","name":"Makan & Minum","kakeibo_type":"need","outflow":120000},
                {"category_id":"00000000-0000-4000-8000-000000000202","name":"Kopi","kakeibo_type":"want","outflow":30000},
                {"category_id":"00000000-0000-4000-8000-000000000203","name":"Lain-lain","kakeibo_type":null,"outflow":10000}]"""
        )
        assertTrue(parsed.isComplete())
        assertEquals(
            listOf(
                TopCategory("00000000-0000-4000-8000-000000000201", "Makan & Minum", "need", 120_000),
                TopCategory("00000000-0000-4000-8000-000000000202", "Kopi", "want", 30_000),
                TopCategory("00000000-0000-4000-8000-000000000203", "Lain-lain", null, 10_000)
            ),
            parsed.shownTopCategories()
        )
    }

    @Test fun anOlderTrackerSendsNoTopCategoriesAndAnEmptyPeriodSendsAnEmptyList() {
        assertTrue(summary().isComplete())
        assertNull(summary().shownTopCategories())
        assertEquals(emptyList<TopCategory>(), summary(""","top_categories":[]""").shownTopCategories())
        assertNull(summary(""","top_categories":null""").shownTopCategories())
    }

    @Test fun topCategoriesThatBreakTheContractAreNotShownButTheSummaryStays() {
        val entry = { name: String, outflow: Long, pillar: String -> """{"category_id":"00000000-0000-4000-8000-000000000201","name":"$name","kakeibo_type":"$pillar","outflow":$outflow}""" }
        listOf(
            "four entries" to listOf(entry("A", 40, "need"), entry("B", 30, "need"), entry("C", 20, "need"), entry("D", 10, "need")).joinToString(","),
            "not sorted" to listOf(entry("A", 10, "need"), entry("B", 30, "want")).joinToString(","),
            "negative" to entry("A", -1, "need"),
            "blank name" to entry(" ", 10, "need"),
            "unknown pillar" to entry("A", 10, "luxury"),
            "missing outflow" to """{"name":"A","kakeibo_type":"need"}""",
            "null entry" to "null",
            "two uncategorised" to """{"category_id":null,"name":"Tanpa kategori","kakeibo_type":null,"outflow":30},""" +
                """{"category_id":null,"name":"Tanpa kategori","kakeibo_type":null,"outflow":20}""",
            "uncategorised with an unknown pillar" to """{"category_id":null,"name":"Tanpa kategori","kakeibo_type":"luxury","outflow":10}"""
        ).forEach { (case, entries) ->
            val parsed = summary(""","top_categories":[$entries]""")
            assertTrue(case, parsed.isComplete())
            assertNull(case, parsed.shownTopCategories())
        }
    }

    @Test fun anUncategorisedEntryHasANullIdAndKeepsTheTrackersName() {
        val parsed = summary(
            ""","top_categories":[
                {"category_id":"00000000-0000-4000-8000-000000000201","name":"Makan & Minum","kakeibo_type":"need","outflow":120000},
                {"category_id":null,"name":"Tanpa kategori","kakeibo_type":null,"outflow":30000}]"""
        )
        assertEquals(
            listOf(
                TopCategory("00000000-0000-4000-8000-000000000201", "Makan & Minum", "need", 120_000),
                TopCategory(null, "Tanpa kategori", null, 30_000)
            ),
            parsed.shownTopCategories()
        )
    }

    @Test fun anUncategorisedEntryWithoutANameIsCalledTanpaKategori() {
        listOf(""""name":"",""", """"name":"  ",""", """"name":null,""", "").forEach { name ->
            val parsed = summary(""","top_categories":[{"category_id":null,$name"kakeibo_type":null,"outflow":30000}]""")
            assertEquals(name, listOf(TopCategory(null, "Tanpa kategori", null, 30_000)), parsed.shownTopCategories())
        }
    }

    @Test fun anUncategorisedEntryShowsNoPillarEvenWhenTheTrackerSendsOne() {
        val parsed = summary(""","top_categories":[{"category_id":null,"name":"Tanpa kategori","kakeibo_type":"need","outflow":30000}]""")
        assertEquals(listOf(TopCategory(null, "Tanpa kategori", null, 30_000)), parsed.shownTopCategories())
    }

    @Test fun anUncategorisedEntrySurvivesTheStoredSummaryJson() {
        val parsed = summary(""","top_categories":[{"category_id":null,"name":"Tanpa kategori","kakeibo_type":null,"outflow":30000}]""")
        val restored = gson.fromJson(gson.toJson(parsed), LedgerCashFlowSummary::class.java)
        assertEquals(parsed, restored)
        assertEquals(listOf(TopCategory(null, "Tanpa kategori", null, 30_000)), restored.shownTopCategories())
    }

    @Test fun topCategoriesSurviveTheStoredSummaryJsonAndAbsenceStaysAbsent() {
        val withCategories = summary(""","top_categories":[{"category_id":"00000000-0000-4000-8000-000000000201","name":"Kopi","kakeibo_type":"want","outflow":30000}]""")
        assertEquals(withCategories, gson.fromJson(gson.toJson(withCategories), LedgerCashFlowSummary::class.java))
        val empty = summary(""","top_categories":[]""")
        assertEquals(emptyList<TopCategory>(), gson.fromJson(gson.toJson(empty), LedgerCashFlowSummary::class.java).topCategories)
        assertNull(gson.fromJson(gson.toJson(summary()), LedgerCashFlowSummary::class.java).topCategories)
    }
}
