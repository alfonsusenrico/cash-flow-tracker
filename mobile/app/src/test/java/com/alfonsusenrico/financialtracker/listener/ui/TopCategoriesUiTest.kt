package com.alfonsusenrico.financialtracker.listener.ui

import androidx.compose.ui.semantics.SemanticsNode
import androidx.compose.ui.semantics.SemanticsProperties
import androidx.compose.ui.semantics.getOrNull
import androidx.compose.ui.test.assertIsDisplayed
import androidx.compose.ui.test.junit4.createComposeRule
import androidx.compose.ui.test.onNodeWithContentDescription
import androidx.compose.ui.test.onNodeWithText
import androidx.compose.ui.test.onRoot
import androidx.compose.ui.test.performScrollTo
import com.alfonsusenrico.financialtracker.listener.data.network.model.LedgerCashFlowSummary
import com.alfonsusenrico.financialtracker.listener.data.network.model.TopCategory
import com.alfonsusenrico.financialtracker.listener.ui.home.HomeScreen
import com.alfonsusenrico.financialtracker.listener.ui.model.CapturePresentation
import com.alfonsusenrico.financialtracker.listener.ui.shell.LedgerSummaryState
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.ParameterizedRobolectricTestRunner
import org.robolectric.annotation.Config

/**
 * "Kategori teratas" shows only the tracker's own categories for the summary's period, with its
 * freshness. Every case runs in both themes at font scale 1.0 and 2.0.
 */
@RunWith(ParameterizedRobolectricTestRunner::class)
@Config(sdk = [34], application = com.alfonsusenrico.financialtracker.listener.TestApplication::class, qualifiers = "w360dp-h740dp")
class TopCategoriesUiTest(private val dark: Boolean, private val fontScale: Float) {
    companion object {
        @JvmStatic @ParameterizedRobolectricTestRunner.Parameters(name = "dark={0}, fontScale={1}")
        fun cases() = listOf(false, true).flatMap { dark -> listOf(1f, 2f).map { arrayOf<Any>(dark, it) } }

        /** Distinct from every local fixture amount and category, so a phone-computed figure cannot pass. */
        val CATEGORIES = listOf(
            TopCategory("00000000-0000-4000-8000-000000000201", "Belanja Rumah", "need", 410_000),
            TopCategory("00000000-0000-4000-8000-000000000202", "Hiburan Contoh", "want", 150_000),
            TopCategory("00000000-0000-4000-8000-000000000203", "Lain-lain", null, 80_000)
        )
        val SUMMARY = HomeActivityUiTest.SUMMARY.copy(topCategories = CATEGORIES)
        val CATEGORY_AMOUNTS = setOf("Rp410.000", "Rp150.000", "Rp80.000")

        /** The tracker's spending without a category, second largest, as the contract sends it. */
        val WITH_UNCATEGORISED = listOf(CATEGORIES[0], TopCategory(null, "Tanpa kategori", null, 150_000), CATEGORIES[2])
    }

    @get:Rule val compose = createComposeRule()

    private val captures = listOf(CaptureFixtures.recorded(), CaptureFixtures.queued()).map(CapturePresentation::of)

    private fun show(ledger: LedgerSummaryState) = compose.show(dark, fontScale) {
        HomeScreen(homeState(captures = captures).copy(ledger = ledger), onOpenQuestions = {}, onOpenActivity = {},
            onOpenSettings = {}, onEnableListener = {})
    }

    private fun everyLabel(): List<String> {
        val labels = mutableListOf<String>()
        fun walk(node: SemanticsNode) {
            node.config.getOrNull(SemanticsProperties.Text)?.forEach { labels += it.text }
            node.config.getOrNull(SemanticsProperties.ContentDescription)?.let { labels += it }
            node.children.forEach(::walk)
        }
        walk(compose.onRoot(useUnmergedTree = true).fetchSemanticsNode())
        return labels
    }

    @Test fun showsTheTrackersTopThreeWithPillarPeriodAndFreshness() {
        show(LedgerSummaryState(summary = SUMMARY))
        compose.onNodeWithText("KATEGORI TERATAS").performScrollTo().assertIsDisplayed()
        compose.onNodeWithText("Belanja Rumah", useUnmergedTree = true).performScrollTo().assertIsDisplayed()
        compose.onNodeWithText("Rp410.000", useUnmergedTree = true).assertExists()
        compose.onNodeWithText("Kebutuhan", useUnmergedTree = true).assertExists()
        compose.onNodeWithText("Keinginan", useUnmergedTree = true).assertExists()
        compose.onNodeWithText("Lain-lain", useUnmergedTree = true).performScrollTo().assertIsDisplayed()
        // The ledger tile and this one show the same period and freshness, because they come from one response.
        val period = "Periode 29 Okt 2026, 00.00 – 29 Okt 2026, 10.00 (Asia/Jakarta)"
        assertEquals(2, everyLabel().count { it == period })
        assertEquals(2, everyLabel().count { it == "Diperbarui 29 Okt 2026, 10.00" })
    }

    @Test fun talkBackReadsEachCategoryWithItsAmountAndPillar() {
        show(LedgerSummaryState(summary = SUMMARY))
        compose.onNodeWithContentDescription("Belanja Rumah, Rp410.000, Kebutuhan").performScrollTo().assertIsDisplayed()
        compose.onNodeWithContentDescription("Hiburan Contoh, Rp150.000, Keinginan").assertExists()
        compose.onNodeWithContentDescription("Lain-lain, Rp80.000").assertExists()
    }

    @Test fun everyAmountOnBerandaIsTheTrackers() {
        show(LedgerSummaryState(summary = SUMMARY))
        val amounts = everyLabel().flatMap { Regex("""-?Rp[0-9.]+""").findAll(it).map { match -> match.value }.toList() }.toSet()
        assertTrue("Category amounts are shown: $amounts", amounts.containsAll(CATEGORY_AMOUNTS))
        assertEquals(
            emptySet<String>(),
            amounts - HomeActivityUiTest.SERVER_AMOUNTS - CATEGORY_AMOUNTS - HomeActivityUiTest.NEXT_QUESTION_AMOUNT
        )
        listOf("Makan & Minum", "Transportasi", "Belanja Harian").forEach { local ->
            assertTrue("$local is not on Beranda", everyLabel().none { it.contains(local) })
        }
    }

    @Test fun anOlderTrackerWithoutTheFieldGetsNoTile() {
        show(LedgerSummaryState(summary = HomeActivityUiTest.SUMMARY))
        compose.onNodeWithText("ARUS KAS TRACKER").assertExists()
        assertTrue(everyLabel().none { it.contains("Kategori teratas", ignoreCase = true) })
    }

    @Test fun categoriesThatBreakTheContractGetNoTile() {
        show(LedgerSummaryState(summary = SUMMARY.copy(topCategories = CATEGORIES.reversed())))
        assertTrue(everyLabel().none { it.contains("Kategori teratas", ignoreCase = true) })
        assertTrue(everyLabel().none { it.contains("Belanja Rumah") })
    }

    @Test fun noSummaryMeansNoTile() {
        show(LedgerSummaryState(loading = true))
        assertTrue(everyLabel().none { it.contains("Kategori teratas", ignoreCase = true) })
    }

    @Test fun aPeriodWithoutSpendingSaysSo() {
        show(LedgerSummaryState(summary = SUMMARY.copy(topCategories = emptyList())))
        compose.onNodeWithText("KATEGORI TERATAS").performScrollTo().assertIsDisplayed()
        compose.onNodeWithText("Belum ada pengeluaran pada periode ini", useUnmergedTree = true).performScrollTo().assertIsDisplayed()
        assertTrue(everyLabel().none { it == "Rp0" })
    }

    @Test fun aStaleSummaryMarksItsCategoriesStaleToo() {
        show(LedgerSummaryState(summary = SUMMARY, stale = true, error = "Tidak terhubung ke tracker."))
        compose.onNodeWithText("Belanja Rumah", useUnmergedTree = true).performScrollTo().assertIsDisplayed()
        assertEquals(2, everyLabel().count { it == "Belum diperbarui. Terakhir berhasil 29 Okt 2026, 10.00" })
        assertEquals(2, everyLabel().count { it == "Tidak terhubung ke tracker." })
    }

    @Test fun showsTheUncategorisedEntryUnderTheTrackersNameWithoutAPillar() {
        show(LedgerSummaryState(summary = SUMMARY.copy(topCategories = WITH_UNCATEGORISED)))
        compose.onNodeWithText("Tanpa kategori", useUnmergedTree = true).performScrollTo().assertIsDisplayed()
        compose.onNodeWithText("Rp150.000", useUnmergedTree = true).assertExists()
        assertEquals(1, everyLabel().count { it == "Kebutuhan" })
        assertTrue(everyLabel().none { it == "Keinginan" })
    }

    @Test fun talkBackReadsTheUncategorisedEntryAsTanpaKategoriAndItsAmount() {
        show(LedgerSummaryState(summary = SUMMARY.copy(topCategories = WITH_UNCATEGORISED)))
        compose.onNodeWithContentDescription("Tanpa kategori, Rp150.000").performScrollTo().assertIsDisplayed()
        compose.onNodeWithContentDescription("Belanja Rumah, Rp410.000, Kebutuhan").assertExists()
    }

    @Test fun anUncategorisedEntryWithABlankNameOrAPillarReadsTanpaKategori() {
        val unnamed = WITH_UNCATEGORISED.toMutableList().apply { this[1] = TopCategory(null, " ", "want", 150_000) }
        show(LedgerSummaryState(summary = SUMMARY.copy(topCategories = unnamed)))
        compose.onNodeWithContentDescription("Tanpa kategori, Rp150.000").performScrollTo().assertIsDisplayed()
        assertTrue(everyLabel().none { it == "Keinginan" })
    }

    @Test fun twoUncategorisedEntriesGetNoTile() {
        val twice = listOf(TopCategory(null, "Tanpa kategori", null, 410_000), TopCategory(null, "Tanpa kategori", null, 150_000))
        show(LedgerSummaryState(summary = SUMMARY.copy(topCategories = twice)))
        compose.onNodeWithText("ARUS KAS TRACKER").assertExists()
        assertTrue(everyLabel().none { it.contains("Kategori teratas", ignoreCase = true) })
        assertTrue(everyLabel().none { it.contains("Tanpa kategori") })
    }

    @Test fun aRestoredSummaryLabelsItsCategoriesAsStored() {
        show(LedgerSummaryState(summary = SUMMARY, cached = true, loading = true))
        compose.onNodeWithText("Belanja Rumah", useUnmergedTree = true).performScrollTo().assertIsDisplayed()
        assertEquals(2, everyLabel().count { it == "Tersimpan di ponsel, terakhir diperbarui 29 Okt 2026, 10.00" })
    }
}
