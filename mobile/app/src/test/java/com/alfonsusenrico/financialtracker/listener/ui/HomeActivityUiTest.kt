package com.alfonsusenrico.financialtracker.listener.ui

import androidx.compose.ui.semantics.SemanticsNode
import androidx.compose.ui.semantics.SemanticsProperties
import androidx.compose.ui.semantics.getOrNull
import androidx.compose.ui.test.assertHeightIsAtLeast
import androidx.compose.ui.test.assertIsDisplayed
import androidx.compose.ui.test.hasText
import androidx.compose.ui.test.junit4.createComposeRule
import androidx.compose.ui.test.onNodeWithContentDescription
import androidx.compose.ui.test.onNodeWithText
import androidx.compose.ui.test.onRoot
import androidx.compose.ui.test.performClick
import androidx.compose.ui.test.performScrollTo
import androidx.compose.ui.unit.dp
import com.alfonsusenrico.financialtracker.listener.data.network.model.LedgerCashFlowSummary
import com.alfonsusenrico.financialtracker.listener.ui.home.HomeScreen
import com.alfonsusenrico.financialtracker.listener.ui.home.HomeState
import com.alfonsusenrico.financialtracker.listener.ui.model.ActivityFilter
import com.alfonsusenrico.financialtracker.listener.ui.model.CapturePresentation
import com.alfonsusenrico.financialtracker.listener.ui.shell.CompanionLists
import com.alfonsusenrico.financialtracker.listener.ui.shell.LedgerSummaryState
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.ParameterizedRobolectricTestRunner
import org.robolectric.annotation.Config

/**
 * Beranda's money comes only from the tracker's summary; this phone's rows and captures appear only as
 * counts. Every case runs in both themes at font scale 1.0 and 2.0.
 */
@RunWith(ParameterizedRobolectricTestRunner::class)
@Config(sdk = [34], application = com.alfonsusenrico.financialtracker.listener.TestApplication::class, qualifiers = "w360dp-h740dp")
class HomeActivityUiTest(private val dark: Boolean, private val fontScale: Float) {
    companion object {
        @JvmStatic @ParameterizedRobolectricTestRunner.Parameters(name = "dark={0}, fontScale={1}")
        fun cases() = listOf(false, true).flatMap { dark -> listOf(1f, 2f).map { arrayOf<Any>(dark, it) } }

        /** Distinct from every local figure of the fixtures, so a leaked local sum cannot pass as the tracker's. */
        val SUMMARY = LedgerCashFlowSummary(
            ok = true, period = "today", timezone = "Asia/Jakarta", currency = "IDR",
            start = "2026-10-28T17:00:00+00:00", end = "2026-10-29T03:00:00+00:00",
            generatedAt = "2026-10-29T03:00:00+00:00", inflow = 1_100_000, outflow = 640_000, net = 460_000,
            grossOutflow = 690_000, reimbursementOffset = 50_000
        )
        val SERVER_AMOUNTS = setOf("Rp640.000", "Rp1.100.000", "Rp460.000", "Rp690.000", "Rp50.000")

        /** The next question's own amount names that question; it is an item, not a total. */
        const val NEXT_QUESTION_AMOUNT = "Rp187.500"
    }

    @get:Rule val compose = createComposeRule()

    /** Recorded, moved and waiting typed captures next to the notification fixtures. */
    private val captures = listOf(
        CaptureFixtures.recorded(), CaptureFixtures.movement(), CaptureFixtures.asking(), CaptureFixtures.queued()
    ).map(CapturePresentation::of)

    private fun show(state: HomeState, opened: MutableList<Any> = mutableListOf()) = compose.show(dark, fontScale) {
        HomeScreen(state, onOpenQuestions = { opened += "questions" }, onOpenActivity = { opened += it },
            onOpenSettings = { opened += "settings" }, onEnableListener = {})
    }

    /** Every text and spoken label on the screen, including tiles that speak one summary for their content. */
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

    @Test fun berandaShowsNoLocalMoneyAmount() {
        show(homeState(captures = captures).copy(ledger = LedgerSummaryState(summary = SUMMARY)))
        val amounts = everyLabel().flatMap { Regex("""-?Rp[0-9.]+""").findAll(it).map { match -> match.value }.toList() }.toSet()

        assertTrue("Tracker amounts are shown: $amounts", amounts.containsAll(setOf("Rp640.000", "Rp1.100.000", "Rp460.000")))
        assertEquals(emptySet<String>(), amounts - SERVER_AMOUNTS - NEXT_QUESTION_AMOUNT)
        listOf("Keluar tercatat hari ini", "Pindah", "Keluar per hari", "Kategori teratas").forEach { gone ->
            assertTrue("$gone is gone", everyLabel().none { it.contains(gone, ignoreCase = true) })
        }
    }

    @Test fun countsTileShowsTodaysActivityAndOpensAktivitas() {
        val opened = mutableListOf<Any>()
        show(homeState(captures = captures), opened)
        compose.onNodeWithContentDescription(
            "Aktivitas ponsel hari ini, 8 notifikasi tertangkap. Menunggu 3 notifikasi, 2 catatan. Dari notifikasi dan catatan di ponsel ini"
        ).performScrollTo().assertIsDisplayed().assertHeightIsAtLeast(48.dp).performClick()
        assertEquals(listOf<Any>(ActivityFilter.ALL), opened)
        compose.onNodeWithText("Uang di Beranda hanya dari arus kas tracker", substring = true).performScrollTo().assertIsDisplayed()
    }

    @Test fun countsNotYetReadAreUnknownNeverZero() {
        show(homeState(lists = CompanionLists(), captures = null))
        compose.onNodeWithContentDescription(
            "Aktivitas ponsel hari ini, notifikasi tertangkap belum dimuat. Menunggu notifikasi belum dimuat, catatan belum dimuat. " +
                "Dari notifikasi dan catatan di ponsel ini"
        ).performScrollTo().assertIsDisplayed()
        assertTrue(everyLabel().none { it.startsWith("Aktivitas ponsel hari ini") && it.contains(" 0 ") })
    }

    @Test fun everyCountTileIsUnknownUntilRoomAnswers() {
        show(homeState(lists = CompanionLists(), captures = null))
        compose.onNode(hasText("PERTANYAAN") and hasText("Belum dimuat")).performScrollTo().assertIsDisplayed()
        compose.onNodeWithContentDescription("Perlu cek, belum dimuat. Belum tuntas di tracker").performScrollTo().assertIsDisplayed()
        compose.onNodeWithContentDescription("Di ponsel, belum dimuat. Belum dikirim").performScrollTo().assertIsDisplayed()
        val zeros = everyLabel().filter { it == "0" || Regex("""(Pertanyaan|Perlu cek|Di ponsel), 0\b""").containsMatchIn(it) || it == "Semua terjawab" }
        assertEquals(emptyList<String>(), zeros)
    }

    @Test fun captureCountsWaitForTheirOwnRead() {
        // Notification rows are read, typed captures not yet: tiles that also count captures stay unknown.
        show(homeState(captures = null))
        compose.onNodeWithContentDescription("Pertanyaan, 4 menunggu", substring = true).performScrollTo().assertIsDisplayed()
        compose.onNodeWithContentDescription("Perlu cek, belum dimuat. Belum tuntas di tracker").performScrollTo().assertIsDisplayed()
        compose.onNodeWithContentDescription("Di ponsel, belum dimuat. Belum dikirim").performScrollTo().assertIsDisplayed()
        compose.onNodeWithContentDescription(
            "Aktivitas ponsel hari ini, 8 notifikasi tertangkap. Menunggu 3 notifikasi, catatan belum dimuat. Dari notifikasi dan catatan di ponsel ini"
        ).performScrollTo().assertIsDisplayed()
    }

    @Test fun nothingCapturedTodayCountsZero() {
        val earlier = CompanionLists(visible = listOf(UiFixtures.review()), loaded = true)
        show(homeState(lists = earlier, captures = emptyList()))
        compose.onNodeWithContentDescription(
            "Aktivitas ponsel hari ini, 0 notifikasi tertangkap. Menunggu 0 notifikasi. Dari notifikasi dan catatan di ponsel ini"
        ).performScrollTo().assertIsDisplayed()
    }

    @Test fun serverTileIsUnchanged() {
        show(homeState(captures = captures).copy(ledger = LedgerSummaryState(summary = SUMMARY, stale = true, error = "Tidak terhubung ke tracker.")))
        compose.onNodeWithText("ARUS KAS TRACKER", substring = true).assertExists()
        compose.onNodeWithContentDescription("Uang keluar tracker Rp640.000").performScrollTo().assertIsDisplayed()
        compose.onNodeWithContentDescription("Masuk Rp1.100.000").assertExists()
        compose.onNodeWithContentDescription("Neto Rp460.000").assertExists()
        compose.onNodeWithContentDescription("Keluar kotor Rp690.000").assertExists()
        compose.onNodeWithContentDescription("Dikurangi penggantian Rp50.000").assertExists()
        compose.onNodeWithText("Periode 29 Okt 2026, 00.00 – 29 Okt 2026, 10.00 (Asia/Jakarta)").assertExists()
        compose.onNodeWithText("Belum diperbarui. Terakhir berhasil 29 Okt 2026, 10.00").assertExists()
        compose.onNodeWithText("Tidak terhubung ke tracker.").assertExists()
        compose.onNodeWithText("Perbarui arus kas").performScrollTo().assertHeightIsAtLeast(48.dp)
    }

    @Test fun topCategoriesAreNotComputedOnThePhone() {
        // The fixtures hold categorised expenses from today and yesterday; the tracker offers no categories for today.
        show(homeState(captures = captures).copy(ledger = LedgerSummaryState(summary = SUMMARY)))
        listOf("Kategori teratas", "Belanja Harian", "Makan & Minum", "Transportasi", "Parkir").forEach { category ->
            assertTrue("$category is not on Beranda", everyLabel().none { it.contains(category, ignoreCase = true) })
        }
    }
}
