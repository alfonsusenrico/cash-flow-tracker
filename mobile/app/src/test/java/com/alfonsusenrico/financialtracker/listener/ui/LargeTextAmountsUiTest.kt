package com.alfonsusenrico.financialtracker.listener.ui

import androidx.compose.ui.semantics.SemanticsActions
import androidx.compose.ui.semantics.SemanticsNode
import androidx.compose.ui.semantics.getOrNull
import androidx.compose.ui.test.junit4.createComposeRule
import androidx.compose.ui.test.onNodeWithContentDescription
import androidx.compose.ui.test.onRoot
import androidx.compose.ui.text.TextLayoutResult
import com.alfonsusenrico.financialtracker.listener.ui.activity.ActivityScreen
import com.alfonsusenrico.financialtracker.listener.ui.home.HomeScreen
import com.alfonsusenrico.financialtracker.listener.ui.model.ActivityFilter
import com.alfonsusenrico.financialtracker.listener.ui.shell.LedgerSummaryState
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.ParameterizedRobolectricTestRunner
import org.robolectric.annotation.Config
import org.robolectric.annotation.GraphicsMode

/**
 * At font scale 2.0 no amount breaks across lines, and Aktivitas' day header wraps only between words.
 * Every text is checked through its real text layout, including texts under a tile's single TalkBack node.
 */
// Native graphics measures the bundled Archivo font as a phone does; the legacy mode fakes text widths.
@GraphicsMode(GraphicsMode.Mode.NATIVE)
@RunWith(ParameterizedRobolectricTestRunner::class)
@Config(sdk = [34], application = com.alfonsusenrico.financialtracker.listener.TestApplication::class)
class LargeTextAmountsUiTest(private val dark: Boolean) {
    companion object {
        @JvmStatic @ParameterizedRobolectricTestRunner.Parameters(name = "dark={0}")
        fun themes() = listOf(arrayOf<Any>(false), arrayOf<Any>(true))

        /** Day headers are upper case ("KELUAR RP59.000"). */
        private val AMOUNT = Regex("""-?Rp[0-9.]+""", RegexOption.IGNORE_CASE)
    }

    @get:Rule val compose = createComposeRule()

    private fun layouts(): List<TextLayoutResult> {
        val found = mutableListOf<TextLayoutResult>()
        fun walk(node: SemanticsNode) {
            node.config.getOrNull(SemanticsActions.GetTextLayoutResult)?.action?.invoke(found)
            node.children.forEach(::walk)
        }
        walk(compose.onRoot(useUnmergedTree = true).fetchSemanticsNode())
        return found
    }

    /** Every amount starts and ends on the same line and, when its text is one line, fits the width it was given. */
    private fun assertNoAmountBreaks() {
        val checked = mutableListOf<String>()
        layouts().forEach { layout ->
            val text = layout.layoutInput.text.text
            AMOUNT.findAll(text).forEach { match ->
                val first = layout.getLineForOffset(match.range.first)
                val last = layout.getLineForOffset(match.range.last)
                assertEquals("${match.value} in \"$text\" stays on one line", first, last)
                if (layout.lineCount == 1) {
                    assertTrue(
                        "\"$text\" (${layout.multiParagraph.intrinsics.maxIntrinsicWidth} px) fits its ${layout.size.width} px",
                        layout.multiParagraph.intrinsics.maxIntrinsicWidth <= layout.size.width + 0.5f
                    )
                }
                checked += match.value
            }
        }
        assertTrue("amounts were checked: $checked", checked.isNotEmpty())
    }

    private fun showHome() {
        compose.show(dark, fontScale = 2f) {
            HomeScreen(homeState().copy(ledger = LedgerSummaryState(summary = TopCategoriesUiTest.SUMMARY)), {}, {}, {}, {})
        }
        // The spoken figures are unchanged.
        listOf("Uang keluar tracker Rp640.000", "Masuk Rp1.100.000", "Neto Rp460.000", "Keluar kotor Rp690.000", "Dikurangi penggantian Rp50.000")
            .forEach { compose.onNodeWithContentDescription(it).assertExists() }
    }

    private fun showActivity() {
        val lists = fixtureLists()
        compose.show(dark, fontScale = 2f) {
            ActivityScreen(lists, ActivityFilter.ALL, lists.visible.size, 0, UiFixtures.today, {}, { _, _ -> }, {}, {})
        }
    }

    /** The day label wraps only at spaces, in at most two lines, and never splits its date ("KAMIS, 29 OKT"). */
    private fun assertDayHeaderWrapsBetweenWords() {
        val headers = layouts().filter { it.layoutInput.text.text.startsWith("HARI INI") || it.layoutInput.text.text.startsWith("KEMARIN") }
        assertTrue("day headers are shown", headers.isNotEmpty())
        headers.forEach { layout ->
            val text = layout.layoutInput.text.text
            assertTrue("\"$text\" takes ${layout.lineCount} lines", layout.lineCount <= 2)
            for (line in 1 until layout.lineCount) {
                val start = layout.getLineStart(line)
                assertTrue("\"$text\" breaks between words at $start", text[start - 1].isWhitespace())
            }
            val date = text.indexOf("· ").let { if (it < 0) 0 else it + 2 }
            assertEquals("the date in \"$text\" stays on one line", layout.getLineForOffset(date), layout.getLineForOffset(text.length - 1))
        }
    }

    @Config(qualifiers = "w360dp-h740dp")
    @Test fun berandaAmountsStayWholeOnA360dpPhone() {
        showHome()
        assertNoAmountBreaks()
    }

    @Config(qualifiers = "w320dp-h640dp")
    @Test fun berandaAmountsStayWholeOnA320dpPhone() {
        showHome()
        assertNoAmountBreaks()
    }

    @Config(qualifiers = "w360dp-h740dp")
    @Test fun aktivitasDayHeaderWrapsCleanlyOnA360dpPhone() {
        showActivity()
        assertDayHeaderWrapsBetweenWords()
        assertNoAmountBreaks()
    }

    @Config(qualifiers = "w320dp-h640dp")
    @Test fun aktivitasDayHeaderWrapsCleanlyOnA320dpPhone() {
        showActivity()
        assertDayHeaderWrapsBetweenWords()
        assertNoAmountBreaks()
    }
}
