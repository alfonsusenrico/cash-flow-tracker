package com.alfonsusenrico.financialtracker.listener.ui

import androidx.compose.ui.semantics.SemanticsActions
import androidx.compose.ui.semantics.SemanticsNode
import androidx.compose.ui.semantics.getOrNull
import androidx.compose.ui.test.junit4.createComposeRule
import androidx.compose.ui.test.onRoot
import androidx.compose.ui.text.TextLayoutResult
import com.alfonsusenrico.financialtracker.listener.ui.activity.ActivityScreen
import com.alfonsusenrico.financialtracker.listener.ui.model.ActivityFilter
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.ParameterizedRobolectricTestRunner
import org.robolectric.annotation.Config
import org.robolectric.annotation.GraphicsMode

/**
 * At font scale 2.0 the Riwayat day header wraps only between words and holds no money total.
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

        /** An amount as the phone writes it, in upper or lower case. */
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

    /** No day shows a total of money: each item keeps its own amount, and the day headers only name the day. */
    private fun assertNoDayTotals() {
        val headers = layouts().map { it.layoutInput.text.text }.filter { it.startsWith("HARI INI") || it.startsWith("KEMARIN") }
        assertTrue("day headers are shown", headers.isNotEmpty())
        headers.forEach { assertTrue("\"$it\" holds no amount", AMOUNT.find(it) == null && !it.contains("KELUAR")) }
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
    @Test fun aktivitasDayHeaderWrapsCleanlyOnA360dpPhone() {
        showActivity()
        assertDayHeaderWrapsBetweenWords()
        assertNoDayTotals()
    }

    @Config(qualifiers = "w320dp-h640dp")
    @Test fun aktivitasDayHeaderWrapsCleanlyOnA320dpPhone() {
        showActivity()
        assertDayHeaderWrapsBetweenWords()
        assertNoDayTotals()
    }
}
