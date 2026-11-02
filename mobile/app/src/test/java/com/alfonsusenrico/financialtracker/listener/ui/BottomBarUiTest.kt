package com.alfonsusenrico.financialtracker.listener.ui

import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.semantics.SemanticsActions
import androidx.compose.ui.semantics.SemanticsProperties
import androidx.compose.ui.semantics.getOrNull
import androidx.compose.ui.test.SemanticsMatcher
import androidx.compose.ui.test.assertHeightIsAtLeast
import androidx.compose.ui.test.assertIsDisplayed
import androidx.compose.ui.test.hasContentDescription
import androidx.compose.ui.test.hasText
import androidx.compose.ui.test.junit4.createComposeRule
import androidx.compose.ui.text.TextLayoutResult
import androidx.compose.ui.unit.dp
import com.alfonsusenrico.financialtracker.listener.ui.shell.CatatButton
import com.alfonsusenrico.financialtracker.listener.ui.shell.CompanionBottomBar
import com.alfonsusenrico.financialtracker.listener.ui.shell.Tab
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.ParameterizedRobolectricTestRunner
import org.robolectric.annotation.Config
import org.robolectric.annotation.GraphicsMode

/** At font scale 2.0 every bottom-bar label is shown whole on one line, with full TalkBack names and 48 dp targets. */
// Native graphics measures the bundled Archivo font as a phone does; the legacy mode fakes text widths.
@GraphicsMode(GraphicsMode.Mode.NATIVE)
@RunWith(ParameterizedRobolectricTestRunner::class)
@Config(sdk = [34], application = com.alfonsusenrico.financialtracker.listener.TestApplication::class)
class BottomBarUiTest(private val dark: Boolean) {
    companion object {
        @JvmStatic @ParameterizedRobolectricTestRunner.Parameters(name = "dark={0}")
        fun themes() = listOf(arrayOf<Any>(false), arrayOf<Any>(true))
    }

    @get:Rule val compose = createComposeRule()

    private fun showBar() = compose.show(dark, fontScale = 2f) {
        Box(Modifier.fillMaxSize(), contentAlignment = Alignment.BottomCenter) {
            CompanionBottomBar(Tab.HOME, questionCount = 4, onSelect = {}) { CatatButton(open = false, onClick = {}) }
        }
    }

    private fun layoutOf(label: String): TextLayoutResult {
        val node = compose.onNode(hasText(label), useUnmergedTree = true).assertIsDisplayed().fetchSemanticsNode()
        val results = mutableListOf<TextLayoutResult>()
        node.config.getOrNull(SemanticsActions.GetTextLayoutResult)?.action?.invoke(results)
        return results.single()
    }

    /**
     * The whole word on one line, within the width the text was given. `hasVisualOverflow` is not used: a
     * one-line text narrower than its constraints reports its paragraph's full constraint width as overflow.
     */
    private fun assertWhole(label: String, layout: TextLayoutResult) {
        assertEquals(label, layout.layoutInput.text.text)
        assertEquals("$label is on one line", 1, layout.lineCount)
        assertEquals("$label ends on its line", label.length, layout.getLineEnd(0))
        assertFalse("$label is not ellipsized", layout.isLineEllipsized(0))
        assertFalse("$label is not cut vertically", layout.didOverflowHeight)
        assertTrue(
            "$label (${layout.multiParagraph.intrinsics.maxIntrinsicWidth} px) fits its ${layout.size.width} px",
            layout.multiParagraph.intrinsics.maxIntrinsicWidth <= layout.size.width
        )
    }

    private fun assertWholeLabels() {
        showBar()
        val spoken = mapOf(
            "Beranda" to "Beranda", "Aktivitas" to "Aktivitas", "Tanya" to "Tanya, 4 pertanyaan menunggu", "Atur" to "Atur"
        )
        spoken.forEach { (label, name) ->
            assertWhole(label, layoutOf(label))
            compose.onNode(hasContentDescription(name) and SemanticsMatcher.keyIsDefined(SemanticsProperties.Selected))
                .assertIsDisplayed().assertHeightIsAtLeast(48.dp)
        }
        assertWhole("Catat", layoutOf("Catat"))
        compose.onNode(hasContentDescription("Catat transaksi")).assertIsDisplayed().assertHeightIsAtLeast(48.dp)
    }

    @Config(qualifiers = "w360dp-h740dp")
    @Test fun labelsAreWholeOnA360dpPhone() = assertWholeLabels()

    @Config(qualifiers = "w320dp-h640dp")
    @Test fun labelsAreWholeOnA320dpPhone() = assertWholeLabels()
}
