package com.alfonsusenrico.financialtracker.listener.ui

import androidx.compose.ui.graphics.Color
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionColors
import com.alfonsusenrico.financialtracker.listener.ui.theme.DarkCompanionColors
import com.alfonsusenrico.financialtracker.listener.ui.theme.LightCompanionColors
import org.junit.Assert.assertTrue
import org.junit.Test
import kotlin.math.max
import kotlin.math.min
import kotlin.math.pow

/**
 * WCAG 2.2 contrast of every foreground/background pair the screens draw, in both themes: 4.5:1 for
 * text, 3:1 for controls and icons that carry meaning.
 */
class ContrastTest {
    private enum class Kind(val minimum: Double) { TEXT(4.5), NON_TEXT(3.0) }

    private data class Pair(val use: String, val fg: (CompanionColors) -> Color, val bg: (CompanionColors) -> Color, val kind: Kind)

    private val pairs = listOf(
        Pair("Text on page", { it.text }, { it.bg }, Kind.TEXT),
        Pair("Text on card or tile", { it.text }, { it.surface }, Kind.TEXT),
        Pair("Text on option", { it.text }, { it.surface2 }, Kind.TEXT),
        Pair("Secondary text on page", { it.text2 }, { it.bg }, Kind.TEXT),
        Pair("Secondary text on card", { it.text2 }, { it.surface }, Kind.TEXT),
        Pair("Secondary text on option", { it.text2 }, { it.surface2 }, Kind.TEXT),
        Pair("Questions tile, suggested option, tab count", { it.onAccent }, { it.accent }, Kind.TEXT),
        Pair("Primary button, selected option", { it.onPrimary }, { it.primary }, Kind.TEXT),
        Pair("Selected filter", { it.bg }, { it.text }, Kind.TEXT),
        Pair("Danger button text on page", { it.danger }, { it.bg }, Kind.TEXT),
        Pair("Danger button text on card", { it.danger }, { it.surface }, Kind.TEXT),
        Pair("Swipe-to-delete label", { it.onDanger }, { it.danger }, Kind.TEXT),
        Pair("Badge: recorded", { it.okFg }, { it.okBg }, Kind.TEXT),
        Pair("Badge: review", { it.warnFg }, { it.warnBg }, Kind.TEXT),
        Pair("Badge: question", { it.askFg }, { it.askBg }, Kind.TEXT),
        Pair("Badge: movement", { it.moveFg }, { it.moveBg }, Kind.TEXT),
        Pair("Badge: on phone", { it.phoneFg }, { it.phoneBg }, Kind.TEXT),
        Pair("Badge: muted", { it.mutedFg }, { it.mutedBg }, Kind.TEXT),
        Pair("Money in on card", { it.moneyIn }, { it.surface }, Kind.TEXT),
        Pair("Money out on card", { it.moneyOut }, { it.surface }, Kind.TEXT),
        Pair("Money moved on card", { it.moveFg }, { it.surface }, Kind.TEXT),
        Pair("Connection result on card", { it.okFg }, { it.surface }, Kind.TEXT),
        Pair("Accent text on card", { it.accentText }, { it.surface }, Kind.TEXT),
        Pair("Snackbar text", { it.snackbarFg }, { it.snackbarBg }, Kind.TEXT),
        Pair("Snackbar action", { it.snackbarAction }, { it.snackbarBg }, Kind.TEXT),
        Pair("Field and ghost-button outline on card", { it.borderStrong }, { it.surface }, Kind.NON_TEXT),
        Pair("Field and ghost-button outline on page", { it.borderStrong }, { it.bg }, Kind.NON_TEXT),
        Pair("Switch on, track", { it.switchOn }, { it.surface }, Kind.NON_TEXT),
        Pair("Switch off, outline and thumb", { it.switchOff }, { it.surface }, Kind.NON_TEXT),
        Pair("Label choice outline on option", { it.borderStrong }, { it.surface2 }, Kind.NON_TEXT),
        Pair("Hub problem symbol, stopped", { it.askFg }, { it.surface }, Kind.NON_TEXT),
        Pair("Hub problem symbol, waiting", { it.warnFg }, { it.surface }, Kind.NON_TEXT),
        Pair("Hub calm illustration outline", { it.text }, { it.surface2 }, Kind.NON_TEXT),
        Pair("Hub link and arrow (Rincian, Lihat semua)", { it.accentText }, { it.surface }, Kind.NON_TEXT),
        Pair("Focus outline", { it.focus }, { it.surface }, Kind.NON_TEXT)
    )

    @Test fun everyPairMeetsWcagAaInBothThemes() {
        val failures = mutableListOf<String>()
        val table = StringBuilder("| Use | Light | Dark | Minimum |\n| --- | --- | --- | --- |\n")
        for (pair in pairs) {
            val light = ratio(pair.fg(LightCompanionColors), pair.bg(LightCompanionColors))
            val dark = ratio(pair.fg(DarkCompanionColors), pair.bg(DarkCompanionColors))
            val minimum = pair.kind.minimum
            table.append("| ${pair.use} | ${"%.2f".format(light)} | ${"%.2f".format(dark)} | ${"%.1f".format(minimum)} |\n")
            if (light < minimum) failures += "${pair.use} light ${"%.2f".format(light)}"
            if (dark < minimum) failures += "${pair.use} dark ${"%.2f".format(dark)}"
        }
        println(table)
        assertTrue("Below WCAG 2.2 AA: $failures", failures.isEmpty())
    }

    private fun ratio(a: Color, b: Color): Double {
        val la = luminance(a)
        val lb = luminance(b)
        return (max(la, lb) + 0.05) / (min(la, lb) + 0.05)
    }

    private fun luminance(color: Color): Double {
        fun channel(c: Float): Double = if (c <= 0.04045) c / 12.92 else ((c + 0.055) / 1.055).pow(2.4)
        return 0.2126 * channel(color.red) + 0.7152 * channel(color.green) + 0.0722 * channel(color.blue)
    }
}
