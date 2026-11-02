package com.alfonsusenrico.financialtracker.listener.ui.theme

import androidx.compose.material3.Typography
import androidx.compose.ui.text.ExperimentalTextApi
import androidx.compose.ui.text.TextStyle
import androidx.compose.ui.text.font.Font
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.text.font.FontVariation
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.TextUnit
import androidx.compose.ui.unit.em
import androidx.compose.ui.unit.sp
import com.alfonsusenrico.financialtracker.listener.R

/** One instance of the bundled Archivo variable font (OFL, see assets/licenses/archivo-OFL.txt). */
@OptIn(ExperimentalTextApi::class)
private fun archivo(weight: Int, width: Float) = Font(
    R.font.archivo,
    weight = FontWeight(weight),
    variationSettings = FontVariation.Settings(FontVariation.weight(weight), FontVariation.width(width))
)

/** Running text at the font's normal width. */
val ArchivoText = FontFamily(
    archivo(400, 100f), archivo(500, 100f), archivo(600, 100f), archivo(650, 100f), archivo(700, 100f),
    archivo(750, 100f)
)

/** Display numbers and titles: weight 700 at 104 % width, the owner's quieter version of option E (850, 112 %). */
val ArchivoDisplay = FontFamily(archivo(700, 104f))

/** Money, counts and times line up in columns. */
const val TABULAR_FIGURES = "tnum"

object CompanionType {
    fun display(size: TextUnit) = TextStyle(
        fontFamily = ArchivoDisplay, fontWeight = FontWeight(700), fontSize = size,
        letterSpacing = (-0.02).em, lineHeight = size * 1.12, fontFeatureSettings = TABULAR_FIGURES
    )

    /** Small uppercase tile and section labels. */
    val label = TextStyle(
        fontFamily = ArchivoText, fontWeight = FontWeight(650), fontSize = 12.sp, letterSpacing = 0.07.em,
        lineHeight = 16.sp
    )
    val body = TextStyle(fontFamily = ArchivoText, fontWeight = FontWeight(400), fontSize = 15.sp, lineHeight = 22.sp)
    val bodyStrong = body.copy(fontWeight = FontWeight(700))
    val small = TextStyle(fontFamily = ArchivoText, fontWeight = FontWeight(500), fontSize = 13.5.sp, lineHeight = 19.sp)
    val meta = TextStyle(fontFamily = ArchivoText, fontWeight = FontWeight(500), fontSize = 12.5.sp, lineHeight = 17.sp)
    val button = TextStyle(fontFamily = ArchivoText, fontWeight = FontWeight(650), fontSize = 15.sp, lineHeight = 20.sp)
    val cardTitle = TextStyle(fontFamily = ArchivoText, fontWeight = FontWeight(700), fontSize = 15.sp, lineHeight = 20.sp)
    val money = TextStyle(
        fontFamily = ArchivoText, fontWeight = FontWeight(750), fontSize = 15.5.sp, lineHeight = 21.sp,
        fontFeatureSettings = TABULAR_FIGURES
    )

    // Title sizes from the final README: page titles 18–20, totals 34, counts 30–38, question titles 21,
    // amounts 28–32, onboarding titles 25.
    val homeTitle = display(20.sp)
    val pageTitle = display(18.sp)
    val todayTotal = display(34.sp)
    val questionCount = display(38.sp)
    val tileCount = display(30.sp)
    val tileState = display(20.sp)
    val questionTitle = display(21.sp)
    val questionAmount = display(28.sp)
    val heroAmount = display(32.sp)
    val onboardingTitle = display(25.sp)
}

val CompanionTypography = Typography().let { base ->
    Typography(
        displayLarge = base.displayLarge.copy(fontFamily = ArchivoDisplay),
        displayMedium = base.displayMedium.copy(fontFamily = ArchivoDisplay),
        displaySmall = base.displaySmall.copy(fontFamily = ArchivoDisplay),
        headlineLarge = base.headlineLarge.copy(fontFamily = ArchivoDisplay),
        headlineMedium = base.headlineMedium.copy(fontFamily = ArchivoDisplay),
        headlineSmall = base.headlineSmall.copy(fontFamily = ArchivoDisplay),
        titleLarge = base.titleLarge.copy(fontFamily = ArchivoText, fontWeight = FontWeight(700)),
        titleMedium = base.titleMedium.copy(fontFamily = ArchivoText, fontWeight = FontWeight(700)),
        titleSmall = base.titleSmall.copy(fontFamily = ArchivoText),
        bodyLarge = base.bodyLarge.copy(fontFamily = ArchivoText),
        bodyMedium = base.bodyMedium.copy(fontFamily = ArchivoText),
        bodySmall = base.bodySmall.copy(fontFamily = ArchivoText),
        labelLarge = base.labelLarge.copy(fontFamily = ArchivoText, fontWeight = FontWeight(650)),
        labelMedium = base.labelMedium.copy(fontFamily = ArchivoText),
        labelSmall = base.labelSmall.copy(fontFamily = ArchivoText)
    )
}
