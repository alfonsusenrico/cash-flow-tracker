package com.alfonsusenrico.financialtracker.listener.ui.components

import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.compose.ui.semantics.clearAndSetSemantics
import androidx.compose.ui.semantics.contentDescription
import androidx.compose.ui.text.TextStyle
import androidx.compose.ui.text.style.TextAlign
import com.alfonsusenrico.financialtracker.listener.ui.model.MoneyDirection
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionTheme
import com.alfonsusenrico.financialtracker.listener.ui.theme.TABULAR_FIGURES

/** What TalkBack reads for an amount: the direction in words, never just a sign. */
fun moneyDescription(amount: Long?, direction: MoneyDirection?): String {
    if (amount == null) return "Nominal belum jelas"
    val word = when (direction) {
        MoneyDirection.IN -> "Masuk "
        MoneyDirection.OUT -> "Keluar "
        MoneyDirection.MOVE -> "Pindah "
        null -> ""
    }
    return word + Formatting.rupiah(amount)
}

fun moneyText(amount: Long?, direction: MoneyDirection?): String {
    if (amount == null) return "Rp —"
    val sign = when (direction) {
        MoneyDirection.IN -> "+"
        MoneyDirection.OUT -> "−"
        else -> ""
    }
    return sign + Formatting.rupiah(amount)
}

/** An amount with its sign and colour; an unknown amount shows "Rp —", never Rp0. */
@Composable
fun MoneyText(
    amount: Long?,
    direction: MoneyDirection?,
    style: TextStyle,
    modifier: Modifier = Modifier,
    textAlign: TextAlign? = null
) {
    val colors = CompanionTheme.colors
    val color = when {
        amount == null -> colors.text2
        direction == MoneyDirection.IN -> colors.moneyIn
        direction == MoneyDirection.MOVE -> colors.moveFg
        else -> colors.moneyOut
    }
    val description = moneyDescription(amount, direction)
    Text(
        moneyText(amount, direction),
        style = style.copy(fontFeatureSettings = TABULAR_FIGURES),
        color = color,
        textAlign = textAlign,
        modifier = modifier.clearAndSetSemantics { contentDescription = description }
    )
}
