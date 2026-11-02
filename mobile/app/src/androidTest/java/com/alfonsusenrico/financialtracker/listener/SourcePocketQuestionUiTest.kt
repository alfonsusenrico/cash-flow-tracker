package com.alfonsusenrico.financialtracker.listener

import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.runtime.CompositionLocalProvider
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.test.*
import androidx.compose.ui.test.junit4.createComposeRule
import androidx.compose.ui.unit.Density
import com.alfonsusenrico.financialtracker.listener.data.network.model.*
import com.alfonsusenrico.financialtracker.listener.ui.questions.SourcePocketOptionState
import com.alfonsusenrico.financialtracker.listener.ui.questions.SourcePocketQuestionControls
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionTheme
import org.junit.Assert.*
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

@RunWith(Parameterized::class)
class SourcePocketQuestionUiTest(private val darkTheme: Boolean) {
    companion object {
        @JvmStatic @Parameterized.Parameters(name = "dark={0}")
        fun themes() = listOf(arrayOf(true), arrayOf(false))
    }
    @get:Rule val compose = createComposeRule()
    private val question = ConfirmationQuestion("source_pocket", "fictional", institution = "jago", amount = 112590,
        currency = "IDR", jagoAccount = AccountChoice("parent", "Bank Jago"), destinationLabel = "Andra")
    private val options = SourcePocketOptionState(questionId = "fictional", options = listOf(
        AccountChoice("main", "Bank Jago · Kantong Utama"), AccountChoice("emergency", "Bank Jago · Dana Darurat")))

    @Test fun explicitNonMainChoiceIsRequiredAtEnlargedText() {
        val answers = mutableListOf<String>()
        compose.setContent {
            val density = LocalDensity.current
            CompositionLocalProvider(LocalDensity provides Density(density.density, 2f)) {
                CompanionTheme(dark = darkTheme) { Column(Modifier.verticalScroll(rememberScrollState())) {
                    SourcePocketQuestionControls(question, options, null, null, false, true, {}, answers::add)
                } }
            }
        }
        compose.onNodeWithText("Konfirmasi kantong").performScrollTo().assertIsNotEnabled()
        compose.onNodeWithText("Bank Jago · Kantong Utama").performScrollTo().assertIsNotSelected()
        compose.onNodeWithText("Bank Jago · Dana Darurat").performScrollTo().performClick().assertIsSelected()
        compose.onNodeWithText("Konfirmasi kantong").performScrollTo().performClick()
        assertEquals(listOf("emergency"), answers)
    }

    @Test fun queuedReplyIsPendingAndCannotBeAnsweredTwice() {
        compose.setContent { CompanionTheme(dark = darkTheme) {
            SourcePocketQuestionControls(question, options, "queued", "emergency", false, true, {}, { fail("Duplicate") })
        } }
        compose.onNodeWithText("Pilihan menunggu koneksi; belum dicatat.").assertIsDisplayed()
        compose.onNodeWithText("Konfirmasi kantong").assertDoesNotExist()
        compose.onNodeWithText("Tercatat").assertDoesNotExist()
    }

    @Test fun staleOptionsCannotBeSelectedAndRefreshRemainsAvailable() {
        var refreshed = false
        compose.setContent { CompanionTheme(dark = darkTheme) {
            SourcePocketQuestionControls(question, options.copy(questionId = "old"), "stale", null, false, true,
                { refreshed = true }, { fail("Stale option") })
        } }
        compose.onNodeWithText("Bank Jago · Kantong Utama").assertDoesNotExist()
        compose.onNodeWithText("Konfirmasi kantong").assertIsNotEnabled()
        compose.onNodeWithText("Segarkan kantong").performClick()
        assertTrue(refreshed)
    }
}
