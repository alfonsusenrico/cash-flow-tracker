package com.alfonsusenrico.financialtracker.listener

import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.MaterialTheme
import androidx.compose.runtime.CompositionLocalProvider
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.test.*
import androidx.compose.ui.test.junit4.createComposeRule
import androidx.compose.ui.unit.Density
import com.alfonsusenrico.financialtracker.listener.data.network.model.*
import com.alfonsusenrico.financialtracker.listener.ui.questions.SenderQuestionControls
import org.junit.Assert.*
import org.junit.Rule
import org.junit.Test

class SenderQuestionUiTest {
    @get:Rule val compose = createComposeRule()
    private val question = ConfirmationQuestion("sender", "fictional", "AN**RA**", "bca", AccountChoice("fictional", "BCA Daily"), 112590, "IDR")

    @Test fun nameUnknownAndValidationAreReachableAtLargeText() {
        val answers = mutableListOf<String?>()
        compose.setContent {
            val density = LocalDensity.current
            CompositionLocalProvider(LocalDensity provides Density(density.density, 2f)) {
                MaterialTheme { Column(Modifier.verticalScroll(rememberScrollState())) {
                    SenderQuestionControls(question, null, false, true) { answers.add(it) }
                } }
            }
        }
        compose.onNodeWithText("Konfirmasi nama").performScrollTo().performClick()
        compose.onNodeWithText("Isi nama pengirim, maksimal 80 karakter.").performScrollTo().assertIsDisplayed()
        compose.onNodeWithText("Nama pengirim").performScrollTo().performTextInput("A".repeat(81))
        compose.onNodeWithText("Konfirmasi nama").performScrollTo().performClick()
        assertTrue(answers.isEmpty())
        compose.onNodeWithText("Nama pengirim").performTextReplacement("A".repeat(80))
        compose.onNodeWithText("Konfirmasi nama").performScrollTo().performClick()
        assertEquals("A".repeat(80), answers.single())
        compose.onNodeWithText("Catat tanpa nama").performScrollTo().performClick()
        assertNull(answers.last())
    }

    @Test fun queuedReplyDoesNotOfferAnotherAnswerOrClaimRecorded() {
        compose.setContent { MaterialTheme { SenderQuestionControls(question, "queued", false, true) { fail("Duplicate answer") } } }
        compose.onNodeWithText("Balasan tersimpan; menunggu server.").assertIsDisplayed()
        compose.onNodeWithText("Konfirmasi nama").assertDoesNotExist()
        compose.onNodeWithText("Tercatat").assertDoesNotExist()
    }
}
