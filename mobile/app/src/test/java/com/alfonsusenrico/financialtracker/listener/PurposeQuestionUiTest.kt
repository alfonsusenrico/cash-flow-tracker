package com.alfonsusenrico.financialtracker.listener

import android.app.Application
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
import androidx.compose.ui.unit.dp
import androidx.test.ext.junit.runners.AndroidJUnit4
import com.alfonsusenrico.financialtracker.listener.data.network.model.PurposeQuestion
import com.alfonsusenrico.financialtracker.listener.data.network.model.PurposeSuggestion
import com.alfonsusenrico.financialtracker.listener.ui.questions.PurposeQuestionControls
import org.junit.Assert.*
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.annotation.Config

/** Robolectric runs these Compose checks on the JVM, so they execute without a device. */
@RunWith(AndroidJUnit4::class)
@Config(sdk = [34], application = com.alfonsusenrico.financialtracker.listener.TestApplication::class)
class PurposeQuestionUiTest {
    @get:Rule val compose = createComposeRule()
    private val question = PurposeQuestion(
        type = "purpose", questionId = "00000000-0000-4000-8000-000000000401",
        transactionId = "00000000-0000-4000-8000-000000000402", direction = "expense", amount = 187500,
        currency = "IDR", prompt = "Apa ini? Rp187.500 dari BCA Harian · kategori bank: Groceries",
        allowsText = true, textMaxLength = 200,
        suggestions = listOf(
            PurposeSuggestion("s1", "history", "makan kafe senja", "Makanan & Minuman"),
            PurposeSuggestion("s2", "current", "Pengeluaran Groceries", null)
        )
    )

    @Test fun chipsAndTextAnswerAreReachableAtLargeText() {
        val answers = mutableListOf<Pair<String?, String?>>()
        compose.setContent {
            val density = LocalDensity.current
            CompositionLocalProvider(LocalDensity provides Density(density.density, 2f)) {
                MaterialTheme { Column(Modifier.verticalScroll(rememberScrollState())) {
                    PurposeQuestionControls(question, null, null, false, true) { id, text -> answers.add(id to text) }
                } }
            }
        }
        compose.onNodeWithContentDescription("Jawab: makan kafe senja, kategori Makanan & Minuman")
            .performScrollTo().assertHeightIsAtLeast(48.dp).assertHasClickAction().performClick()
        compose.onNodeWithContentDescription("Jawab: Pengeluaran Groceries").performScrollTo().performClick()
        assertEquals(listOf<Pair<String?, String?>>("s1" to null, "s2" to null), answers)

        compose.onNodeWithText("Tulis sendiri").performScrollTo().assertHeightIsAtLeast(48.dp).performClick()
        compose.onNodeWithText("Kirim jawaban").performScrollTo().assertHeightIsAtLeast(48.dp).performClick()
        compose.onNodeWithText("Tulis 1–200 karakter tanpa karakter khusus.").performScrollTo().assertIsDisplayed()
        assertEquals(2, answers.size)
        compose.onNodeWithText("Jawaban kamu").performScrollTo().performTextInput("  sewa lapangan futsal ")
        compose.onNodeWithText("Kirim jawaban").performScrollTo().performClick()
        assertEquals(null to "sewa lapangan futsal", answers.last())
    }

    @Test fun pendingAnswerHidesControls() {
        compose.setContent { MaterialTheme { PurposeQuestionControls(question, "queued", null, false, true) { _, _ -> fail("Duplicate answer") } } }
        compose.onNodeWithText("Jawaban tersimpan; mengirim ke server.").assertIsDisplayed()
        compose.onNodeWithText("Kirim jawaban").assertDoesNotExist()
        compose.onNodeWithText("makan kafe senja").assertDoesNotExist()
    }

    @Test fun failedAnswerShowsWhyAndAllowsAnotherAnswer() {
        compose.setContent { MaterialTheme { PurposeQuestionControls(question, "failed", "unknown_suggestion", false, true) { _, _ -> } } }
        compose.onNodeWithText("Saran ini tidak berlaku lagi. Pilih saran lain atau tulis jawaban.").assertIsDisplayed()
        compose.onNodeWithContentDescription("Jawab: Pengeluaran Groceries").assertIsEnabled()
        compose.onNodeWithText("Tulis sendiri").performClick()
        compose.onNodeWithText("Kirim jawaban").assertExists()
    }

    @Test fun disconnectedAccountDisablesAnswers() {
        compose.setContent { MaterialTheme { PurposeQuestionControls(question, null, null, false, false) { _, _ -> fail("Answered while disconnected") } } }
        compose.onNodeWithText("Hubungkan akun asal notifikasi di Pengaturan.").assertIsDisplayed()
        compose.onNodeWithContentDescription("Jawab: Pengeluaran Groceries").assertIsNotEnabled()
    }
}
