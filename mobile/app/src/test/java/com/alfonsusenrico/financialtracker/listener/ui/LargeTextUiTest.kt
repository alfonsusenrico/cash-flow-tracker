package com.alfonsusenrico.financialtracker.listener.ui

import android.app.Application
import androidx.compose.ui.semantics.SemanticsProperties
import androidx.compose.ui.test.SemanticsMatcher
import androidx.compose.ui.test.assertHeightIsAtLeast
import androidx.compose.ui.test.assertIsDisplayed
import androidx.compose.ui.test.hasContentDescription
import androidx.compose.ui.test.hasScrollToNodeAction
import androidx.compose.ui.test.junit4.createComposeRule
import androidx.compose.ui.test.onNodeWithContentDescription
import androidx.compose.ui.test.onNodeWithText
import androidx.compose.ui.test.performClick
import androidx.compose.ui.test.performScrollTo
import androidx.compose.ui.test.performScrollToNode
import androidx.compose.ui.unit.dp
import androidx.compose.material3.Text
import androidx.test.ext.junit.runners.AndroidJUnit4
import com.alfonsusenrico.financialtracker.listener.ui.activity.ActivityScreen
import com.alfonsusenrico.financialtracker.listener.ui.detail.DetailActions
import com.alfonsusenrico.financialtracker.listener.ui.detail.DetailScreen
import com.alfonsusenrico.financialtracker.listener.ui.home.HomeScreen
import com.alfonsusenrico.financialtracker.listener.ui.model.ActivityFilter
import com.alfonsusenrico.financialtracker.listener.ui.model.SyncHealth
import com.alfonsusenrico.financialtracker.listener.ui.questions.QuestionAnswers
import com.alfonsusenrico.financialtracker.listener.ui.questions.QuestionsScreen
import com.alfonsusenrico.financialtracker.listener.ui.questions.SourcePocketOptionState
import com.alfonsusenrico.financialtracker.listener.ui.settings.AppToggle
import com.alfonsusenrico.financialtracker.listener.ui.settings.SettingsActions
import com.alfonsusenrico.financialtracker.listener.ui.settings.SettingsScreen
import com.alfonsusenrico.financialtracker.listener.ui.settings.SettingsState
import com.alfonsusenrico.financialtracker.listener.ui.theme.ThemeMode
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.annotation.Config

/** The densest screens at font scale 2.0: every control stays reachable and at least 48 dp high. */
@RunWith(AndroidJUnit4::class)
@Config(sdk = [34], application = com.alfonsusenrico.financialtracker.listener.TestApplication::class, qualifiers = "w360dp-h740dp")
class LargeTextUiTest {
    @get:Rule val compose = createComposeRule()
    private val lists = fixtureLists()

    @Test fun homeStacksItsTilesAndKeepsThemReachable() {
        compose.show(dark = true, fontScale = 2f) { HomeScreen(homeState(), {}, {}, {}, {}) }
        compose.onNodeWithContentDescription("Pertanyaan, 4 menunggu", substring = true).performScrollTo().assertHeightIsAtLeast(48.dp)
        compose.onNodeWithContentDescription("Aktivitas ponsel hari ini", substring = true).performScrollTo().assertHeightIsAtLeast(48.dp)
        compose.onNodeWithContentDescription("Di ponsel, 1. Belum dikirim").performScrollTo().assertIsDisplayed()
        compose.onNodeWithContentDescription("Pengaturan").assertHeightIsAtLeast(48.dp)
    }

    @Test fun purposeQuestionOptionsStayReachable() {
        compose.show(dark = false, fontScale = 2f) {
            QuestionsScreen(UiFixtures.purpose(), lists.questions, emptyList(), UiFixtures.PAIRING, emptyList(), SourcePocketOptionState(),
                false, UiFixtures.today, {}, {}, {}, QuestionAnswers({ _, _, _, _ -> }, { _, _, _ -> }, { _, _, _ -> }, {}, { _, _ -> }))
        }
        compose.onNodeWithContentDescription("Jawab: Makan keluarga, kategori Makan & Minum").performScrollTo().assertHeightIsAtLeast(56.dp)
        compose.onNodeWithText("Tulis sendiri").performScrollTo().performClick()
        compose.onNodeWithText("Kirim jawaban").performScrollTo().assertHeightIsAtLeast(48.dp)
        compose.onNodeWithContentDescription("Pertanyaan berikutnya").performScrollTo().assertHeightIsAtLeast(48.dp)
    }

    @Test fun detailEditFormStaysReachable() {
        compose.show(dark = true, fontScale = 2f) {
            DetailScreen(UiFixtures.recorded(20, "Kopi Contoh", 32_000), emptyList(), UiFixtures.PAIRING, emptyList(), emptyList(), false,
                UiFixtures.today, DetailActions({}, {}, {}, {}, {}, {}, {}, { _, _, _, _, _ -> }))
        }
        compose.onNodeWithText("Ubah transaksi").performScrollTo().performClick()
        compose.onNodeWithText("Masuk").performScrollTo().assertHeightIsAtLeast(48.dp)
        compose.onNodeWithText("Perbarui").performScrollTo().assertHeightIsAtLeast(48.dp)
        compose.onNodeWithText("Notifikasi asli").performScrollTo().assertIsDisplayed()
    }

    @Test fun feedFiltersAndRowsStayReachable() {
        compose.show(dark = false, fontScale = 2f) {
            ActivityScreen(lists, ActivityFilter.ALL, 9, 2, UiFixtures.today, {}, { _, _ -> }, {}, {})
        }
        compose.onNodeWithContentDescription("Diabaikan, 1").performScrollTo().assertHeightIsAtLeast(48.dp)
        compose.onNode(hasScrollToNodeAction() and SemanticsMatcher.keyIsDefined(SemanticsProperties.VerticalScrollAxisRange))
            .performScrollToNode(hasContentDescription("Ojek Contoh", substring = true))
        compose.onNodeWithContentDescription("Ojek Contoh", substring = true).assertHeightIsAtLeast(64.dp)
    }

    @Test fun settingsStayReachable() {
        compose.show(dark = true, fontScale = 2f) {
            SettingsScreen(
                SettingsState(SyncHealth(true, 0, 0, 0), true, listOf(AppToggle("com.gojek.gopay", "GoPay", true)), true, true, ThemeMode.SYSTEM),
                SettingsActions({}, {}, { _, _ -> }, {}, {}, {}, {}),
                connection = { Text("Koneksi") }
            )
        }
        compose.onNodeWithText("GoPay").performScrollTo().assertIsDisplayed()
        compose.onNodeWithText("Gelap").performScrollTo().assertHeightIsAtLeast(48.dp)
    }
}
