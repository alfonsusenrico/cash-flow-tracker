package com.alfonsusenrico.financialtracker.listener.ui

import android.app.Application
import androidx.compose.ui.semantics.SemanticsActions
import androidx.compose.ui.semantics.SemanticsProperties
import androidx.compose.ui.test.SemanticsMatcher
import androidx.compose.ui.test.assert
import androidx.compose.ui.test.hasContentDescription
import androidx.compose.ui.test.hasText
import androidx.compose.ui.test.junit4.createComposeRule
import androidx.compose.ui.test.onNodeWithContentDescription
import androidx.compose.ui.test.assertHeightIsAtLeast
import androidx.compose.ui.test.onNodeWithTag
import androidx.compose.ui.test.onNodeWithText
import androidx.compose.ui.test.performTextInput
import androidx.compose.ui.unit.dp
import com.alfonsusenrico.financialtracker.listener.data.textcapture.TextCaptures
import com.alfonsusenrico.financialtracker.listener.ui.capture.CAPTURE_INPUT_TAG
import kotlinx.coroutines.flow.first
import androidx.compose.ui.test.performClick
import androidx.compose.ui.test.performScrollTo
import androidx.compose.ui.test.performTouchInput
import androidx.compose.ui.test.swipeLeft
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import androidx.room.Room
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.ui.capture.CaptureViewModel
import com.alfonsusenrico.financialtracker.listener.ui.components.DELETE_ACTION_LABEL
import com.alfonsusenrico.financialtracker.listener.ui.shell.CompanionApp
import com.alfonsusenrico.financialtracker.listener.ui.shell.CompanionViewModel
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionTheme
import com.alfonsusenrico.financialtracker.listener.ui.theme.ThemeMode
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.annotation.Config

/** The real shell over an in-memory database: tabs, notification taps, delete with undo, and the theme choice. */
@RunWith(AndroidJUnit4::class)
// A tall screen keeps every fixture row of the lazy feed composed.
@Config(sdk = [34], application = com.alfonsusenrico.financialtracker.listener.TestApplication::class, qualifiers = "w411dp-h1600dp")
class CompanionShellUiTest {
    @get:Rule val compose = createComposeRule()

    private val app = ApplicationProvider.getApplicationContext<Application>()
    private lateinit var db: AppDatabase
    private lateinit var pairing: String
    private val viewModels = mutableListOf<ViewModel>()

    @Before fun open() {
        db = Room.inMemoryDatabaseBuilder(app, AppDatabase::class.java).build()
        pairing = AppPreferences(app).pairingContext
        runBlocking {
            listOf(UiFixtures.purpose(), UiFixtures.sender(), UiFixtures.recorded(20, "Kopi Contoh", 32_000), UiFixtures.review())
                .forEach { db.notificationDao().insertOrIgnore(it.copy(pairingContext = pairing)) }
        }
    }

    @After fun close() {
        // The view models outlive the composition here; stop their database work before closing it.
        viewModels.forEach { it.viewModelScope.cancel() }
        db.close()
        AppPreferences(app).themeMode = null
    }

    private fun viewModel() = CompanionViewModel(app, db).also { viewModels += it }

    private fun shell(viewModel: CompanionViewModel = viewModel()): CompanionViewModel {
        val captures = CaptureViewModel(app, db).also { viewModels += it }
        compose.setContent { CompanionTheme(dark = false) { CompanionApp(viewModel, captures, listenerPermissionGranted = true) } }
        return viewModel
    }

    private fun waitFor(matcher: SemanticsMatcher) =
        compose.waitUntil(5_000) { compose.onAllNodes(matcher).fetchSemanticsNodes().isNotEmpty() }

    private val heading = SemanticsMatcher.keyIsDefined(SemanticsProperties.Heading)

    @Test fun bottomBarSwitchesTabsAndCountsOpenQuestions() {
        shell()
        waitFor(hasContentDescription("Tanya, 2 pertanyaan menunggu"))
        compose.onNodeWithContentDescription("Beranda").assertSelectedTab()
        compose.onNodeWithContentDescription("Aktivitas").performClick()
        waitFor(hasText("Aktivitas") and heading)
        compose.onNodeWithContentDescription("Aktivitas").assertSelectedTab()
        compose.onNodeWithContentDescription("Tanya, 2 pertanyaan menunggu").performClick()
        waitFor(hasText("1 dari 2 menunggu"))
        compose.onNodeWithContentDescription("Atur").performClick()
        waitFor(hasText("Sinkron & atur") and heading)
        compose.onNodeWithContentDescription("Beranda").performClick()
        waitFor(hasText("Kas") and heading)
    }

    @Test fun aQuestionOpenedFromTheFeedBelongsToTanya() {
        shell()
        compose.onNodeWithContentDescription("Aktivitas").performClick()
        waitFor(hasContentDescription("Siapa pengirimnya?", substring = true))
        compose.onNodeWithContentDescription("Siapa pengirimnya?", substring = true).performClick()
        waitFor(hasText("Siapa pengirimnya?") and heading)
        compose.onNodeWithContentDescription("Tanya, 2 pertanyaan menunggu").assertSelectedTab()
        compose.onNodeWithContentDescription("Aktivitas").performClick()
        waitFor(hasText("Aktivitas") and heading)
        compose.onNodeWithContentDescription("Aktivitas").assertSelectedTab()
    }

    @Test fun questionNotificationOpensTanyaAndOthersOpenRincian() {
        val viewModel = shell()
        viewModel.requestOpen(UiFixtures.sender().id)
        waitFor(hasText("Siapa pengirimnya?") and heading)
        compose.onNodeWithText("2 dari 2 menunggu").assertExists()
        viewModel.requestOpen(20)
        waitFor(hasText("Rincian") and heading)
        compose.onNodeWithText("Yang dicatat tracker").assertExists()
        compose.onNodeWithContentDescription("Kembali").performClick()
        waitFor(hasText("Siapa pengirimnya?") and heading)
    }

    @Test fun deletingFromTheFeedCanBeUndone() {
        shell()
        compose.onNodeWithContentDescription("Aktivitas").performClick()
        waitFor(hasContentDescription("Kopi Contoh", substring = true))
        val action = compose.onNodeWithContentDescription("Kopi Contoh", substring = true).fetchSemanticsNode()
            .config[SemanticsActions.CustomActions].single { it.label == DELETE_ACTION_LABEL }
        compose.runOnIdle { action.action() }
        waitFor(hasText("Dihapus dari aplikasi"))
        compose.waitUntil(5_000) { runBlocking { db.notificationDao().getById(20)!!.processingStatus } == "dismissed" }
        compose.waitUntil(5_000) { compose.onAllNodes(hasContentDescription("Kopi Contoh", substring = true)).fetchSemanticsNodes().isEmpty() }
        compose.onNodeWithText("Urungkan").performClick()
        compose.waitUntil(5_000) { runBlocking { db.notificationDao().getById(20)!!.processingStatus } == "recorded" }
        waitFor(hasContentDescription("Kopi Contoh", substring = true))
    }

    @Test fun aRowSwipedAwayAndRestoredStaysRestored() {
        shell()
        compose.onNodeWithContentDescription("Aktivitas").performClick()
        waitFor(hasContentDescription("Kopi Contoh", substring = true))
        compose.onNodeWithContentDescription("Kopi Contoh", substring = true).performTouchInput { swipeLeft() }
        waitFor(hasText("Dihapus dari aplikasi"))
        compose.waitUntil(5_000) { runBlocking { db.notificationDao().getById(20)!!.processingStatus } == "dismissed" }
        compose.onNodeWithText("Urungkan").performClick()
        waitFor(hasContentDescription("Kopi Contoh", substring = true))
        // The restored row must not be swiped away again by a remembered dismissed state.
        compose.mainClock.advanceTimeBy(2_000)
        compose.waitForIdle()
        assertEquals("recorded", runBlocking { db.notificationDao().getById(20)!!.processingStatus })
        compose.onNodeWithText("Dihapus dari aplikasi").assertDoesNotExist()
    }

    @Test fun deletingFromRincianGoesBackAndOffersUndo() {
        val viewModel = shell()
        viewModel.requestOpen(UiFixtures.review().id)
        waitFor(hasText("Rincian") and heading)
        compose.onNodeWithContentDescription("Hapus dari aplikasi").performClick()
        compose.onNodeWithText("Hapus").performClick()
        waitFor(hasText("Dihapus dari aplikasi"))
        waitFor(hasText("Kas") and heading)
        compose.onNodeWithText("Urungkan").performClick()
        compose.waitUntil(5_000) { runBlocking { db.notificationDao().getById(UiFixtures.review().id)!!.processingStatus } == "needs_review" }
    }

    @Test fun catatOpensTheSheetAndATypedCaptureWaitsInTheQueueWithoutATracker() {
        shell()
        compose.onNodeWithContentDescription("Catat transaksi").assertHeightIsAtLeast(48.dp).performClick()
        waitFor(hasText("Proses dengan AI"))
        compose.onNodeWithText("Catat cepat").assertExists()
        compose.onNodeWithTag(CAPTURE_INPUT_TAG).performTextInput("kopi contoh 25rb")
        compose.onNodeWithText("Proses dengan AI").performClick()
        waitFor(hasText("Disimpan di antrean"))
        assertEquals(TextCaptures.NOT_PAIRED, runBlocking { db.textCaptureDao().observeVisible().first().single().errorCode })
        compose.onNodeWithText("Lihat antrean").performScrollTo().performClick()
        waitFor(hasContentDescription("kopi contoh 25rb, Nominal belum jelas, Di antrean", substring = true))
        compose.onNodeWithContentDescription("Di ponsel, 1").assertSelectedTab()
    }

    @Test fun closingTheSheetReturnsToTheTab() {
        shell()
        compose.onNodeWithContentDescription("Catat transaksi").performClick()
        waitFor(hasText("Proses dengan AI"))
        compose.onNodeWithContentDescription("Tutup catat cepat").performClick()
        compose.waitUntil(5_000) { compose.onAllNodes(hasText("Proses dengan AI")).fetchSemanticsNodes().isEmpty() }
        compose.onNodeWithContentDescription("Beranda").assertSelectedTab()
    }

    @Test fun themeChoiceIsStoredAndReadBack() {
        assertNull(AppPreferences(app).themeMode)
        assertEquals(ThemeMode.SYSTEM, viewModel().themeMode.value)
        val viewModel = shell()
        compose.onNodeWithContentDescription("Atur").performClick()
        waitFor(hasText("Gelap"))
        compose.onNodeWithText("Gelap").performScrollTo().performClick()
        compose.runOnIdle { assertEquals(ThemeMode.DARK, viewModel.themeMode.value) }
        assertEquals("dark", AppPreferences(app).themeMode)
        assertEquals(ThemeMode.DARK, viewModel().themeMode.value)
    }

    private fun androidx.compose.ui.test.SemanticsNodeInteraction.assertSelectedTab() =
        this.assert(SemanticsMatcher.expectValue(SemanticsProperties.Selected, true))
}
