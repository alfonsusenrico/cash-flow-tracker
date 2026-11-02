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
import com.alfonsusenrico.financialtracker.listener.ui.shell.PhoneState
import com.alfonsusenrico.financialtracker.listener.ui.hub.HubStatus
import kotlinx.coroutines.flow.MutableStateFlow
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

/** The real shell over an in-memory database: the hub, its sheets, notification taps, delete with undo, and the theme choice. */
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
        compose.setContent {
            CompanionTheme(dark = false) {
                CompanionApp(viewModel, captures, PhoneState(), now = { UiFixtures.at(UiFixtures.today, 10, 0) }, listener = MutableStateFlow(true))
            }
        }
        return viewModel
    }

    private fun waitFor(matcher: SemanticsMatcher) =
        compose.waitUntil(5_000) { compose.onAllNodes(matcher).fetchSemanticsNodes().isNotEmpty() }

    private fun waitGone(matcher: SemanticsMatcher) =
        compose.waitUntil(5_000) { compose.onAllNodes(matcher).fetchSemanticsNodes().isEmpty() }

    private val heading = SemanticsMatcher.keyIsDefined(SemanticsProperties.Heading)

    private fun openTile(label: String, title: String) {
        compose.onNodeWithText(label).performScrollTo().assertHeightIsAtLeast(48.dp).performClick()
        waitFor(hasText(title) and heading)
    }

    private fun closeSheet() {
        compose.onNodeWithText("Tutup").performClick()

        waitFor(hasText(HubStatus.CALM_TITLE) and heading)
    }

    @Test fun hubShowsTheStatusAndTheNextQuestionAndOpensEverySheet() {
        shell()
        waitFor(hasText("Ada 2 pertanyaan") and heading)
        compose.onNode(hasText(HubStatus.CALM_TITLE) and heading).assertExists()
        compose.onNodeWithText("notifikasi ketangkep hari ini", substring = true).assertExists()
        // The urgent sender question comes first, answerable in place, before the light purpose question.
        compose.onNodeWithText("Perlu sekarang").assertExists()
        compose.onNode(hasText("Siapa pengirimnya?") and heading).performScrollTo().assertExists()
        compose.onNodeWithText("1 bisa nanti malam, 20.00").assertExists()
        openTile("Riwayat", "Riwayat tangkapan")
        closeSheet()
        openTile("Pengaturan", "Pengaturan")
        closeSheet()
        openTile("Buka web", "Buka di web")
        compose.onNodeWithText("Buku Kas").assertExists()
        closeSheet()
        compose.onNodeWithContentDescription("Buka rincian status").performScrollTo().performClick()
        waitFor(hasText("Rincian status") and heading)
        compose.onNodeWithText("Tes HP-ku").performScrollTo().assertExists()
        closeSheet()
        compose.onNodeWithContentDescription("Lihat semua pertanyaan, 2").performScrollTo().performClick()
        waitFor(hasText("1 dari 2 menunggu"))
    }

    @Test fun aQuestionOpenedFromRiwayatOpensTheQuestionSheet() {
        shell()
        openTile("Riwayat", "Riwayat tangkapan")
        waitFor(hasContentDescription("Siapa pengirimnya?", substring = true))
        compose.onNodeWithContentDescription("Siapa pengirimnya?", substring = true).performClick()
        waitFor(hasText("Pertanyaan") and heading)
        waitFor(hasText("Siapa pengirimnya?") and heading)
    }

    @Test fun questionNotificationOpensTheQuestionSheetAndOthersOpenRincian() {
        val viewModel = shell()
        viewModel.requestOpen(UiFixtures.sender().id)
        waitFor(hasText("Pertanyaan") and heading)
        compose.onNodeWithText("1 dari 2 menunggu").assertExists()
        viewModel.requestOpen(20)
        waitFor(hasText("Yang dicatat tracker"))
        compose.onNodeWithContentDescription("Kembali").performClick()
        waitFor(hasText(HubStatus.CALM_TITLE) and heading)
    }

    @Test fun theEveningReminderOpensTheQuestionSheet() {
        val viewModel = shell()
        viewModel.requestQuestions()
        waitFor(hasText("Pertanyaan") and heading)
        compose.onNodeWithText("Nanti malam · 20.00").assertDoesNotExist()
        compose.onNodeWithText("Perlu sekarang").assertExists()
    }

    @Test fun rincianFromRiwayatGoesBackToRiwayat() {
        shell()
        openTile("Riwayat", "Riwayat tangkapan")
        waitFor(hasContentDescription("Kopi Contoh", substring = true))
        compose.onNodeWithContentDescription("Kopi Contoh", substring = true).performClick()
        waitFor(hasText("Yang dicatat tracker"))
        compose.onNodeWithContentDescription("Kembali").performClick()
        waitFor(hasText("Riwayat tangkapan") and heading)
    }

    @Test fun deletingFromRiwayatCanBeUndone() {
        shell()
        openTile("Riwayat", "Riwayat tangkapan")
        waitFor(hasContentDescription("Kopi Contoh", substring = true))
        val action = compose.onNodeWithContentDescription("Kopi Contoh", substring = true).fetchSemanticsNode()
            .config[SemanticsActions.CustomActions].single { it.label == DELETE_ACTION_LABEL }
        compose.runOnIdle { action.action() }
        waitFor(hasText("Dihapus dari aplikasi"))
        compose.waitUntil(5_000) { runBlocking { db.notificationDao().getById(20)!!.processingStatus } == "dismissed" }
        waitGone(hasContentDescription("Kopi Contoh", substring = true))
        compose.onNodeWithText("Urungkan").performClick()
        compose.waitUntil(5_000) { runBlocking { db.notificationDao().getById(20)!!.processingStatus } == "recorded" }
        waitFor(hasContentDescription("Kopi Contoh", substring = true))
    }

    @Test fun aRowSwipedAwayAndRestoredStaysRestored() {
        shell()
        openTile("Riwayat", "Riwayat tangkapan")
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

    @Test fun deletingFromRincianClosesItAndOffersUndo() {
        val viewModel = shell()
        viewModel.requestOpen(UiFixtures.review().id)
        waitFor(hasText("Rincian") and heading)
        compose.onNodeWithContentDescription("Hapus dari aplikasi").performClick()
        compose.onNodeWithText("Hapus").performClick()
        waitFor(hasText("Dihapus dari aplikasi"))
        waitFor(hasText(HubStatus.CALM_TITLE) and heading)
        compose.onNodeWithText("Urungkan").performClick()
        compose.waitUntil(5_000) { runBlocking { db.notificationDao().getById(UiFixtures.review().id)!!.processingStatus } == "needs_review" }
    }

    @Test fun catatOpensTheSheetAndATypedCaptureWaitsInTheQueueWithoutATracker() {
        shell()
        compose.onNodeWithContentDescription("Catat transaksi").performScrollTo().assertHeightIsAtLeast(60.dp).performClick()
        waitFor(hasText("Proses dengan AI"))
        compose.onNodeWithText("Catat cepat").assertExists()
        compose.onNodeWithTag(CAPTURE_INPUT_TAG).performTextInput("kopi contoh 25rb")
        compose.onNodeWithText("Proses dengan AI").performClick()
        waitFor(hasText("Disimpan di antrean"))
        assertEquals(TextCaptures.NOT_PAIRED, runBlocking { db.textCaptureDao().observeVisible().first().single().errorCode })
        compose.onNodeWithText("Lihat antrean").performScrollTo().performClick()
        waitFor(hasContentDescription("kopi contoh 25rb, Nominal belum jelas, Di antrean", substring = true))
        compose.onNodeWithContentDescription("Di ponsel, 1").assert(SemanticsMatcher.expectValue(SemanticsProperties.Selected, true))
    }

    @Test fun closingCatatReturnsToTheHub() {
        shell()
        compose.onNodeWithContentDescription("Catat transaksi").performScrollTo().performClick()
        waitFor(hasText("Proses dengan AI"))
        compose.onNodeWithContentDescription("Tutup catat cepat").performClick()
        waitGone(hasText("Proses dengan AI"))
        compose.onNode(hasText(HubStatus.CALM_TITLE) and heading).assertExists()
    }

    @Test fun themeChoiceIsStoredAndReadBack() {
        assertNull(AppPreferences(app).themeMode)
        assertEquals(ThemeMode.SYSTEM, viewModel().themeMode.value)
        val viewModel = shell()
        openTile("Pengaturan", "Pengaturan")
        waitFor(hasText("Gelap"))
        compose.onNodeWithText("Gelap").performScrollTo().performClick()
        compose.runOnIdle { assertEquals(ThemeMode.DARK, viewModel.themeMode.value) }
        assertEquals("dark", AppPreferences(app).themeMode)
        assertEquals(ThemeMode.DARK, viewModel().themeMode.value)
    }
}
