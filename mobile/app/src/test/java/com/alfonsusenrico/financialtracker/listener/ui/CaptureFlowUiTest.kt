package com.alfonsusenrico.financialtracker.listener.ui

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.ui.Modifier
import androidx.compose.ui.semantics.SemanticsProperties
import androidx.compose.ui.test.SemanticsMatcher
import androidx.compose.ui.test.assert
import androidx.compose.ui.test.assertIsDisplayed
import androidx.compose.ui.test.assertIsFocused
import androidx.compose.ui.test.hasContentDescription
import androidx.compose.ui.test.hasText
import androidx.compose.ui.test.junit4.createComposeRule
import androidx.compose.ui.test.onLast
import androidx.compose.ui.test.onNodeWithContentDescription
import androidx.compose.ui.test.onNodeWithTag
import androidx.compose.ui.test.onNodeWithText
import androidx.compose.ui.test.performClick
import androidx.compose.ui.test.performScrollTo
import androidx.compose.ui.test.performTextInput
import androidx.compose.ui.unit.dp
import androidx.test.ext.junit.runners.AndroidJUnit4
import com.alfonsusenrico.financialtracker.listener.data.model.TextCaptureEntity
import com.alfonsusenrico.financialtracker.listener.data.network.model.AccountItemResponse
import com.alfonsusenrico.financialtracker.listener.data.textcapture.CaptureAnswer
import com.alfonsusenrico.financialtracker.listener.ui.activity.ActivityScreen
import com.alfonsusenrico.financialtracker.listener.ui.capture.CAPTURE_INPUT_TAG
import com.alfonsusenrico.financialtracker.listener.ui.capture.CaptureDetailActions
import com.alfonsusenrico.financialtracker.listener.ui.capture.CaptureDetailScreen
import com.alfonsusenrico.financialtracker.listener.ui.capture.CaptureSheetActions
import com.alfonsusenrico.financialtracker.listener.ui.capture.CaptureSheetContent
import com.alfonsusenrico.financialtracker.listener.ui.capture.CaptureSheetState
import com.alfonsusenrico.financialtracker.listener.ui.capture.SuggestionState
import com.alfonsusenrico.financialtracker.listener.ui.capture.UNDO_BAR_TAG
import com.alfonsusenrico.financialtracker.listener.ui.capture.sheetPhase
import com.alfonsusenrico.financialtracker.listener.ui.detail.DetailActions
import com.alfonsusenrico.financialtracker.listener.ui.detail.DetailScreen
import com.alfonsusenrico.financialtracker.listener.ui.model.ActivityFilter
import com.alfonsusenrico.financialtracker.listener.ui.model.CapturePresentation
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.annotation.Config

/** What the owner's taps do in the capture sheet, a capture's Rincian and the feed; synthetic captures only. */
@RunWith(AndroidJUnit4::class)
@Config(sdk = [34], application = com.alfonsusenrico.financialtracker.listener.TestApplication::class, qualifiers = "w411dp-h1400dp")
class CaptureFlowUiTest {
    @get:Rule val compose = createComposeRule()

    private val now = CaptureFixtures.NOW
    private val submitted = mutableListOf<String>()
    private val answers = mutableListOf<Pair<Long, CaptureAnswer>>()
    private val undone = mutableListOf<Long>()

    private val actions = CaptureSheetActions(
        onClose = {}, onPrefilled = {}, onSubmit = { text, _ -> submitted += text }, onAnother = {}, onUndo = { undone += it },
        onEdit = { _, _, _ -> }, onAnswer = { id, answer -> answers += id to answer }, onResendAnswer = {}, onSend = {},
        onRewrite = { _, _ -> }, onOpenQueue = {}, onOpenDetail = {}, onInvalid = {}
    )

    private fun sheet(
        capture: TextCaptureEntity?, suggestions: SuggestionState = SuggestionState.Loaded(CaptureFixtures.suggestions),
        reducedMotion: Boolean = false, accounts: List<AccountItemResponse> = emptyList()
    ) {
        val state = CaptureSheetState(
            capture = capture?.let(CapturePresentation::of), phase = sheetPhase(capture, false, now), suggestions = suggestions,
            now = now, reducedMotion = reducedMotion, canEdit = true, accounts = accounts
        )
        compose.show(dark = false) {
            Column(Modifier.fillMaxSize().verticalScroll(rememberScrollState()), verticalArrangement = Arrangement.spacedBy(12.dp)) {
                CaptureSheetContent(state, actions)
            }
        }
    }

    @Test fun aSuggestionFillsAndFocusesTheInputButNeverSends() {
        sheet(null)
        compose.onNodeWithContentDescription("Isi dengan “parkir contoh 10rb”", substring = true).performClick()
        compose.onNodeWithTag(CAPTURE_INPUT_TAG).assert(SemanticsMatcher.expectValue(SemanticsProperties.EditableText, androidx.compose.ui.text.AnnotatedString("parkir contoh 10rb")))
        compose.onNodeWithTag(CAPTURE_INPUT_TAG).assertIsFocused()
        assertTrue(submitted.isEmpty())
        compose.onNodeWithText("Proses dengan AI").performClick()
        assertEquals(listOf("parkir contoh 10rb"), submitted)
    }

    @Test fun anEmptyInputIsExplainedAndNotSent() {
        sheet(null, suggestions = SuggestionState.Loaded(emptyList()))
        compose.onNodeWithText("Proses dengan AI").performClick()
        compose.onNodeWithText("Tulis transaksinya dulu", substring = true).assertIsDisplayed()
        compose.onNodeWithTag(CAPTURE_INPUT_TAG).performTextInput("ojek contoh 24rb")
        compose.onNodeWithText("Proses dengan AI").performClick()
        assertEquals(listOf("ojek contoh 24rb"), submitted)
    }

    @Test fun suggestionsThatCouldNotLoadSaySoWithoutExamples() {
        sheet(null, suggestions = SuggestionState.Unavailable)
        compose.onNodeWithText("Saran dari riwayat belum bisa dimuat.").assertIsDisplayed()
        compose.onNodeWithContentDescription("Isi dengan", substring = true).assertDoesNotExist()
    }

    @Test fun undoBarDrainsAndIsRemovedWhenAnimationsAreOff() {
        sheet(CaptureFixtures.recorded(undoDeadline = now + 5_000))
        compose.onNodeWithTag(UNDO_BAR_TAG, useUnmergedTree = true).assertExists()
        compose.onNodeWithContentDescription("Batalkan, sisa 5 detik").performClick()
        assertEquals(listOf(1L), undone)
    }

    @Test fun reducedMotionKeepsTheCountdownTextOnly() {
        sheet(CaptureFixtures.recorded(undoDeadline = now + 5_000), reducedMotion = true)
        compose.onNodeWithTag(UNDO_BAR_TAG, useUnmergedTree = true).assertDoesNotExist()
        compose.onNodeWithContentDescription("Batalkan, sisa 5 detik").assertIsDisplayed()
    }

    @Test fun chipsAnswerWithTheOfferedOptionAndOtherAccountsWithTheirId() {
        val other = AccountItemResponse(UiFixtures.uuid(7100), "Bank Contoh", type = "bank")
        val pocketParent = AccountItemResponse(UiFixtures.uuid(7200), "Bank Induk", type = "bank")
        val pocket = AccountItemResponse(UiFixtures.uuid(7201), "Kantong", type = "bank", parentId = UiFixtures.uuid(7200))
        val stock = AccountItemResponse(UiFixtures.uuid(7300), "Saham Contoh", type = "investment")
        sheet(CaptureFixtures.asking(), accounts = listOf(other, pocketParent, pocket, stock))
        compose.onNodeWithContentDescription("Jawab: Dompet Contoh").performClick()
        compose.onNodeWithText("Rekening lain").performClick()
        compose.onNodeWithContentDescription("Jawab: Saham Contoh").assertDoesNotExist()
        compose.onNodeWithContentDescription("Jawab: Bank Induk").assertDoesNotExist()
        compose.onNodeWithContentDescription("Jawab: Kantong").performScrollTo().performClick()
        val question = CaptureFixtures.accountQuestion().questionId!!
        assertEquals(
            listOf(3L to CaptureAnswer(question, optionId = UiFixtures.uuid(7002)), 3L to CaptureAnswer(question, accountId = UiFixtures.uuid(7201))),
            answers
        )
    }

    @Test fun amountIsAnsweredAsTypedText() {
        sheet(CaptureFixtures.asking(question = CaptureFixtures.amountQuestion(), amount = null))
        compose.onNodeWithText("Simpan").performClick()
        compose.onNodeWithText("Tulis nominalnya", substring = true).assertIsDisplayed()
        compose.onNode(hasText("Nominal") and SemanticsMatcher.keyIsDefined(SemanticsProperties.EditableText)).performTextInput("45rb")
        compose.onNodeWithText("Simpan").performClick()
        assertEquals(CaptureAnswer(CaptureFixtures.amountQuestion().questionId!!, text = "45rb"), answers.single().second)
    }

    @Test fun captureRincianOffersHapusDariTrackerBehindAConfirmation() {
        val deleted = mutableListOf<Long>()
        compose.show(dark = true) {
            CaptureDetailScreen(
                CapturePresentation.of(CaptureFixtures.recorded(undoDeadline = 0)), UiFixtures.PAIRING, emptyList(), emptyList(), false, null,
                CaptureDetailActions({}, {}, {}, {}, { deleted += it }, { _, _, _ -> }, {})
            )
        }
        // The hero and the typed-text card both name the source as the web does.
        assertEquals(2, compose.onAllNodes(hasText("· Diketik di ponsel", substring = true)).fetchSemanticsNodes().size)
        compose.onNodeWithText("Ubah transaksi").performScrollTo().assertIsDisplayed()
        compose.onNodeWithText("Hapus dari tracker").performScrollTo().performClick()
        compose.onNodeWithText("Hapus dari tracker?").assertIsDisplayed()
        compose.onNodeWithText("Batal").performClick()
        assertTrue(deleted.isEmpty())
        compose.onNodeWithText("Hapus dari tracker").performScrollTo().performClick()
        // The page's button and the dialog's confirmation; the dialog's is the later root.
        assertEquals(2, compose.onAllNodes(hasText("Hapus dari tracker")).fetchSemanticsNodes().size)
        compose.onAllNodes(hasText("Hapus dari tracker")).onLast().performClick()
        assertEquals(listOf(1L), deleted)
    }

    @Test fun capturedMoveHasNoTrackerDeleteFromThePhone() {
        compose.show(dark = false) {
            CaptureDetailScreen(
                CapturePresentation.of(CaptureFixtures.movement()), UiFixtures.PAIRING, emptyList(), emptyList(), false, null,
                CaptureDetailActions({}, {}, {}, {}, {}, { _, _, _ -> }, {})
            )
        }
        compose.onNodeWithText("Pindah saldo diubah dan dihapus di Tracker Web.").performScrollTo().assertIsDisplayed()
        compose.onNodeWithText("Hapus dari tracker").assertDoesNotExist()
    }

    @Test fun notificationRincianKeepsOnlyTheLocalDelete() {
        compose.show(dark = false) {
            DetailScreen(UiFixtures.recorded(20, "Kopi Contoh", 32_000), emptyList(), UiFixtures.PAIRING, emptyList(), emptyList(), false,
                UiFixtures.today, DetailActions({}, {}, {}, {}, {}, {}, {}, { _, _, _, _, _ -> }))
        }
        compose.onNodeWithText("Hapus dari aplikasi").performScrollTo().assertIsDisplayed()
        compose.onNodeWithText("Hapus dari tracker").assertDoesNotExist()
    }

    @Test fun feedListsCapturesAsTypedOnThePhone() {
        val captures = listOf(CaptureFixtures.recorded(), CaptureFixtures.movement(), CaptureFixtures.queued()).map(CapturePresentation::of)
        val opened = mutableListOf<Long>()
        compose.show(dark = false) {
            ActivityScreen(fixtureLists(), ActivityFilter.ALL, 9, 0, UiFixtures.today, {}, { _, _ -> }, {}, {}, captures) { opened += it.id }
        }
        compose.onNodeWithContentDescription("Parkir Contoh, Keluar Rp10.000, Tercatat, Diketik di ponsel · Tunai", substring = true)
            .performScrollTo().performClick()
        compose.onNodeWithContentDescription("Tarik tunai, Pindah Rp200.000, Pindah saldo, Diketik di ponsel · Bank Contoh → Tunai", substring = true)
            .performScrollTo().assertIsDisplayed()
        compose.onNodeWithContentDescription("kopi contoh 32rb, Nominal belum jelas, Di antrean", substring = true).performScrollTo().assertIsDisplayed()
        assertEquals(listOf(1L), opened)
    }

    @Test fun diPonselListsQueuedCapturesWithTheirOwnNote() {
        val captures = listOf(CaptureFixtures.recorded(), CaptureFixtures.queued()).map(CapturePresentation::of)
        compose.show(dark = false) {
            ActivityScreen(fixtureLists(), ActivityFilter.LOCAL, 9, 0, UiFixtures.today, {}, { _, _ -> }, {}, {}, captures) {}
        }
        compose.onNodeWithText("1 catatan ketik menunggu dikirim ke tracker").assertIsDisplayed()
        compose.onNodeWithContentDescription("Di ponsel, 2").assertExists()
        compose.onNode(hasContentDescription("kopi contoh 32rb", substring = true)).assertExists()
        compose.onNode(hasContentDescription("Parkir Contoh", substring = true)).assertDoesNotExist()
    }
}
