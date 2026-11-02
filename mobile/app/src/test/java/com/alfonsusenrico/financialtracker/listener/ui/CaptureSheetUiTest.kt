package com.alfonsusenrico.financialtracker.listener.ui

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.ui.Modifier
import androidx.compose.ui.test.assertHeightIsAtLeast
import androidx.compose.ui.test.assertIsDisplayed
import androidx.compose.ui.test.junit4.createComposeRule
import androidx.compose.ui.test.onNodeWithContentDescription
import androidx.compose.ui.test.onNodeWithText
import androidx.compose.ui.test.performScrollTo
import androidx.compose.ui.unit.dp
import com.alfonsusenrico.financialtracker.listener.data.textcapture.TextCaptures
import com.alfonsusenrico.financialtracker.listener.data.textcapture.CaptureAnswer
import com.alfonsusenrico.financialtracker.listener.data.network.model.CaptureOption
import com.alfonsusenrico.financialtracker.listener.data.network.model.CaptureQuestion
import com.google.gson.Gson
import androidx.compose.ui.test.hasSetTextAction
import androidx.compose.ui.test.performClick
import androidx.compose.ui.test.performTextInput
import org.junit.Assert.assertEquals
import com.alfonsusenrico.financialtracker.listener.ui.capture.CaptureSheetActions
import com.alfonsusenrico.financialtracker.listener.ui.capture.CaptureSheetContent
import com.alfonsusenrico.financialtracker.listener.ui.capture.CaptureSheetState
import com.alfonsusenrico.financialtracker.listener.ui.capture.SUGGESTIONS_HEADING
import com.alfonsusenrico.financialtracker.listener.ui.capture.SuggestionState
import com.alfonsusenrico.financialtracker.listener.ui.capture.sheetPhase
import com.alfonsusenrico.financialtracker.listener.ui.model.CapturePresentation
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.ParameterizedRobolectricTestRunner
import org.robolectric.annotation.Config

/** Every capture sheet state renders its copy and keeps its controls reachable in both themes and at font scale 2.0. */
@RunWith(ParameterizedRobolectricTestRunner::class)
@Config(sdk = [34], application = com.alfonsusenrico.financialtracker.listener.TestApplication::class, qualifiers = "w360dp-h740dp")
class CaptureSheetUiTest(private val dark: Boolean, private val fontScale: Float) {
    @get:Rule val compose = createComposeRule()

    companion object {
        @JvmStatic
        @ParameterizedRobolectricTestRunner.Parameters(name = "dark={0} font={1}")
        fun configurations() = listOf(arrayOf(false, 1f), arrayOf(true, 1f), arrayOf(false, 2f), arrayOf(true, 2f))
    }

    private val now = CaptureFixtures.NOW
    private val answers = mutableListOf<CaptureAnswer>()
    private val noActions = CaptureSheetActions(
        onClose = {}, onPrefilled = {}, onSubmit = { _, _ -> }, onAnother = {}, onUndo = {}, onEdit = { _, _, _ -> },
        onAnswer = { _, answer -> answers += answer }, onResendAnswer = {}, onSend = {}, onRewrite = { _, _ -> }, onOpenQueue = {}, onOpenDetail = {},
        onInvalid = {}
    )

    private fun sheet(
        capture: com.alfonsusenrico.financialtracker.listener.data.model.TextCaptureEntity?,
        at: Long = now,
        sending: Boolean = false,
        suggestions: SuggestionState = SuggestionState.Loaded(CaptureFixtures.suggestions),
        autoSync: Boolean = true,
        canEdit: Boolean = true
    ) {
        val state = CaptureSheetState(
            capture = capture?.let(CapturePresentation::of), phase = sheetPhase(capture, sending, at), suggestions = suggestions,
            now = at, autoSync = autoSync, canEdit = canEdit
        )
        compose.show(dark, fontScale) {
            Column(
                Modifier.fillMaxSize().verticalScroll(rememberScrollState()).padding(14.dp),
                verticalArrangement = Arrangement.spacedBy(12.dp)
            ) { CaptureSheetContent(state, noActions) }
        }
    }

    private fun reachable(text: String) = compose.onNodeWithText(text).performScrollTo().assertIsDisplayed().assertHeightIsAtLeast(48.dp)

    @Test fun inputWithSuggestions() {
        sheet(null)
        compose.onNodeWithText("Catat cepat").assertIsDisplayed()
        compose.onNodeWithContentDescription("Tutup catat cepat").assertHeightIsAtLeast(48.dp)
        compose.onNodeWithContentDescription("Tulis transaksi").assertExists()
        reachable("Proses dengan AI")
        compose.onNodeWithText(SUGGESTIONS_HEADING).performScrollTo().assertIsDisplayed()
        compose.onNodeWithContentDescription("Isi dengan “kopi contoh 32rb”", substring = true).performScrollTo()
            .assertIsDisplayed().assertHeightIsAtLeast(48.dp)
    }

    @Test fun inputWithoutHistoryShowsNoSuggestionsAndNoExamples() {
        sheet(null, suggestions = SuggestionState.Loaded(emptyList()))
        compose.onNodeWithText(SUGGESTIONS_HEADING).assertDoesNotExist()
        compose.onNodeWithContentDescription("Isi dengan", substring = true).assertDoesNotExist()
        reachable("Proses dengan AI")
    }

    @Test fun processing() {
        sheet(CaptureFixtures.queued(error = null, attempts = 0), sending = true)
        compose.onNodeWithText("AI memproses…").performScrollTo().assertIsDisplayed()
        compose.onNodeWithText("Mencocokkan rekening & kategori").performScrollTo().assertIsDisplayed()
        compose.onNodeWithText("“kopi contoh 32rb”").assertExists()
    }

    @Test fun recordedWithUndoCountdown() {
        sheet(CaptureFixtures.recorded(undoDeadline = now + 7_200))
        compose.onNodeWithText("Tercatat di tracker").performScrollTo().assertIsDisplayed()
        compose.onNodeWithContentDescription("Keluar Rp10.000").assertExists()
        compose.onNodeWithContentDescription("Batalkan, sisa 8 detik").performScrollTo().assertIsDisplayed().assertHeightIsAtLeast(48.dp)
        reachable("Ubah")
        reachable("Catat lagi")
        reachable("Selesai")
    }

    @Test fun savedAfterTheWindowPointsToRincian() {
        sheet(CaptureFixtures.recorded(undoDeadline = now + 10_000), at = now + 10_100)
        compose.onNodeWithContentDescription("Batalkan", substring = true).assertDoesNotExist()
        compose.onNodeWithText("Tersimpan").performScrollTo().assertIsDisplayed()
        compose.onNodeWithText("Untuk membatalkan sekarang, buka Rincian lalu Hapus dari tracker.").performScrollTo().assertIsDisplayed()
        reachable("Buka rincian")
    }

    @Test fun recordedMoveIsLabelledAsAMove() {
        sheet(CaptureFixtures.movement().copy(undoDeadline = now + 5_000), canEdit = false)
        compose.onNodeWithText("Pindah saldo tercatat").performScrollTo().assertIsDisplayed()
        compose.onNodeWithText("Bank Contoh → Tunai").performScrollTo().assertIsDisplayed()
        compose.onNodeWithText("Ubah").assertDoesNotExist()
    }

    @Test fun accountQuestionWithChipsAndOtherChoice() {
        sheet(CaptureFixtures.asking())
        compose.onNodeWithText("Dari rekening mana?").performScrollTo().assertIsDisplayed()
        compose.onNodeWithContentDescription("Belum diketahui: Rekening").assertExists()
        compose.onNodeWithContentDescription("Jawab: Dompet Contoh").performScrollTo().assertHeightIsAtLeast(48.dp)
        reachable("Rekening lain")
    }

    @Test fun amountQuestionWithTypedAnswer() {
        sheet(CaptureFixtures.asking(question = CaptureFixtures.amountQuestion(), amount = null))
        compose.onNodeWithText("Berapa nominalnya?").performScrollTo().assertIsDisplayed()
        compose.onNodeWithText("Nominal").performScrollTo().assertIsDisplayed()
        reachable("Simpan")
    }

    @Test fun categoryQuestionShowsPillars() {
        sheet(CaptureFixtures.asking(question = CaptureFixtures.categoryQuestion()))
        compose.onNodeWithContentDescription("Jawab: Jajan, Keinginan").performScrollTo().assertHeightIsAtLeast(48.dp)
        reachable("Kategori lain")
    }

    @Test fun offlineQueue() {
        sheet(CaptureFixtures.queued())
        compose.onNodeWithText("Disimpan di antrean").performScrollTo().assertIsDisplayed()
        compose.onNodeWithText("dikirim otomatis saat ponsel online", substring = true).assertExists()
        reachable("Kirim")
        reachable("Lihat antrean")
    }

    @Test fun offlineQueueWithAutoSyncOffWaitsForKirim() {
        sheet(CaptureFixtures.queued(), autoSync = false)
        compose.onNodeWithText("Kirim otomatis mati", substring = true).performScrollTo().assertIsDisplayed()
        reachable("Kirim")
    }

    @Test fun serverError() {
        sheet(CaptureFixtures.queued(error = TextCaptures.SERVER))
        compose.onNodeWithText("AI belum bisa memproses").performScrollTo().assertIsDisplayed()
        compose.onNodeWithText("Teks kamu masih tersimpan", substring = true).assertExists()
        reachable("Coba lagi")
        reachable("Tutup")
    }

    @Test fun refusedCapture() {
        sheet(CaptureFixtures.failed())
        compose.onNodeWithText("Tracker menolak catatan ini").performScrollTo().assertIsDisplayed()
        compose.onNodeWithText("Tanggal di teks tidak valid", substring = true).assertExists()
        reachable("Tulis ulang")
    }

    @Test fun undone() {
        sheet(CaptureFixtures.undone())
        compose.onNodeWithText("Dibatalkan").performScrollTo().assertIsDisplayed()
        reachable("Catat lagi")
    }

    private val timeQuestion = CaptureQuestion(
        questionId = "00000000-0000-4000-8000-000000006010", type = "time", prompt = "Jam berapa?", reason = "ambiguous",
        options = listOf(
            CaptureOption("t1", "Hari ini 14.00", date = "2026-10-29T14:00:00+07:00"),
            CaptureOption("t2", "Kemarin 14.00", date = "2026-10-28T14:00:00+07:00")
        ),
        allowsText = true
    )

    @Test fun timeQuestionOffersItsMomentsAndTheOwnersWords() {
        sheet(CaptureFixtures.asking(question = timeQuestion))
        compose.onNodeWithText("Jam berapa?").performScrollTo().assertIsDisplayed()
        compose.onNodeWithText("Kapan?").performScrollTo().assertIsDisplayed()
        compose.onNodeWithContentDescription("Jawab: Kemarin 14.00").performScrollTo().assertHeightIsAtLeast(48.dp).performClick()
        compose.onNode(hasSetTextAction()).performScrollTo().performTextInput("tadi jam 2 siang")
        compose.onNodeWithText("Simpan").performScrollTo().assertHeightIsAtLeast(48.dp).performClick()
        assertEquals(
            listOf(CaptureAnswer(timeQuestion.questionId!!, optionId = "t2"), CaptureAnswer(timeQuestion.questionId!!, text = "tadi jam 2 siang")),
            answers
        )
    }

    @Test fun recordedCaptureShowsWhenTheTrackerReadIt() {
        val recorded = CaptureFixtures.recorded()
        val view = TextCaptures.view(recorded)!!
        val withTime = recorded.copy(summaryJson = Gson().toJson(view.copy(summary = view.summary!!.copy(timeLabel = "Hari ini 14.00"))))
        sheet(withTime)
        compose.onNodeWithText("Hari ini 14.00").performScrollTo().assertIsDisplayed()
    }
}
