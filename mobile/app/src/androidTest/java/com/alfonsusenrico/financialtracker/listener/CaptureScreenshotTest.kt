package com.alfonsusenrico.financialtracker.listener

import android.graphics.Bitmap
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.setValue
import androidx.compose.ui.test.junit4.createComposeRule
import androidx.test.platform.app.InstrumentationRegistry
import com.alfonsusenrico.financialtracker.listener.data.model.TextCaptureEntity
import com.alfonsusenrico.financialtracker.listener.data.network.model.CaptureMovement
import com.alfonsusenrico.financialtracker.listener.data.network.model.CaptureOption
import com.alfonsusenrico.financialtracker.listener.data.network.model.CaptureQuestion
import com.alfonsusenrico.financialtracker.listener.data.network.model.CaptureSuggestion
import com.alfonsusenrico.financialtracker.listener.data.network.model.CaptureSummary
import com.alfonsusenrico.financialtracker.listener.data.network.model.CaptureTransaction
import com.alfonsusenrico.financialtracker.listener.data.network.model.CaptureView
import com.alfonsusenrico.financialtracker.listener.data.textcapture.TextCaptures
import com.alfonsusenrico.financialtracker.listener.ui.capture.CaptureSheetActions
import com.alfonsusenrico.financialtracker.listener.ui.capture.CaptureSheetContent
import com.alfonsusenrico.financialtracker.listener.ui.capture.CaptureSheetFrame
import com.alfonsusenrico.financialtracker.listener.ui.capture.CaptureSheetState
import com.alfonsusenrico.financialtracker.listener.ui.capture.SuggestionState
import com.alfonsusenrico.financialtracker.listener.ui.capture.sheetPhase
import com.alfonsusenrico.financialtracker.listener.ui.model.CapturePresentation
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionTheme
import com.google.gson.Gson
import org.junit.Assume.assumeTrue
import org.junit.Rule
import org.junit.Test
import java.io.File

/**
 * Screenshots of every capture sheet state with invented captures, in the theme given by
 * `-e deviceScreenshots light|dark` and at the phone's current font scale. App content only, no network.
 */
class CaptureScreenshotTest {
    @get:Rule val compose = createComposeRule()
    private val instrumentation = InstrumentationRegistry.getInstrumentation()
    private val context = instrumentation.targetContext
    private val gson = Gson()

    @Test fun capturesEveryState() {
        val theme = InstrumentationRegistry.getArguments().getString("deviceScreenshots")
        assumeTrue(theme == "light" || theme == "dark")
        check(context.packageName.endsWith(".verification"))
        val folder = File(context.getExternalFilesDir(null), "screens/capture-$theme-font${"%.1f".format(context.resources.configuration.fontScale)}")
            .apply { mkdirs() }
        val now = System.currentTimeMillis()
        val none = CaptureSheetActions({}, {}, { _, _ -> }, {}, {}, { _, _, _ -> }, { _, _ -> }, {}, {}, { _, _ -> }, {}, {}, {})
        var state by mutableStateOf(state(null, now))
        compose.setContent { CompanionTheme(dark = theme == "dark") { CaptureSheetFrame({}) { CaptureSheetContent(state, none) } } }
        val states = listOf(
            "01-input" to state(null, now),
            "02-input-no-history" to state(null, now, SuggestionState.Loaded(emptyList())),
            "03-processing" to state(capture("queued", "kopi contoh 32rb"), now, sending = true),
            "04-recorded" to state(recorded(now + 7_000), now),
            "05-saved" to state(recorded(now - 1), now),
            "06-move" to state(move(now + 5_000), now),
            "07-ask-account" to state(asking(accountQuestion()), now),
            "08-ask-amount" to state(asking(CaptureQuestion(uuid(602), "amount", prompt = "Berapa nominalnya?", options = emptyList(), allowsText = true), null), now),
            "09-queued" to state(capture("queued", "kopi contoh 32rb", error = TextCaptures.NETWORK), now),
            "10-error" to state(capture("queued", "kopi contoh 32rb", error = TextCaptures.SERVER), now),
            "11-refused" to state(capture("failed", "besok makan 20rb", error = "invalid_capture_date"), now),
            "12-undone" to state(capture("undone", "kopi contoh 32rb"), now)
        )
        for ((name, next) in states) {
            compose.runOnIdle { state = next }
            compose.waitForIdle()
            Thread.sleep(400)
            val screen = instrumentation.uiAutomation.takeScreenshot() ?: error("No screenshot")
            val id = context.resources.getIdentifier("status_bar_height", "dimen", "android")
            val top = if (id > 0) context.resources.getDimensionPixelSize(id) else 0
            val cropped = Bitmap.createBitmap(screen, 0, top, screen.width, screen.height - top)
            File(folder, "$name.png").outputStream().use { cropped.compress(Bitmap.CompressFormat.PNG, 100, it) }
        }
    }

    private fun state(capture: TextCaptureEntity?, now: Long, suggestions: SuggestionState = SuggestionState.Loaded(SUGGESTIONS), sending: Boolean = false) =
        CaptureSheetState(
            capture = capture?.let(CapturePresentation::of), phase = sheetPhase(capture, sending, now), suggestions = suggestions,
            now = now, canEdit = capture?.state == "recorded" && capture.text.startsWith("bayar")
        )

    private fun uuid(n: Int) = "00000000-0000-4000-8000-%012d".format(n)

    private fun capture(state: String, text: String, view: CaptureView? = null, question: CaptureQuestion? = null, error: String? = null, deadline: Long? = null) =
        TextCaptureEntity(
            id = 1, clientCaptureId = uuid(1), text = text, deviceTime = "2026-10-30T09:15:00+07:00", createdAt = System.currentTimeMillis(),
            state = state, serverId = view?.id, pairingContext = "fictional", questionJson = question?.let(gson::toJson),
            summaryJson = view?.let(gson::toJson), transactionId = view?.transaction?.id, undoDeadline = deadline, attempts = 1, errorCode = error
        )

    private fun recorded(deadline: Long) = capture(
        "recorded", "bayar parkir contoh 10rb",
        CaptureView(
            id = uuid(201), status = "recorded", summary = CaptureSummary("expense", 10_000, "2026-10-30", "Parkir Contoh"),
            transaction = CaptureTransaction(uuid(301), "expense", 10_000, "2026-10-30T09:15:00+07:00", accountName = "Tunai",
                merchant = "Parkir Contoh", notes = "Parkir Contoh", categoryName = "Parkir", kakeibo = "need")
        ), deadline = deadline
    )

    private fun move(deadline: Long) = capture(
        "recorded", "tarik tunai 200rb",
        CaptureView(
            id = uuid(202), status = "recorded", summary = CaptureSummary("movement", 200_000, "2026-10-30", "Tarik tunai"),
            transaction = CaptureTransaction(uuid(302), "expense", 200_000, "2026-10-30T09:15:00+07:00", accountName = "Bank Contoh", notes = "Tarik tunai"),
            movement = CaptureMovement(uuid(402), CaptureTransaction(uuid(302), accountName = "Bank Contoh"), CaptureTransaction(uuid(303), accountName = "Tunai"))
        ), deadline = deadline
    )

    private fun accountQuestion() = CaptureQuestion(
        uuid(601), "account", "source", prompt = "Dari rekening mana?",
        options = listOf(CaptureOption(uuid(701), "Tunai"), CaptureOption(uuid(702), "Dompet Contoh"), CaptureOption(uuid(703), "Bank Contoh")),
        allowsOther = true
    )

    private fun asking(question: CaptureQuestion, amount: Long? = 45_000) = capture(
        "needs_detail", "makan siang contoh 45rb",
        CaptureView(id = uuid(203), status = "needs_detail", summary = CaptureSummary("expense", amount, "2026-10-30", "Makan siang contoh")),
        question
    )

    private companion object {
        val SUGGESTIONS = listOf(
            CaptureSuggestion("parkir contoh 10rb", "Parkir Contoh", 10_000, "Parkir", "Tunai", "biasanya jam segini"),
            CaptureSuggestion("kopi contoh 32rb", "Kopi Contoh", 32_000, "Makan & Minum", "Dompet Contoh", "sering")
        )
    }
}
