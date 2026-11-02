package com.alfonsusenrico.financialtracker.listener

import android.app.Application
import androidx.compose.ui.test.SemanticsMatcher
import androidx.compose.ui.test.assertIsFocused
import androidx.compose.ui.test.hasContentDescription
import androidx.compose.ui.test.hasText
import androidx.compose.ui.test.junit4.createComposeRule
import androidx.compose.ui.test.onAllNodesWithText
import androidx.compose.ui.test.onLast
import androidx.compose.ui.test.onNodeWithContentDescription
import androidx.compose.ui.test.onNodeWithTag
import androidx.compose.ui.test.onNodeWithText
import androidx.compose.ui.test.performClick
import androidx.compose.ui.test.performScrollTo
import androidx.compose.ui.test.performTextInput
import androidx.test.platform.app.InstrumentationRegistry
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.model.TextCaptureState
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.ui.capture.CAPTURE_INPUT_TAG
import com.alfonsusenrico.financialtracker.listener.ui.capture.CaptureViewModel
import com.alfonsusenrico.financialtracker.listener.ui.capture.SUGGESTIONS_HEADING
import com.alfonsusenrico.financialtracker.listener.ui.shell.CompanionApp
import com.alfonsusenrico.financialtracker.listener.ui.shell.PhoneState
import com.alfonsusenrico.financialtracker.listener.ui.shell.CompanionViewModel
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionTheme
import kotlinx.coroutines.runBlocking
import okhttp3.OkHttpClient
import okhttp3.Request
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assume.assumeTrue
import org.junit.Before
import org.junit.Rule
import org.junit.Test

/**
 * Catat on the phone against the synthetic backend of `scripts/verify/capture_backend.py` through
 * `adb reverse tcp:18093 tcp:18093`. Runs only with `-e localBackend true` in the `.verification` app; every
 * input goes to the app's own composition.
 */
class CaptureBackendDeviceTest {
    @get:Rule val compose = createComposeRule()
    private val app = InstrumentationRegistry.getInstrumentation().targetContext.applicationContext as Application
    private val dao get() = AppDatabase.getDatabase(app).textCaptureDao()

    @Before fun setup() {
        assumeTrue(InstrumentationRegistry.getArguments().getString("localBackend") == "true")
        check(app.packageName.endsWith(".verification"))
        TestEndpoint.set(app, BACKEND)
        AppPreferences(app).apiKey = HISTORY_KEY
        AppPreferences(app).isAutoSyncEnabled = true
    }

    @After fun cleanup() {
        if (app.packageName.endsWith(".verification")) AppPreferences(app).apiKey = ""
    }

    private fun shell() {
        val captures = CaptureViewModel(app)
        compose.setContent { CompanionTheme(dark = false) { CompanionApp(CompanionViewModel(app), captures, PhoneState()) } }
        compose.onNodeWithContentDescription("Catat transaksi").performClick()
    }

    private fun waitFor(matcher: SemanticsMatcher, timeout: Long = 15_000) =
        compose.waitUntil(timeout) { compose.onAllNodes(matcher).fetchSemanticsNodes().isNotEmpty() }

    private fun type(text: String) {
        waitFor(hasText("Proses dengan AI"))
        compose.onNodeWithTag(CAPTURE_INPUT_TAG).performTextInput(text)
        compose.onNodeWithText("Proses dengan AI").performClick()
    }

    private fun transactionStatus(id: String): Int = OkHttpClient().newCall(
        Request.Builder().url("$BACKEND/api/transactions/$id").header("Authorization", "Bearer $HISTORY_KEY").build()
    ).execute().use { it.code }

    private fun count(): Int = AppDatabase.getDatabase(app).openHelper.readableDatabase
        .query("SELECT COUNT(*) FROM text_captures").use { it.moveToFirst(); it.getInt(0) }

    private fun latest() = runBlocking {
        val id = AppDatabase.getDatabase(app).openHelper.readableDatabase
            .query("SELECT MAX(id) FROM text_captures").use { it.moveToFirst(); it.getLong(0) }
        dao.getById(id)!!
    }

    @Test fun suggestionsFillTheInputWithoutSending() {
        shell()
        waitFor(hasText(SUGGESTIONS_HEADING))
        val before = count()
        compose.onNodeWithContentDescription("Isi dengan", substring = true).performClick()
        compose.waitForIdle()
        compose.onNodeWithTag(CAPTURE_INPUT_TAG).assertIsFocused()
        assertEquals(before, count())
    }

    @Test fun noHistoryShowsNoSuggestions() {
        AppPreferences(app).apiKey = EMPTY_KEY
        shell()
        waitFor(hasText("Proses dengan AI"))
        Thread.sleep(3_000)
        compose.waitForIdle()
        compose.onNodeWithText(SUGGESTIONS_HEADING).assertDoesNotExist()
    }

    @Test fun confidentCaptureIsRecordedAndCanBeUndone() {
        shell()
        type("parkir contoh 10rb")
        waitFor(hasText("Tercatat di tracker"))
        val recorded = latest()
        compose.onNodeWithContentDescription("Batalkan", substring = true).performClick()
        waitFor(hasText("Dibatalkan"))
        assertEquals(TextCaptureState.UNDONE, runBlocking { dao.getById(recorded.id)!!.state })
        assertEquals(404, transactionStatus(recorded.transactionId!!))
    }

    @Test fun unsureCaptureAsksAccountThenCategory() {
        shell()
        type("makan ${(1..8).map { ('a'..'z').random() }.joinToString("")} 45rb")
        waitFor(hasText("Dari rekening mana?"))
        compose.onNodeWithContentDescription("Jawab: Tunai").performScrollTo().performClick()
        waitFor(hasText("Kategorinya apa?"))
        compose.onAllNodes(hasContentDescription("Jawab: ", substring = true)).onLast().performScrollTo().performClick()
        waitFor(hasText("Tercatat di tracker"))
    }

    @Test fun afterTheWindowHapusDariTrackerDeletes() {
        shell()
        type("parkir contoh 10rb")
        waitFor(hasText("Tercatat di tracker"))
        val recorded = latest()
        waitFor(hasText("Tersimpan"), timeout = 20_000)
        compose.onNodeWithText("Buka rincian").performScrollTo().performClick()
        waitFor(hasText("Hapus dari tracker"))
        compose.onNodeWithText("Hapus dari tracker").performScrollTo().performClick()
        waitFor(hasText("Hapus dari tracker?"))
        compose.onAllNodesWithText("Hapus dari tracker").onLast().performClick()
        compose.waitUntil(15_000) { runBlocking { dao.getById(recorded.id)!!.state } == TextCaptureState.DELETED }
        assertEquals(404, transactionStatus(recorded.transactionId!!))
    }

    @Test fun offlineCaptureWaitsThenKirimSendsIt() {
        TestEndpoint.set(app, "http://127.0.0.1:1")
        shell()
        type("parkir contoh 10rb")
        waitFor(hasText("Disimpan di antrean"))
        TestEndpoint.set(app, BACKEND)
        compose.onNodeWithText("Kirim").performScrollTo().performClick()
        waitFor(hasText("Tercatat di tracker"))
    }

    private companion object {
        const val BACKEND = "http://127.0.0.1:18093"
        const val HISTORY_KEY = "fictional-capture-key"
        const val EMPTY_KEY = "fictional-empty-key"
    }
}
