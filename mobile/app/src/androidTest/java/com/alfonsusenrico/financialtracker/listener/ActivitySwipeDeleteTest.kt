package com.alfonsusenrico.financialtracker.listener

import android.app.Application
import androidx.compose.ui.test.hasContentDescription
import androidx.compose.ui.test.hasText
import androidx.compose.ui.test.junit4.createComposeRule
import androidx.compose.ui.test.assertIsDisplayed
import androidx.compose.ui.test.onNodeWithContentDescription
import androidx.compose.ui.test.onNodeWithText
import androidx.compose.ui.test.performClick
import androidx.compose.ui.test.performTouchInput
import androidx.compose.ui.test.swipeLeft
import androidx.test.platform.app.InstrumentationRegistry
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.model.RawNotificationEntity
import com.alfonsusenrico.financialtracker.listener.ui.model.ActivityFilter
import com.alfonsusenrico.financialtracker.listener.ui.capture.CaptureViewModel
import com.alfonsusenrico.financialtracker.listener.ui.shell.CompanionApp
import com.alfonsusenrico.financialtracker.listener.ui.shell.CompanionViewModel
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionTheme
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Rule
import org.junit.Test

/** Runs the real Aktivitas tab against the app database; use -PcompanionVerification so owner data is never touched. */
class ActivitySwipeDeleteTest {
    @get:Rule val compose = createComposeRule()

    private val app = InstrumentationRegistry.getInstrumentation().targetContext.applicationContext as Application
    private val dao = AppDatabase.getDatabase(app).notificationDao()
    private val title = "Baris uji hapus ${System.nanoTime()}"
    private var rowId = 0L

    private fun fictionalRow() = RawNotificationEntity(
        deviceId = "fictional-device", packageName = "com.jago.digitalbanking", appLabel = "Jago",
        postTime = System.currentTimeMillis(), payloadHash = title, isFinancial = true,
        expectedAmount = 125000.0, eventClass = "expense", expectedCounterparty = title, title = title
    )

    @After fun cleanup() = runBlocking { dao.dismiss(rowId); Unit }

    private fun openActivity(viewModel: CompanionViewModel) {
        compose.setContent { CompanionTheme(dark = false) { CompanionApp(viewModel, CaptureViewModel(app), listenerPermissionGranted = true) } }
        compose.onNodeWithContentDescription("Aktivitas").performClick()
    }

    @Test fun swipeLeftDeletesRowAndUndoRestoresIt() {
        rowId = runBlocking { dao.insertOrIgnore(fictionalRow()) }
        openActivity(CompanionViewModel(app))
        compose.waitUntil(5_000) { compose.onAllNodes(hasContentDescription(title, substring = true)).fetchSemanticsNodes().isNotEmpty() }

        compose.onNodeWithContentDescription(title, substring = true).performTouchInput { swipeLeft() }
        compose.waitUntil(5_000) { compose.onAllNodes(hasText("Dihapus dari aplikasi")).fetchSemanticsNodes().isNotEmpty() }
        compose.waitUntil(5_000) { compose.onAllNodes(hasContentDescription(title, substring = true)).fetchSemanticsNodes().isEmpty() }
        assertEquals("dismissed", runBlocking { dao.getById(rowId)!!.processingStatus })

        compose.onNodeWithText("Urungkan").performClick()
        compose.waitUntil(5_000) { compose.onAllNodes(hasContentDescription(title, substring = true)).fetchSemanticsNodes().isNotEmpty() }
        assertEquals(null, runBlocking { dao.getById(rowId)!!.processingStatus })
    }

    @Test fun ignoredListOffersClearAllThatAsksFirst() {
        rowId = runBlocking { dao.insertOrIgnore(fictionalRow().copy(processingStatus = "ignored")) }
        val viewModel = CompanionViewModel(app).apply { setFilter(ActivityFilter.IGNORED) }
        openActivity(viewModel)
        compose.waitUntil(5_000) { compose.onAllNodes(hasContentDescription(title, substring = true)).fetchSemanticsNodes().isNotEmpty() }
        compose.onNodeWithText("Hapus Semua").assertDoesNotExist()

        compose.onNodeWithText("Hapus semua").assertIsDisplayed().performClick()
        compose.onNodeWithText("Hapus semua yang diabaikan?").assertIsDisplayed()
        compose.onNodeWithText("Batal").performClick()
        compose.waitUntil(5_000) { compose.onAllNodes(hasText("Hapus semua yang diabaikan?")).fetchSemanticsNodes().isEmpty() }
        assertEquals("ignored", runBlocking { dao.getById(rowId)!!.processingStatus })
    }
}
