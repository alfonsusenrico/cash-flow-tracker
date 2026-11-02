package com.alfonsusenrico.financialtracker.listener

import android.app.Application
import androidx.compose.ui.test.assertIsEnabled
import androidx.compose.ui.test.junit4.createComposeRule
import androidx.compose.ui.test.onNodeWithText
import androidx.compose.ui.test.performClick
import androidx.compose.ui.test.performScrollTo
import androidx.test.ext.junit.runners.AndroidJUnit4
import com.alfonsusenrico.financialtracker.listener.data.model.RawNotificationEntity
import com.alfonsusenrico.financialtracker.listener.data.network.model.AccountItemResponse
import com.alfonsusenrico.financialtracker.listener.data.network.model.CategoryItemResponse
import com.alfonsusenrico.financialtracker.listener.ui.detail.DetailActions
import com.alfonsusenrico.financialtracker.listener.ui.detail.DetailScreen
import com.alfonsusenrico.financialtracker.listener.ui.detail.FormMode
import com.alfonsusenrico.financialtracker.listener.ui.detail.TransactionDraft
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionTheme
import org.junit.Assert.assertEquals
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.annotation.Config
import java.time.LocalDate

/** Sending an unsent notification offers only what the send carries to the tracker. */
@RunWith(AndroidJUnit4::class)
@Config(sdk = [34], application = com.alfonsusenrico.financialtracker.listener.TestApplication::class)
class DetailSendFormUiTest {
    @get:Rule val compose = createComposeRule()

    private val pairing = "fictional-pairing"
    private val accounts = listOf(AccountItemResponse(id = "00000000-0000-4000-8000-000000000501", name = "Contoh Bank"))
    private val categories = listOf(CategoryItemResponse(id = "00000000-0000-4000-8000-000000000502", name = "Makan", kind = "expense"))

    private fun show(row: RawNotificationEntity, onSubmit: (FormMode, TransactionDraft) -> Unit) {
        compose.setContent {
            CompanionTheme(dark = false) {
                DetailScreen(
                    row, emptyList(), pairing, accounts, categories, busy = false, today = LocalDate.of(2026, 10, 29),
                    actions = DetailActions(
                        onBack = {}, onDelete = { error("Row deleted") }, onOpenQuestion = { error("No question") },
                        onRetry = {}, onSendLocal = { error("Not a kept row") }, onResend = { error("Not ignored") },
                        onInvalid = { error("Unexpected validation: $it") },
                        onSubmit = { _, mode, _, draft, done -> onSubmit(mode, draft); done(true) }
                    )
                )
            }
        }
    }

    @Test fun unsentRowOffersNoAccountOrCategoryAndSendsItsLabels() {
        val row = RawNotificationEntity(
            id = 7, deviceId = "fictional", packageName = "com.gojek.gopay", appLabel = "GoPay",
            postTime = 1000, payloadHash = "fictional-unsent", title = "Pembayaran berhasil",
            bodyText = "Kamu baru aja bayar Rp45.000 ke Kedai Contoh pake GoPay",
            isFinancial = true, eventClass = "expense", expectedAmount = 45_000.0, expectedDirection = "out",
            expectedCounterparty = "Kedai Contoh", labelNotes = "Kedai Contoh"
        )
        val sent = mutableListOf<Pair<FormMode, TransactionDraft>>()
        show(row) { mode, draft -> sent.add(mode to draft) }

        compose.onNodeWithText("Periksa lalu kirim").performScrollTo().performClick()
        compose.onNodeWithText("Rekening").assertDoesNotExist()
        compose.onNodeWithText("Kategori").assertDoesNotExist()
        compose.onNodeWithText("Tracker memilih rekening dan kategori", substring = true).performScrollTo()
        compose.onNodeWithText("Kirim Notifikasi ke Tracker").performScrollTo().assertIsEnabled().performClick()

        compose.runOnIdle {
            assertEquals(listOf(FormMode.SEND_UNSENT to TransactionDraft(45_000L, "expense", null, null, "Kedai Contoh")), sent)
        }
    }

    @Test fun recordedRowStillOffersAccountAndCategory() {
        val row = RawNotificationEntity(
            id = 8, deviceId = "fictional", packageName = "com.gojek.gopay", appLabel = "GoPay",
            postTime = 1000, payloadHash = "fictional-review", isSynced = true, processingStatus = "needs_review",
            backendEventId = "00000000-0000-4000-8000-000000000503", pairingContext = pairing,
            title = "Pembayaran berhasil", bodyText = "Kamu baru aja bayar Rp45.000 ke Kedai Contoh pake GoPay"
        )
        show(row) { _, _ -> error("Synced row resubmitted") }

        compose.onNodeWithText("Catat manual").performScrollTo().performClick()
        compose.onNodeWithText("Rekening").performScrollTo()
        compose.onNodeWithText("Kategori").performScrollTo()
        compose.onNodeWithText("Catat ke Tracker").performScrollTo()
    }
}
