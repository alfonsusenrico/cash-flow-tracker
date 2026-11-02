package com.alfonsusenrico.financialtracker.listener

import androidx.compose.foundation.layout.Column
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.CompositionLocalProvider
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.semantics.SemanticsActions
import androidx.compose.ui.semantics.getOrNull
import androidx.compose.ui.test.SemanticsMatcher
import androidx.compose.ui.test.assertIsDisplayed
import androidx.compose.ui.test.assertIsNotEnabled
import androidx.compose.ui.test.junit4.createComposeRule
import androidx.compose.ui.test.onNodeWithText
import androidx.compose.ui.test.performClick
import androidx.compose.ui.test.performScrollTo
import androidx.compose.ui.test.performTouchInput
import androidx.compose.ui.test.swipeLeft
import androidx.compose.ui.test.swipeRight
import androidx.compose.ui.unit.Density
import androidx.test.platform.app.InstrumentationRegistry
import com.alfonsusenrico.financialtracker.listener.data.model.RawNotificationEntity
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.ui.activity.FeedItem
import com.alfonsusenrico.financialtracker.listener.ui.components.DELETE_ACTION_LABEL
import com.alfonsusenrico.financialtracker.listener.ui.components.StatusBadge
import com.alfonsusenrico.financialtracker.listener.ui.components.SwipeToDeleteItem
import com.alfonsusenrico.financialtracker.listener.ui.detail.DetailActions
import com.alfonsusenrico.financialtracker.listener.ui.detail.DetailScreen
import com.alfonsusenrico.financialtracker.listener.ui.model.RowPresentation
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionTheme
import org.junit.Assert.assertEquals
import org.junit.Rule
import org.junit.Test
import java.time.LocalDate

class NotificationStatusUiTest {
    @get:Rule val compose = createComposeRule()

    private val pairing get() = AppPreferences(InstrumentationRegistry.getInstrumentation().targetContext).pairingContext

    private fun row(synced: Boolean, status: String? = null, error: String? = null, recognised: Boolean = true) = RawNotificationEntity(
        deviceId = "fictional", packageName = "com.gojek.gopay", appLabel = "GoPay", postTime = 1000,
        payloadHash = "fictional-$status-$error-$synced", isSynced = synced, processingStatus = status,
        processingError = error, isFinancial = recognised
    )

    @Composable
    private fun Badge(row: RawNotificationEntity) = StatusBadge(RowPresentation.of(row).status)

    private fun detail(row: RawNotificationEntity, actions: DetailActions, fontScale: Float = 1f) {
        compose.setContent {
            val density = LocalDensity.current
            CompositionLocalProvider(LocalDensity provides Density(density.density, fontScale)) {
                CompanionTheme(dark = true) {
                    DetailScreen(row, emptyList(), pairing, emptyList(), emptyList(), false, LocalDate.of(2026, 10, 29), actions)
                }
            }
        }
    }

    private fun actions(
        onRetry: (Long) -> Unit = { error("Unexpected retry") },
        onSendLocal: (Long) -> Unit = { error("Unexpected send") },
        onResend: (Long) -> Unit = { error("Unexpected resend") },
        onDelete: (Long) -> Unit = { error("Unexpected delete") }
    ) = DetailActions(
        onBack = {}, onDelete = onDelete, onOpenQuestion = { error("No question") }, onRetry = onRetry,
        onSendLocal = onSendLocal, onResend = onResend, onInvalid = { error("Unexpected validation: $it") },
        onSubmit = { _, _, _, _, _ -> error("Unexpected submit") }
    )

    @Test fun acceptedQueuedStatusDoesNotClaimRecorded() {
        compose.setContent { CompanionTheme(dark = false) { Badge(row(synced = true, status = "queued")) } }
        compose.onNodeWithText("Diproses").assertIsDisplayed()
        compose.onNodeWithText("Tercatat").assertDoesNotExist()
    }

    @Test fun recordedReviewErrorAndLegacyStatesAreDistinctAtLargeText() {
        compose.setContent {
            val density = LocalDensity.current
            CompositionLocalProvider(LocalDensity provides Density(density.density, 2f)) {
                CompanionTheme(dark = true) {
                    Column {
                        Badge(row(true, "recorded"))
                        Badge(row(true, "needs_review"))
                        Badge(row(true, "failed"))
                        Badge(row(true, "recorded", "invalid_result"))
                        Badge(row(true))
                        Badge(row(false))
                    }
                }
            }
        }
        listOf("Tercatat", "Perlu cek", "Gagal", "Perlu diperiksa", "Terkirim", "Belum dikirim")
            .forEach { compose.onNodeWithText(it).assertIsDisplayed() }
    }

    @Test fun serverRetriesReviewAndUnrecognisedStatesAreDistinct() {
        compose.setContent {
            CompanionTheme(dark = false) {
                Column {
                    Badge(row(true, "failed", "provider_unavailable"))
                    Badge(row(true, "needs_review", "model_uncertain"))
                    Badge(row(false, recognised = false))
                }
            }
        }
        listOf("Diulang otomatis", "Perlu cek", "Tidak dikenali").forEach { compose.onNodeWithText(it).assertIsDisplayed() }
    }

    @Test fun reviewableEventOffersRetryAndManualRecord() {
        val row = RawNotificationEntity(
            deviceId = "fictional", packageName = "com.shopeepay.id", appLabel = "ShopeePay",
            postTime = 1000, payloadHash = "fictional-review", isSynced = true, processingStatus = "needs_review",
            processingError = "model_uncertain", backendEventId = "00000000-0000-4000-8000-000000000042",
            pairingContext = pairing, title = "Saldo ShopeePay diterima!",
            bodyText = "RAKA PURNAMA mengirimkan dana sebesar Rp500.000 ke ShopeePay-mu melalui BI-Fast."
        )
        var retried: Long? = null
        detail(row, actions(onRetry = { retried = it }))
        compose.onNodeWithText("Penafsir otomatis", substring = true).performScrollTo().assertIsDisplayed()
        compose.onNodeWithText("Coba Proses Ulang").performScrollTo().performClick()
        compose.runOnIdle { assert(retried == row.id) }
        compose.onNodeWithText("Catat manual").performScrollTo().performClick()
        compose.onNodeWithText("Catat ke Tracker").performScrollTo().assertIsDisplayed()
    }

    @Test fun ignoredNotificationCanBeResent() {
        val row = RawNotificationEntity(
            deviceId = "fictional", packageName = "com.jago.digitalbanking", appLabel = "Bank Jago",
            postTime = 1000, payloadHash = "fictional-ignored", isSynced = true, processingStatus = "ignored",
            title = "Jago", bodyText = "You've transferred Rp625.000 to RAKA PURNAMA."
        )
        var resent: Long? = null
        detail(row, actions(onResend = { resent = it }))
        compose.onNodeWithText("Kirim Ulang").performScrollTo().performClick()
        compose.runOnIdle { assert(resent == row.id) }
    }

    @Test fun keptNotificationCanBeSentToTracker() {
        val row = RawNotificationEntity(
            deviceId = "fictional", packageName = "com.gojek.gopay", appLabel = "GoPay",
            postTime = 1000, payloadHash = "fictional-kept", captureOutcome = "keep_local", captureReason = "promotional",
            title = "Promo", bodyText = "Diskon 50% GoFood hari ini, klaim vouchernya!"
        )
        var sent: Long? = null
        detail(row, actions(onSendLocal = { sent = it }))
        compose.onNodeWithText("Terlihat seperti promo", substring = true).performScrollTo().assertIsDisplayed()
        compose.onNodeWithText("Kirim ke Tracker").performScrollTo().performClick()
        compose.runOnIdle { assert(sent == row.id) }
    }

    @Test fun deleteFromPhoneRequiresConfirmation() {
        val row = RawNotificationEntity(
            deviceId = "fictional", packageName = "com.stockbit.android", appLabel = "Stockbit",
            postTime = 1000, payloadHash = "fictional-delete", isSynced = true, processingStatus = "needs_review",
            processingError = "unsupported_broker_event", title = "Deposit Berhasil",
            bodyText = "Dana kamu senilai Rp225,180 sudah dapat digunakan"
        )
        var deleted: Long? = null
        detail(row, actions(onRetry = {}, onDelete = { deleted = it }))
        compose.onNodeWithText("Hapus dari aplikasi").performScrollTo().performClick()
        compose.runOnIdle { assert(deleted == null) }
        compose.onNodeWithText("Hapus").performClick()
        compose.runOnIdle { assert(deleted == row.id) }
    }

    @Test fun movementCannotPatchOrdinaryTransactionAndActionRemainsVisibleAtLargeText() {
        val row = RawNotificationEntity(
            deviceId = "fictional", packageName = "com.jago.digitalbanking", appLabel = "Bank Jago",
            postTime = 1000, payloadHash = "fictional-ui", isSynced = true, processingStatus = "recorded",
            recordKey = "movement:fictional", serverTransactionId = "movement:fictional",
            title = "Fictional movement", bodyText = "Rp500.000 moved between pockets"
        )
        detail(row, actions(), fontScale = 2f)
        compose.onNodeWithText("Edit di Tracker Web").performScrollTo().assertIsDisplayed().assertIsNotEnabled()
    }

    @Test fun swipingLeftDeletesButSwipingRightDoesNot() {
        var deletions = 0
        compose.setContent { SwipeToDeleteItem(onDelete = { deletions++ }) { Text("Baris fiktif") } }
        compose.onNodeWithText("Baris fiktif").performTouchInput { swipeRight() }
        compose.waitForIdle()
        assertEquals(0, deletions)
        compose.onNodeWithText("Baris fiktif").performTouchInput { swipeLeft() }
        compose.waitForIdle()
        assertEquals(1, deletions)
    }

    @Test fun deleteIsOfferedAsAccessibilityAction() {
        var deletions = 0
        compose.setContent {
            CompanionTheme(dark = false) {
                FeedItem(RowPresentation.of(row(true, "recorded")), onClick = {}, onDelete = { deletions++ })
            }
        }
        val hasDeleteAction = SemanticsMatcher("has delete action") { node ->
            node.config.getOrNull(SemanticsActions.CustomActions).orEmpty().any { it.label == DELETE_ACTION_LABEL }
        }
        compose.onNode(hasDeleteAction).assertExists()
        val deleteAction = compose.onNode(hasDeleteAction).fetchSemanticsNode().config[SemanticsActions.CustomActions]
            .first { it.label == DELETE_ACTION_LABEL }
        compose.runOnUiThread { deleteAction.action() }
        assertEquals(1, deletions)
    }
}
