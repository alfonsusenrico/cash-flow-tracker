package com.alfonsusenrico.financialtracker.listener.ui

import androidx.compose.runtime.CompositionLocalProvider
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.semantics.SemanticsProperties
import androidx.compose.ui.test.SemanticsMatcher
import androidx.compose.ui.test.assert
import androidx.compose.ui.test.assertHeightIsAtLeast
import androidx.compose.ui.test.assertIsDisplayed
import androidx.compose.ui.test.assertIsNotEnabled
import androidx.compose.ui.test.hasSetTextAction
import androidx.compose.ui.test.junit4.createComposeRule
import androidx.compose.ui.test.onNodeWithContentDescription
import androidx.compose.ui.test.onNodeWithText
import androidx.compose.ui.test.performClick
import androidx.compose.ui.test.performScrollTo
import androidx.compose.ui.test.performTextInput
import androidx.compose.ui.unit.Density
import androidx.compose.ui.unit.dp
import androidx.test.ext.junit.runners.AndroidJUnit4
import com.alfonsusenrico.financialtracker.listener.TestApplication
import com.alfonsusenrico.financialtracker.listener.data.pairing.PairingOutcome
import com.alfonsusenrico.financialtracker.listener.ui.pairing.PairingActions
import com.alfonsusenrico.financialtracker.listener.ui.pairing.PairingContent
import com.alfonsusenrico.financialtracker.listener.ui.pairing.PairingScreen
import com.alfonsusenrico.financialtracker.listener.ui.pairing.PairingUiState
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionTheme
import org.junit.Assert.assertEquals
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.robolectric.ParameterizedRobolectricTestRunner
import org.robolectric.annotation.Config

@RunWith(ParameterizedRobolectricTestRunner::class)
@Config(sdk = [34], application = TestApplication::class, qualifiers = "w360dp-h740dp")
class PairingUiTest(private val dark: Boolean, private val scale: Float) {
    companion object {
        @JvmStatic @ParameterizedRobolectricTestRunner.Parameters(name = "dark={0},scale={1}")
        fun modes() = listOf(arrayOf(false, 1f), arrayOf(true, 1f), arrayOf(false, 2f), arrayOf(true, 2f))
    }
    @get:Rule val compose = createComposeRule()
    private val events = mutableListOf<String>()
    private val actions = PairingActions(
        { events += it.name }, { events += "scan" }, { events += "confirm" }, { events += "manual" },
        { events += "verify" }, { events += "finish" }, { events += "notification" }, { events += "battery" }, { events += "app-info" }
    )
    private fun show(screen: PairingScreen, busy: Boolean = false, outcome: PairingOutcome? = null) {
        compose.setContent {
            val density = LocalDensity.current.density
            CompositionLocalProvider(LocalDensity provides Density(density, scale)) {
                CompanionTheme(dark) { PairingContent(PairingUiState(screen, "Synthetic Phone", busy, outcome, replacing = true), actions) }
            }
        }
    }
    @Test fun qrAndManualActionsStayReachableWithoutServerInput() {
        show(PairingScreen.CHOOSE)
        compose.onNodeWithText("Hubungkan ke tracker").assert(SemanticsMatcher.keyIsDefined(SemanticsProperties.Heading))
        compose.onNodeWithText("Synthetic Phone").performScrollTo().assertIsDisplayed()
        compose.onNodeWithText("Pindai QR dari Pengaturan web").performScrollTo().assertHeightIsAtLeast(48.dp).performClick()
        compose.onNodeWithText("Masukkan API key manual").performScrollTo().performClick()
        assertEquals(listOf("scan", "MANUAL"), events)
        compose.onNodeWithText("Base URL").assertDoesNotExist()
        compose.onNode(hasSetTextAction()).assertDoesNotExist()
    }
    @Test fun confirmationDoesNotSubmitUntilOwnerActs() {
        show(PairingScreen.CONFIRM)
        assertEquals(emptyList<String>(), events)
        compose.onNodeWithText("Hubungkan").performScrollTo().assertHeightIsAtLeast(48.dp).performClick()
        assertEquals(listOf("confirm"), events)
    }
    @Test fun busyExchangeCannotBeSubmittedOrCancelledTwice() {
        show(PairingScreen.CONFIRM, busy = true)
        compose.onNodeWithText("Hubungkan").performScrollTo().assertIsNotEnabled()
        compose.onNodeWithText("Batal").performScrollTo().assertIsNotEnabled()
        compose.onNodeWithContentDescription("Kembali").performScrollTo().assertIsNotEnabled()
    }
    @Test fun manualKeyUsesPasswordSemanticsAndCanBePasted() {
        show(PairingScreen.MANUAL)
        compose.onNode(hasSetTextAction()).performScrollTo().assert(SemanticsMatcher.keyIsDefined(SemanticsProperties.Password))
            .performTextInput("synthetic-paste")
        compose.onNodeWithText("Tes Koneksi").performScrollTo().performClick()
        assertEquals(listOf("manual"), events)
    }
    @Test fun failedCheckAfterExchangeOffersRetryWithoutLosingSavedKey() {
        show(PairingScreen.RESULT, outcome = PairingOutcome(false, "Kunci sudah tersimpan. Periksa internet.", true))
        compose.onNodeWithText("Tes Koneksi").performScrollTo().assertHeightIsAtLeast(48.dp).performClick()
        compose.onNodeWithText("Pindai lagi").performScrollTo().assertHeightIsAtLeast(48.dp)
        assertEquals(listOf("verify"), events)
    }
    @Test fun expiredQrShowsRescanAndManualFallback() {
        show(PairingScreen.RESULT, outcome = PairingOutcome(false, "Kode QR sudah kedaluwarsa atau dipakai."))
        compose.onNodeWithText("Pindai lagi").performScrollTo().performClick()
        compose.onNodeWithText("Masukkan API key manual").performScrollTo().assertIsDisplayed()
        compose.onNodeWithText("Tes Koneksi").assertDoesNotExist()
        assertEquals(listOf("scan"), events)
    }
    @Test fun listenerStepCanSkipToBattery() {
        show(PairingScreen.LISTENER)
        compose.onNodeWithText("Buka Akses Notifikasi").performScrollTo().assertHeightIsAtLeast(48.dp)
        compose.onNodeWithText("Lewati dulu").performScrollTo().performClick()
        assertEquals(listOf("BATTERY"), events)
    }
    @Test fun batteryStepShowsHiosAndCanSkipToPairing() {
        show(PairingScreen.BATTERY)
        compose.onNodeWithText("Kecualikan dari optimasi baterai").performScrollTo().assertHeightIsAtLeast(48.dp)
        compose.onNodeWithText("Lewati dulu").performScrollTo().performClick()
        assertEquals(listOf("CHOOSE"), events)
    }
}
