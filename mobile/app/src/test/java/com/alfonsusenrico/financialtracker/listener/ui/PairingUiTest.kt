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
import com.alfonsusenrico.financialtracker.listener.TestApplication
import com.alfonsusenrico.financialtracker.listener.data.pairing.PairingOutcome
import com.alfonsusenrico.financialtracker.listener.ui.pairing.PairingActions
import com.alfonsusenrico.financialtracker.listener.ui.pairing.PairingContent
import com.alfonsusenrico.financialtracker.listener.ui.pairing.PairingScreen
import com.alfonsusenrico.financialtracker.listener.ui.pairing.PairingUiState
import com.alfonsusenrico.financialtracker.listener.ui.pairing.SetupPermissions
import com.alfonsusenrico.financialtracker.listener.ui.hub.OemHints
import com.alfonsusenrico.financialtracker.listener.data.health.SelfTestOutcome
import com.alfonsusenrico.financialtracker.listener.data.health.SelfTestProgress
import com.alfonsusenrico.financialtracker.listener.data.health.SelfTestStep
import com.alfonsusenrico.financialtracker.listener.data.health.StepState
import androidx.compose.ui.test.hasText
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionTheme
import org.junit.Assert.assertEquals
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
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
        { events += "verify" }, { events += "finish" }, { events += "notification" }, { events += "battery" }, { events += "app-info" },
        typed = { events += "typed:$it" }, askNotifications = { events += "ask" }, runSelfTest = { events += "test" },
        retryServer = { events += "retry" }, skipPost = { events += "skip" }, openWeb = { events += "web" }
    )
    private val heading = SemanticsMatcher.keyIsDefined(SemanticsProperties.Heading)

    private fun show(
        screen: PairingScreen, busy: Boolean = false, outcome: PairingOutcome? = null, setup: Boolean = true,
        permissions: SetupPermissions = SetupPermissions(), selfTest: SelfTestProgress = SelfTestProgress(),
        answered: Boolean? = null
    ) {
        compose.setContent {
            val density = LocalDensity.current.density
            CompositionLocalProvider(LocalDensity provides Density(density, scale)) {
                CompanionTheme(dark) {
                    PairingContent(
                        PairingUiState(screen, "Synthetic Phone", busy, outcome, replacing = !setup, setup = setup, notificationsAnswered = answered),
                        actions, permissions = permissions, selfTest = selfTest
                    )
                }
            }
        }
    }

    /** A body control scrolls into view; the sticky action bar is always on screen. */
    private fun body(text: String) = compose.onNodeWithText(text).performScrollTo().assertHeightIsAtLeast(48.dp)
    private fun bar(text: String) = compose.onNodeWithText(text).assertIsDisplayed().assertHeightIsAtLeast(48.dp)

    @Test fun welcomeSaysWhatTheHelperDoesAndStartsTheStory() {
        show(PairingScreen.WELCOME)
        compose.onNode(hasText("Halo! Aku pendamping Cash Flow Tracker di HP-mu") and heading).assertExists()
        compose.onNodeWithText("Uangmu tetap dibaca di web", substring = true).assertExists()
        bar("Mulai").performClick()
        assertEquals(listOf("CHOOSE"), events)
    }

    @Test fun pairingOffersQrTheTypedCodeAndTheKeyWithoutAServerField() {
        show(PairingScreen.CHOOSE)
        compose.onNode(hasText("Sambungin ke akunmu") and heading).assertExists()
        compose.onNodeWithContentDescription("Langkah 1 dari 5").assertExists()
        compose.onNodeWithText("Synthetic Phone").performScrollTo().assertIsDisplayed()
        bar("Scan QR").performClick()
        bar("Ketik kode aja").performClick()
        body("Pakai kunci API").performClick()
        assertEquals(listOf("scan", "CODE", "MANUAL"), events)
        compose.onNodeWithText("Base URL").assertDoesNotExist()
    }

    @Test fun theTypedCodeIsSentOnlyWhenTheOwnerActs() {
        show(PairingScreen.CODE)
        compose.onNode(hasSetTextAction()).performTextInput("7k4q-9mzx-b0")
        body("Sambungin").performClick()
        assertEquals(listOf("typed:7k4q-9mzx-b0"), events)
    }

    @Test fun confirmationDoesNotSubmitUntilOwnerActs() {
        show(PairingScreen.CONFIRM, setup = false)
        assertEquals(emptyList<String>(), events)
        bar("Hubungkan").performClick()
        assertEquals(listOf("confirm"), events)
    }

    @Test fun busyExchangeCannotBeSubmittedOrCancelledTwice() {
        show(PairingScreen.CONFIRM, busy = true)
        compose.onNodeWithText("Hubungkan").assertIsNotEnabled()
        compose.onNodeWithText("Batal").assertIsNotEnabled()
    }

    @Test fun manualKeyUsesPasswordSemanticsAndCanBePasted() {
        show(PairingScreen.MANUAL)
        compose.onNode(hasSetTextAction()).assert(SemanticsMatcher.keyIsDefined(SemanticsProperties.Password))
            .performTextInput("cfk_SYNTHETIC_KEY_1234567890")
        body("Tes Koneksi").performClick()
        assertEquals(listOf("manual"), events)
    }

    @Test fun failedCheckAfterExchangeOffersRetryWithoutLosingSavedKey() {
        show(PairingScreen.RESULT, outcome = PairingOutcome(false, "Kunci sudah tersimpan. Periksa internet.", true))
        compose.onNode(hasText("Hampir. Sambungannya belum bisa dicek") and heading).assertExists()
        bar("Tes Koneksi").performClick()
        compose.onNodeWithText("Pindai lagi").assertDoesNotExist()
        assertEquals(listOf("verify"), events)
    }

    @Test fun expiredQrShowsRescanAndBothFallbacks() {
        show(PairingScreen.RESULT, outcome = PairingOutcome(false, "Kode QR sudah kedaluwarsa atau dipakai."))
        bar("Pindai lagi").performClick()
        bar("Ketik kode aja")
        body("Masukkan API key manual").assertIsDisplayed()
        compose.onNodeWithText("Tes Koneksi").assertDoesNotExist()
    }

    @Test fun listenerStepCanSkipToBattery() {
        show(PairingScreen.LISTENER)
        compose.onNodeWithText("Chat, SMS, kode OTP", substring = true).assertExists()
        bar("Buka Akses Notifikasi").performClick()
        bar("Lewati dulu").performClick()
        assertEquals(listOf("notification", "BATTERY"), events)
    }

    @Test fun batteryStepShowsThisPhonesSaverAndCanSkip() {
        show(PairingScreen.BATTERY, permissions = SetupPermissions(oem = OemHints.of("TECNO")))
        bar("Kecualikan dari optimasi baterai")
        compose.onNodeWithText("Phone Master", substring = true).performScrollTo().assertIsDisplayed()
        bar("Lewati dulu").performClick()
        assertEquals(listOf("NOTIFICATIONS"), events)
    }

    @Test fun refusedNotificationsAreExplainedAndCanBeAskedAgain() {
        show(PairingScreen.NOTIFICATIONS, answered = false)
        compose.onNodeWithText("Pertanyaannya tetap ada di aplikasi", substring = true).performScrollTo().assertIsDisplayed()
        bar("Coba izinin lagi").performClick()
        bar("Lanjut").performClick()
        assertEquals(listOf("ask", "SELF_TEST"), events)
    }

    @Test fun selfTestShowsItsStepsAndEachFailureHasAFix() {
        show(PairingScreen.SELF_TEST, selfTest = SelfTestProgress(
            steps = mapOf(SelfTestStep.POST to StepState.DONE, SelfTestStep.CAPTURE to StepState.FAILED, SelfTestStep.SERVER to StepState.WAITING),
            outcome = SelfTestOutcome(false, SelfTestStep.CAPTURE)
        ))
        compose.onNode(hasText("Tes bentar, ya") and heading).assertExists()
        compose.onNodeWithText("Nangkep pakai pendengarku", substring = true).performScrollTo().assertIsDisplayed()
        compose.onNodeWithText("aku nggak bisa nangkep", substring = true).performScrollTo().assertIsDisplayed()
        bar("Nyalain akses").performClick()
        bar("Tes ulang").performClick()
        assertEquals(listOf("notification", "test"), events)
    }

    @Test fun aFailedNotificationStepCanBeSkippedAndAFailedServerRetried() {
        show(PairingScreen.SELF_TEST, selfTest = SelfTestProgress(outcome = SelfTestOutcome(false, SelfTestStep.POST)))
        bar("Izinin notifikasi").performClick()
        bar("Lewati bagian ini").performClick()
        assertEquals(listOf("ask", "skip"), events)
    }

    @Test fun serverFailureOffersRetry() {
        show(PairingScreen.SELF_TEST, selfTest = SelfTestProgress(outcome = SelfTestOutcome(false, SelfTestStep.SERVER)))
        compose.onNodeWithText("Aku nggak bisa ngabarin web", substring = true).performScrollTo().assertIsDisplayed()
        bar("Coba lagi").performClick()
        assertEquals(listOf("retry"), events)
    }

    @Test fun readySaysTheHelperRunsOnItsOwn() {
        show(PairingScreen.READY)
        compose.onNode(hasText("HP-mu siap") and heading).assertExists()
        compose.onNodeWithText("Saldo dan laporan dibaca di web.").performScrollTo().assertIsDisplayed()
        bar("Buka web").performClick()
        bar("Oke, sip").performClick()
        assertEquals(listOf("web", "finish"), events)
    }
}
