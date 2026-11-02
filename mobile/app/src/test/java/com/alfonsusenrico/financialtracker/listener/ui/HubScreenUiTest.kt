package com.alfonsusenrico.financialtracker.listener.ui

import androidx.compose.material3.Text
import androidx.compose.ui.semantics.SemanticsProperties
import androidx.compose.ui.test.SemanticsMatcher
import androidx.compose.ui.test.assertHeightIsAtLeast
import androidx.compose.ui.test.assertIsDisplayed
import androidx.compose.ui.test.hasText
import androidx.compose.ui.test.junit4.createComposeRule
import androidx.compose.ui.test.onNodeWithContentDescription
import androidx.compose.ui.test.onNodeWithText
import androidx.compose.ui.test.performClick
import androidx.compose.ui.test.performScrollTo
import androidx.compose.ui.unit.dp
import com.alfonsusenrico.financialtracker.listener.data.prefs.ServerContact
import com.alfonsusenrico.financialtracker.listener.data.prefs.StoredReport
import com.alfonsusenrico.financialtracker.listener.ui.hub.FixAction
import com.alfonsusenrico.financialtracker.listener.ui.hub.HubActions
import com.alfonsusenrico.financialtracker.listener.ui.hub.HubInputs
import com.alfonsusenrico.financialtracker.listener.ui.hub.HubScreen
import com.alfonsusenrico.financialtracker.listener.ui.hub.HubStatus
import com.alfonsusenrico.financialtracker.listener.ui.hub.HubUiState
import com.alfonsusenrico.financialtracker.listener.ui.hub.OemHints
import org.junit.Assert.assertEquals
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.ParameterizedRobolectricTestRunner
import org.robolectric.annotation.Config

/** The hub in every state, in both themes and at font scale 1 and 2: the status, each problem and its fix, and no money total. */
@RunWith(ParameterizedRobolectricTestRunner::class)
@Config(sdk = [34], application = com.alfonsusenrico.financialtracker.listener.TestApplication::class, qualifiers = "w360dp-h740dp")
class HubScreenUiTest(private val dark: Boolean, private val fontScale: Float) {
    companion object {
        @JvmStatic @ParameterizedRobolectricTestRunner.Parameters(name = "dark={0},font={1}")
        fun modes() = listOf(arrayOf<Any>(false, 1f), arrayOf<Any>(true, 1f), arrayOf<Any>(false, 2f), arrayOf<Any>(true, 2f))
    }

    @get:Rule val compose = createComposeRule()
    private val heading = SemanticsMatcher.keyIsDefined(SemanticsProperties.Heading)
    private val log = mutableListOf<Any>()
    private val actions = HubActions(
        onFix = { log += it }, onStatusDetails = { log += "status" }, onCatat = { log += "catat" }, onOpenReport = { log += "report:${it.start}" },
        onAllQuestions = { log += "questions" }, onLog = { log += "log" }, onSettings = { log += "settings" }, onWeb = { log += "web" }
    )

    private fun hub(input: HubInputs, questionCount: Int? = 0, report: StoredReport? = null, fixing: Boolean = false, top: Boolean = false) {
        compose.show(dark, fontScale) {
            HubScreen(
                HubUiState(HubStatus.of(input), fixing = fixing, report = report, questionCount = questionCount, lightCount = 1, reminderLabel = "20.00"),
                actions, topQuestion = { if (top) Text("Pertanyaan contoh") }
            )
        }
    }

    private fun fix(label: String) = compose.onNodeWithText(label).performScrollTo().assertHeightIsAtLeast(48.dp).performClick()

    @Test fun calmHubSaysItIsListeningWithCountsOnlyAndOpensEverything() {
        hub(hubInputs())
        compose.onNode(hasText("Lagi jagain notifikasimu") and heading).assertExists()
        compose.onNodeWithText("7 notifikasi ketangkep hari ini · terakhir GoPay, 09.41").assertExists()
        compose.onNodeWithText("Kekirim semua, 09.42").assertExists()
        compose.onNodeWithContentDescription("Catat transaksi").performScrollTo().assertHeightIsAtLeast(60.dp).performClick()
        compose.onNodeWithContentDescription("Buka rincian status").performScrollTo().assertHeightIsAtLeast(48.dp).performClick()
        listOf("Riwayat", "Pengaturan", "Buka web").forEach { fix(it) }
        compose.onNode(hasText("Nggak ada pertanyaan") and heading).performScrollTo().assertExists()
        assertEquals(listOf("catat", "status", "log", "settings", "web"), log)
    }

    @Test fun theMoneyTilesAreGone() {
        hub(hubInputs(), questionCount = null)
        listOf("Arus kas tracker", "Kategori teratas", "Uang keluar", "Neto", "Perbarui arus kas", "Aktivitas ponsel hari ini").forEach {
            compose.onNodeWithText(it, substring = true).assertDoesNotExist()
        }
        compose.onNodeWithText("Rp", substring = true).assertDoesNotExist()
        // Questions not yet read are not shown as "none".
        compose.onNodeWithText("Nggak ada pertanyaan").assertDoesNotExist()
    }

    @Test fun questionsAreCountedWithTheEveningShareAndTheTopOneShown() {
        hub(hubInputs(), questionCount = 3, top = true)
        compose.onNode(hasText("Ada 3 pertanyaan") and heading).performScrollTo().assertExists()
        compose.onNodeWithText("1 bisa nanti malam, 20.00").assertExists()
        compose.onNodeWithText("Pertanyaan contoh").performScrollTo().assertIsDisplayed()
        compose.onNodeWithContentDescription("Lihat semua pertanyaan, 3").performScrollTo().assertHeightIsAtLeast(48.dp).performClick()
        assertEquals(listOf("questions"), log)
    }

    @Test fun accessLostTakesOverTheCardWithItsFix() {
        hub(hubInputs().copy(notificationAccess = false))
        compose.onNode(hasText("Aku nggak bisa denger notifikasi") and heading).assertExists()
        compose.onNodeWithText("Akses notifikasinya mati", substring = true).assertExists()
        fix("Nyalain lagi")
        assertEquals(listOf(FixAction.OPEN_ACCESS), log)
    }

    @Test fun aFrozenListenerShowsTheHiosSteps() {
        hub(hubInputs().copy(listenerConnected = false, oem = OemHints.of("TECNO")))
        compose.onNode(hasText("HP-mu nidurin aku") and heading).assertExists()
        compose.onNodeWithText("Phone Master", substring = true).performScrollTo().assertIsDisplayed()
        fix("Atur baterai")
        assertEquals(listOf(FixAction.OPEN_BATTERY), log)
    }

    @Test fun aDisconnectedListenerReconnects() {
        hub(hubInputs().copy(listenerConnected = false))
        compose.onNode(hasText("Aku lagi putus dari notifikasi") and heading).assertExists()
        fix("Sambungin lagi")
        assertEquals(listOf(FixAction.RECONNECT_LISTENER), log)
    }

    @Test fun anUnreachableServerRetries() {
        hub(hubInputs(waiting = 3).copy(serverContact = ServerContact.UNREACHABLE))
        compose.onNode(hasText("Server lagi nggak kejangkau") and heading).assertExists()
        compose.onNodeWithText("Tenang, 3 yang nunggu aman di HP. Aku kirim begitu nyambung.").assertExists()
        fix("Coba lagi")
        assertEquals(listOf(FixAction.RETRY_SERVER), log)
    }

    @Test fun aRevokedKeyAsksToPairAgain() {
        hub(hubInputs().copy(serverContact = ServerContact.REFUSED))
        compose.onNode(hasText("Sambungan ke akunmu putus") and heading).assertExists()
        fix("Sambungin ulang")
        assertEquals(listOf(FixAction.PAIR), log)
    }

    @Test fun itemsWaitingOfflineCanBeSentNow() {
        hub(hubInputs(waiting = 2).copy(online = false))
        compose.onNode(hasText("Lagi offline") and heading).assertExists()
        compose.onNodeWithText("Offline, 2 nunggu dikirim").assertExists()
        fix("Kirim sekarang")
        assertEquals(listOf(FixAction.SEND_NOW), log)
    }

    @Test fun aQuietDayRunsTheSelfTestAndSaysItIsBusy() {
        hub(hubInputs(capturedToday = 0, now = UiFixtures.at(UiFixtures.today, 15, 0)))
        compose.onNode(hasText("Hari ini masih sepi") and heading).assertExists()
        // The fix is the strong button, so Catat steps back to a quieter one.
        compose.onNodeWithContentDescription("Catat transaksi").performScrollTo().assertIsDisplayed()
        fix("Tes sekarang")
        assertEquals(listOf(FixAction.RUN_SELF_TEST), log)
    }

    @Test fun aRunningFixSaysToWait() {
        hub(hubInputs(capturedToday = 0, now = UiFixtures.at(UiFixtures.today, 15, 0)), fixing = true)
        compose.onNodeWithText("Bentar…").performScrollTo().assertIsDisplayed()
        compose.onNodeWithText("Tes sekarang").assertDoesNotExist()
    }

    @Test fun softIssuesEachHaveTheirFix() {
        hub(hubInputs().copy(batteryUnrestricted = false, postNotifications = false))
        compose.onNode(hasText("Lagi jagain notifikasimu") and heading).assertExists()
        fix("Izinin")
        fix("Nyalain")
        assertEquals(listOf(FixAction.OPEN_BATTERY, FixAction.OPEN_NOTIFICATION_SETTINGS), log)
    }

    @Test fun aReadyReportOpensOnTheWebUntilOpened() {
        val report = StoredReport("Oktober 2026", "/laporan?basis=month&start=2026-10-01", "2026-10-01", opened = false)
        hub(hubInputs(), report = report)
        compose.onNode(hasText("Laporan Oktober 2026 udah siap") and heading).performScrollTo().assertExists()
        fix("Buka laporan di web")
        assertEquals(listOf("report:2026-10-01"), log)
    }

    @Test fun anOpenedReportStepsBack() {
        hub(hubInputs(), report = StoredReport("Oktober 2026", "/laporan", "2026-10-01", opened = true))
        compose.onNodeWithText("Laporan Oktober 2026 udah siap").assertDoesNotExist()
    }
}
