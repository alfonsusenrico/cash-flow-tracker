package com.alfonsusenrico.financialtracker.listener

import android.graphics.Bitmap
import androidx.compose.ui.test.junit4.createComposeRule
import androidx.test.platform.app.InstrumentationRegistry
import com.alfonsusenrico.financialtracker.listener.data.prefs.ServerContact
import com.alfonsusenrico.financialtracker.listener.data.prefs.StoredReport
import com.alfonsusenrico.financialtracker.listener.ui.hub.HubActions
import com.alfonsusenrico.financialtracker.listener.ui.hub.HubInputs
import com.alfonsusenrico.financialtracker.listener.ui.hub.HubScreen
import com.alfonsusenrico.financialtracker.listener.ui.hub.HubStatus
import com.alfonsusenrico.financialtracker.listener.ui.hub.HubUiState
import com.alfonsusenrico.financialtracker.listener.ui.hub.OemHints
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionTheme
import androidx.compose.runtime.mutableStateOf
import org.junit.Assume.assumeTrue
import org.junit.Rule
import org.junit.Test
import java.io.File
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZoneId

/**
 * Every hub state on the phone's own screen, from synthetic inputs: calm, each problem with its fix, soft
 * issues and "Laporan siap". Runs only with `-e deviceScreenshots light|dark` in the `.verification` app.
 */
class HubStatesScreenshotTest {
    @get:Rule val compose = createComposeRule()

    private val instrumentation = InstrumentationRegistry.getInstrumentation()
    private val context = instrumentation.targetContext
    private val zone = ZoneId.systemDefault()
    private fun at(hour: Int, minute: Int = 0) = LocalDate.of(2026, 11, 2).atTime(LocalTime.of(hour, minute)).atZone(zone).toInstant().toEpochMilli()

    private val calm = HubInputs(
        paired = true, notificationAccess = true, listenerConnected = true, listenerDisconnectedAt = 0, listenerConnectedAt = at(7, 2),
        serverContact = ServerContact.OK, online = true, autoSync = true, batteryUnrestricted = true, postNotifications = true,
        capturedToday = 7, lastCaptureApp = "GoPay", lastCaptureAt = at(12, 41), lastSyncAt = at(12, 42), waiting = 0,
        oldestWaitingAt = null, selfTestPassedAt = 0, oem = null, now = at(14, 5), zone = zone
    )

    @Test fun savesEveryHubState() {
        val theme = InstrumentationRegistry.getArguments().getString("deviceScreenshots")
        assumeTrue(theme == "light" || theme == "dark")
        check(context.packageName.endsWith(".verification"))
        val scale = context.resources.configuration.fontScale
        val folder = File(context.getExternalFilesDir(null), "screens/hub-$theme-font${"%.1f".format(scale)}").apply { mkdirs() }
        val states = listOf(
            "01-calm" to HubUiState(HubStatus.of(calm), questionCount = 0),
            "02-access-lost" to HubUiState(HubStatus.of(calm.copy(notificationAccess = false)), questionCount = 0),
            "03-frozen-hios" to HubUiState(HubStatus.of(calm.copy(listenerConnected = false, listenerDisconnectedAt = at(9, 40), oem = OemHints.of("TECNO"))), questionCount = 0),
            "04-server-unreachable" to HubUiState(HubStatus.of(calm.copy(serverContact = ServerContact.UNREACHABLE, waiting = 3)), questionCount = 0),
            "05-key-refused" to HubUiState(HubStatus.of(calm.copy(serverContact = ServerContact.REFUSED)), questionCount = 0),
            "06-offline-waiting" to HubUiState(HubStatus.of(calm.copy(online = false, waiting = 2)), questionCount = 0),
            "07-quiet-day" to HubUiState(HubStatus.of(calm.copy(capturedToday = 0)), questionCount = 0),
            "08-soft-issues" to HubUiState(HubStatus.of(calm.copy(batteryUnrestricted = false, postNotifications = false, autoSync = false)), questionCount = 0),
            "09-report-ready" to HubUiState(
                HubStatus.of(calm), questionCount = 0,
                report = StoredReport("Oktober 2026", "/laporan?basis=month&start=2026-10-01", "2026-10-01", opened = false)
            )
        )
        val shown = mutableStateOf(states.first().second)
        compose.setContent {
            CompanionTheme(dark = theme == "dark") { HubScreen(shown.value, HubActions({}, {}, {}, {}, {}, {}, {}, {})) }
        }
        states.forEach { (name, state) ->
            shown.value = state
            compose.waitForIdle()
            Thread.sleep(300)
            val screen = instrumentation.uiAutomation.takeScreenshot() ?: error("No screenshot")
            val id = context.resources.getIdentifier("status_bar_height", "dimen", "android")
            val top = if (id > 0) context.resources.getDimensionPixelSize(id) else 0
            val cropped = Bitmap.createBitmap(screen, 0, top, screen.width, screen.height - top)
            File(folder, "$name.png").outputStream().use { cropped.compress(Bitmap.CompressFormat.PNG, 100, it) }
        }
    }
}
