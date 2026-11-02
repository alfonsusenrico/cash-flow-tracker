package com.alfonsusenrico.financialtracker.listener.ui

import androidx.compose.runtime.Composable
import androidx.compose.runtime.CompositionLocalProvider
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.test.junit4.ComposeContentTestRule
import androidx.compose.ui.unit.Density
import com.alfonsusenrico.financialtracker.listener.data.model.RawNotificationEntity
import com.alfonsusenrico.financialtracker.listener.data.prefs.ServerContact
import com.alfonsusenrico.financialtracker.listener.ui.hub.HubInputs
import com.alfonsusenrico.financialtracker.listener.ui.model.OpenQuestions
import com.alfonsusenrico.financialtracker.listener.ui.shell.CompanionLists
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionTheme

/** Shows a screen in the given theme and at the given font scale, as the app would. */
fun ComposeContentTestRule.show(dark: Boolean, fontScale: Float = 1f, content: @Composable () -> Unit) {
    setContent {
        val density = LocalDensity.current
        CompositionLocalProvider(LocalDensity provides Density(density.density, fontScale)) {
            CompanionTheme(dark = dark, content = content)
        }
    }
}

/** Every fixture row, split the way the DAO lists them. */
fun fixtureLists(): CompanionLists {
    val visible = listOf(
        UiFixtures.unsent(), UiFixtures.purpose(), UiFixtures.recorded(20, "Kopi Contoh", 32_000, hour = 10),
        UiFixtures.sender(), UiFixtures.pocket(), UiFixtures.movement(), UiFixtures.mapping(), UiFixtures.review(),
        UiFixtures.recorded(22, "Ojek Contoh", 24_000, date = UiFixtures.today.minusDays(1), category = "Transportasi")
    ).sortedByDescending { it.postTime }
    val needsAnswer = visible.filter { it.purposeQuestionId != null }
    return CompanionLists(
        visible = visible, ignored = listOf(UiFixtures.ignored()), local = listOf(UiFixtures.phone()),
        questions = OpenQuestions.open(visible, emptyList(), needsAnswer), loaded = true
    )
}

/** A calm, paired phone at 10.00 on the fixture day; tests change what they are about. */
fun hubInputs(
    capturedToday: Int? = 7,
    waiting: Int? = 0,
    now: Long = UiFixtures.at(UiFixtures.today, 10, 0)
) = HubInputs(
    paired = true, notificationAccess = true, listenerConnected = true,
    listenerDisconnectedAt = 0, listenerConnectedAt = UiFixtures.at(UiFixtures.today, 7, 2),
    serverContact = ServerContact.OK, online = true, autoSync = true, batteryUnrestricted = true, postNotifications = true,
    capturedToday = capturedToday, lastCaptureApp = "GoPay", lastCaptureAt = UiFixtures.at(UiFixtures.today, 9, 41),
    lastSyncAt = UiFixtures.at(UiFixtures.today, 9, 42), waiting = waiting, oldestWaitingAt = null,
    selfTestPassedAt = 0, oem = null, now = now, zone = UiFixtures.zone
)

fun List<RawNotificationEntity>.byId(id: Long) = first { it.id == id }
