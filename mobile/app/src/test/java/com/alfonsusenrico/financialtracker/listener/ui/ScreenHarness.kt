package com.alfonsusenrico.financialtracker.listener.ui

import androidx.compose.runtime.Composable
import androidx.compose.runtime.CompositionLocalProvider
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.test.junit4.ComposeContentTestRule
import androidx.compose.ui.unit.Density
import com.alfonsusenrico.financialtracker.listener.data.model.RawNotificationEntity
import com.alfonsusenrico.financialtracker.listener.ui.home.HomeState
import com.alfonsusenrico.financialtracker.listener.ui.model.CapturePresentation
import com.alfonsusenrico.financialtracker.listener.ui.model.HomeFigures
import com.alfonsusenrico.financialtracker.listener.ui.model.OpenQuestions
import com.alfonsusenrico.financialtracker.listener.ui.model.RowPresentation
import com.alfonsusenrico.financialtracker.listener.ui.model.SyncHealth
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

fun homeState(
    lists: CompanionLists = fixtureLists(), permission: Boolean = true, connected: Boolean = true,
    captures: List<CapturePresentation>? = emptyList()
): HomeState {
    val loaded = lists.takeIf { it.loaded }
    return HomeState(
        today = UiFixtures.today,
        figures = HomeFigures.of(loaded?.let { it.visible + it.local + it.ignored }, UiFixtures.today, captures),
        questionCount = loaded?.questions?.size,
        nextQuestion = lists.questions.firstOrNull()?.let(RowPresentation::of),
        health = SyncHealth(listenerConnected = connected, lastSyncAt = UiFixtures.at(UiFixtures.today, 10, 42), failedCaptures = 2, dropped = 0),
        needsCheck = HomeFigures.needsCheck(loaded?.visible, captures),
        onPhone = HomeFigures.onPhone(loaded?.local, captures),
        listenerPermissionGranted = permission
    )
}

fun List<RawNotificationEntity>.byId(id: Long) = first { it.id == id }
