package com.alfonsusenrico.financialtracker.listener.ui

import androidx.compose.ui.test.assertIsDisplayed
import androidx.compose.ui.test.junit4.createComposeRule
import androidx.compose.ui.test.onAllNodesWithText
import androidx.compose.ui.test.onNodeWithContentDescription
import androidx.compose.ui.test.onNodeWithText
import androidx.compose.ui.test.performScrollTo
import com.alfonsusenrico.financialtracker.listener.ui.activity.ActivityScreen
import com.alfonsusenrico.financialtracker.listener.ui.detail.DetailActions
import com.alfonsusenrico.financialtracker.listener.ui.detail.DetailScreen
import com.alfonsusenrico.financialtracker.listener.ui.model.ActivityFilter
import com.alfonsusenrico.financialtracker.listener.ui.shell.CompanionLists
import org.junit.Assert.assertEquals
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.ParameterizedRobolectricTestRunner
import org.robolectric.annotation.Config

/**
 * A row the tracker recorded from the owner's habits says so in Aktivitas, in TalkBack and in Rincian;
 * other rows do not. Every case runs in both themes at font scale 1.0 and 2.0.
 */
@RunWith(ParameterizedRobolectricTestRunner::class)
@Config(sdk = [34], application = com.alfonsusenrico.financialtracker.listener.TestApplication::class, qualifiers = "w360dp-h740dp")
class HabitLabelUiTest(private val dark: Boolean, private val fontScale: Float) {
    companion object {
        @JvmStatic @ParameterizedRobolectricTestRunner.Parameters(name = "dark={0}, fontScale={1}")
        fun cases() = listOf(false, true).flatMap { dark -> listOf(1f, 2f).map { arrayOf<Any>(dark, it) } }
    }

    @get:Rule val compose = createComposeRule()

    private val habit = UiFixtures.habit()
    private val ordinary = UiFixtures.recorded(20, "Kopi Contoh", 32_000, hour = 10)

    private fun activity() = compose.show(dark, fontScale) {
        ActivityScreen(CompanionLists(visible = listOf(habit, ordinary), loaded = true), ActivityFilter.ALL, totalCount = 2,
            hiddenDuplicates = 0, today = UiFixtures.today, onFilter = {}, onOpenRow = { _, _ -> }, onDelete = {}, onClearIgnored = {})
    }

    private fun detail(row: com.alfonsusenrico.financialtracker.listener.data.model.RawNotificationEntity) = compose.show(dark, fontScale) {
        DetailScreen(row, emptyList(), UiFixtures.PAIRING, emptyList(), emptyList(), busy = false, today = UiFixtures.today,
            actions = DetailActions({}, {}, {}, {}, {}, {}, {}, { _, _, _, _, _ -> }))
    }

    @Test fun activityTagsOnlyTheHabitRow() {
        activity()
        compose.onNodeWithText("Dari kebiasaan", useUnmergedTree = true).performScrollTo().assertIsDisplayed()
        assertEquals(1, compose.onAllNodesWithText("Dari kebiasaan", useUnmergedTree = true).fetchSemanticsNodes().size)
    }

    @Test fun talkBackReadsTheHabitWithTheRow() {
        activity()
        compose.onNodeWithContentDescription(
            "Warung Contoh, Keluar Rp27.000, Tercatat, Dari kebiasaan, GoPay Contoh, 12.10"
        ).performScrollTo().assertIsDisplayed()
        compose.onNodeWithContentDescription("Kopi Contoh, Keluar Rp32.000, Tercatat, GoPay Contoh, 10.00").assertExists()
    }

    @Test fun detailSaysWhichHabitRecordedIt() {
        detail(habit)
        compose.onNodeWithText("Dicatat dari kebiasaan: Makan & Minum · 3 dari 3 jawaban sebelumnya")
            .performScrollTo().assertIsDisplayed()
    }

    @Test fun detailOfAnOrdinaryRowHasNoHabitLine() {
        detail(ordinary)
        compose.onNodeWithText("Yang dicatat tracker").assertExists()
        assertEquals(0, compose.onAllNodesWithText("kebiasaan", substring = true, ignoreCase = true).fetchSemanticsNodes().size)
    }
}
