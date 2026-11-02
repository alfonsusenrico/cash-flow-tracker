package com.alfonsusenrico.financialtracker.listener.ui

import com.alfonsusenrico.financialtracker.listener.data.model.TextCaptureState
import com.alfonsusenrico.financialtracker.listener.data.textcapture.CaptureAbilities
import com.alfonsusenrico.financialtracker.listener.data.textcapture.CaptureText
import com.alfonsusenrico.financialtracker.listener.data.textcapture.TextCaptures
import com.alfonsusenrico.financialtracker.listener.data.textcapture.UndoWindow
import com.alfonsusenrico.financialtracker.listener.ui.capture.SheetPhase
import com.alfonsusenrico.financialtracker.listener.ui.capture.sheetPhase
import com.alfonsusenrico.financialtracker.listener.ui.model.ActivityFilter
import com.alfonsusenrico.financialtracker.listener.ui.model.CapturePresentation
import com.alfonsusenrico.financialtracker.listener.ui.model.HomeFigures
import com.alfonsusenrico.financialtracker.listener.ui.model.MoneyDirection
import com.alfonsusenrico.financialtracker.listener.ui.model.StatusTone
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

/** Pure capture rules: text bounds, the undo window, the sheet's state machine, abilities and figures. */
class CaptureModelTest {
    private val now = CaptureFixtures.NOW

    @Test fun textFollowsTheBackendBounds() {
        assertEquals("parkir 10rb", CaptureText.normalize(" parkir\t\n 10rb "))
        assertEquals("kopi", CaptureText.normalize("\uFF4B\uFF4F\uFF50\uFF49"))
        assertNull(CaptureText.normalize(""))
        assertNull(CaptureText.normalize("a".repeat(201)))
        assertEquals(200, CaptureText.normalize("a".repeat(200))!!.length)
        assertNull(CaptureText.normalize("kopi​ 10rb"))
        assertNull(CaptureText.normalize("x".repeat(51), CaptureText.ANSWER_MAX_LENGTH))
    }

    @Test fun serverStatusesMapToLocalStates() {
        assertEquals(TextCaptureState.RECORDED, TextCaptureState.fromServer("recorded"))
        assertEquals(TextCaptureState.NEEDS_DETAIL, TextCaptureState.fromServer("needs_detail"))
        assertEquals(TextCaptureState.UNDONE, TextCaptureState.fromServer("undone"))
        assertNull(TextCaptureState.fromServer("deleted"))
        assertNull(TextCaptureState.fromServer(null))
    }

    @Test fun undoWindowIsTenSecondsOnThePhoneClock() {
        val recorded = CaptureFixtures.recorded(undoDeadline = now + 10_000)
        assertTrue(UndoWindow.isOpen(recorded, now + 9_900))
        assertFalse(UndoWindow.isOpen(recorded, now + 10_100))
        assertFalse(UndoWindow.isOpen(recorded, now + 10_000))
        assertEquals(10, UndoWindow.secondsLeft(recorded, now))
        assertEquals(1, UndoWindow.secondsLeft(recorded, now + 9_900))
        assertEquals(0, UndoWindow.secondsLeft(recorded, now + 10_100))
        assertFalse(UndoWindow.isOpen(recorded.copy(edited = true), now))
        assertFalse(UndoWindow.isOpen(CaptureFixtures.asking(), now))
    }

    @Test fun localDeadlineUsesTheServerClockAndNeverExceedsTheWindow() {
        val until = "2027-01-15T08:00:10Z"
        val serverNow = java.time.Instant.parse("2027-01-15T08:00:00Z").toEpochMilli()
        assertEquals(500L + 10_000, UndoWindow.localDeadline(until, serverNow, 500))
        assertEquals(500L + 4_000, UndoWindow.localDeadline(until, serverNow + 6_000, 500))
        assertEquals(500L, UndoWindow.localDeadline(until, serverNow + 60_000, 500))
        assertEquals(500L + 10_000, UndoWindow.localDeadline(until, serverNow - 60_000, 500))
        assertEquals(500L + 10_000, UndoWindow.localDeadline(until, null, 500))
        assertNull(UndoWindow.localDeadline(null, serverNow, 500))
        assertNull(UndoWindow.localDeadline("not a time", serverNow, 500))
    }

    @Test fun sheetFollowsEveryCaptureState() {
        assertEquals(SheetPhase.INPUT, sheetPhase(null, sending = false, now))
        assertEquals(SheetPhase.PROCESSING, sheetPhase(CaptureFixtures.queued(error = null, attempts = 0), sending = true, now))
        assertEquals(SheetPhase.PROCESSING, sheetPhase(CaptureFixtures.sending(), sending = false, now))
        assertEquals(SheetPhase.QUEUED, sheetPhase(CaptureFixtures.queued(), sending = false, now))
        assertEquals(SheetPhase.QUEUED, sheetPhase(CaptureFixtures.queued(error = TextCaptures.NOT_PAIRED), sending = false, now))
        assertEquals(SheetPhase.ERROR, sheetPhase(CaptureFixtures.queued(error = TextCaptures.SERVER), sending = false, now))
        assertEquals(SheetPhase.ERROR, sheetPhase(CaptureFixtures.queued(error = TextCaptures.AUTH), sending = false, now))
        assertEquals(SheetPhase.RECORDED, sheetPhase(CaptureFixtures.recorded(), sending = false, now + 9_900))
        assertEquals(SheetPhase.SAVED, sheetPhase(CaptureFixtures.recorded(), sending = false, now + 10_100))
        assertEquals(SheetPhase.QUESTION, sheetPhase(CaptureFixtures.asking(), sending = false, now))
        assertEquals(SheetPhase.UNDONE, sheetPhase(CaptureFixtures.undone(), sending = false, now))
        assertEquals(SheetPhase.FAILED, sheetPhase(CaptureFixtures.failed(), sending = false, now))
        assertEquals(SheetPhase.DELETED, sheetPhase(CaptureFixtures.recorded().copy(state = TextCaptureState.DELETED), sending = false, now))
    }

    @Test fun hapusDariTrackerIsOfferedOnlyForTheCapturesOwnLedgerRow() {
        fun allowed(capture: com.alfonsusenrico.financialtracker.listener.data.model.TextCaptureEntity, pairing: String = UiFixtures.PAIRING) =
            CaptureAbilities.canDeleteFromTracker(capture, TextCaptures.view(capture), pairing)
        assertTrue(allowed(CaptureFixtures.recorded()))
        assertTrue(allowed(CaptureFixtures.recorded(type = "income", category = "Gaji")))
        assertFalse("A move is deleted on the web", allowed(CaptureFixtures.movement()))
        assertFalse(allowed(CaptureFixtures.asking()))
        assertFalse(allowed(CaptureFixtures.queued()))
        assertFalse(allowed(CaptureFixtures.undone()))
        assertFalse(allowed(CaptureFixtures.recorded().copy(state = TextCaptureState.DELETED)))
        assertFalse("Another pairing's key cannot delete it", allowed(CaptureFixtures.recorded(), "other-pairing"))
    }

    @Test fun onlyCapturesTheTrackerNeverSawCanBeDiscarded() {
        assertTrue(CaptureAbilities.canDiscard(CaptureFixtures.queued(attempts = 0)))
        assertFalse(CaptureAbilities.canDiscard(CaptureFixtures.queued(attempts = 1)))
        assertTrue(CaptureAbilities.canDiscard(CaptureFixtures.failed()))
        assertFalse(CaptureAbilities.canDiscard(CaptureFixtures.recorded()))
    }

    @Test fun presentationLabelsCapturesAndMoves() {
        val recorded = CapturePresentation.of(CaptureFixtures.recorded())
        assertEquals("Parkir Contoh", recorded.title)
        assertEquals(MoneyDirection.OUT, recorded.direction)
        assertEquals("Tercatat", recorded.status.label)
        assertEquals("Tunai", recorded.account)
        assertEquals("Parkir", recorded.category)

        val move = CapturePresentation.of(CaptureFixtures.movement())
        assertEquals(MoneyDirection.MOVE, move.direction)
        assertEquals("Pindah saldo", move.status.label)
        assertEquals(StatusTone.MOVE, move.status.tone)
        assertEquals("Bank Contoh → Tunai", move.account)
        assertNull(move.category)

        val queued = CapturePresentation.of(CaptureFixtures.queued())
        assertNull("Unknown is never 0", queued.amount)
        assertEquals("kopi contoh 32rb", queued.title)
        assertEquals("Di antrean", queued.status.label)
        assertEquals("Perlu jawaban", CapturePresentation.of(CaptureFixtures.asking()).status.label)
    }

    @Test fun homeFiguresCountWaitingCapturesButNeverTheirMoney() {
        val today = CaptureFixtures.today
        val notifications = listOf(UiFixtures.recorded(20, "Kopi Contoh", 32_000))
        val captures = listOf(
            CaptureFixtures.recorded(), CaptureFixtures.movement(), CaptureFixtures.asking(), CaptureFixtures.queued(),
            CaptureFixtures.recorded(id = 9, amount = 20_000, date = today.minusDays(1), hour = 13)
        ).map(CapturePresentation::of)
        val figures = HomeFigures.of(notifications, today, captures)
        // Asking and queued wait; recorded captures and the move are settled and appear on Beranda nowhere.
        assertEquals(HomeFigures(capturedToday = 1, waitingToday = 0, waitingCapturesToday = 2), figures)
        // Captures never change the notification counts.
        assertEquals(HomeFigures(1, 0, 0), HomeFigures.of(notifications, today, emptyList()))
    }

    @Test fun filtersListCapturesWhereTheyBelong() {
        val captures = listOf(
            CaptureFixtures.recorded(), CaptureFixtures.asking(), CaptureFixtures.queued(), CaptureFixtures.sending(), CaptureFixtures.failed()
        ).map(CapturePresentation::of)
        assertEquals(5, ActivityFilter.ALL.selectCaptures(captures).size)
        assertEquals(listOf(3L, 4L, 5L, 6L), ActivityFilter.PENDING.selectCaptures(captures).map { it.id })
        assertEquals(listOf(4L, 5L, 6L), ActivityFilter.LOCAL.selectCaptures(captures).map { it.id })
        assertTrue(ActivityFilter.NEEDS_ANSWER.selectCaptures(captures).isEmpty())
        assertTrue(ActivityFilter.IGNORED.selectCaptures(captures).isEmpty())
        assertTrue(ActivityFilter.GOPAY.selectCaptures(captures).isEmpty())
    }
}
