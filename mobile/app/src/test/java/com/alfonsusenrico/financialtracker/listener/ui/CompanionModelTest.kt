package com.alfonsusenrico.financialtracker.listener.ui

import com.alfonsusenrico.financialtracker.listener.data.model.SenderReplyEntity
import com.alfonsusenrico.financialtracker.listener.ui.UiFixtures.today
import com.alfonsusenrico.financialtracker.listener.ui.model.ActivityFilter
import com.alfonsusenrico.financialtracker.listener.ui.components.Formatting
import com.alfonsusenrico.financialtracker.listener.ui.hub.HubCounts
import com.alfonsusenrico.financialtracker.listener.ui.shell.CompanionLists
import com.alfonsusenrico.financialtracker.listener.ui.model.MoneyDirection
import com.alfonsusenrico.financialtracker.listener.ui.model.OpenQuestions
import com.alfonsusenrico.financialtracker.listener.ui.model.QuestionKind
import com.alfonsusenrico.financialtracker.listener.ui.model.RowAbilities
import com.alfonsusenrico.financialtracker.listener.ui.model.RowPresentation
import com.alfonsusenrico.financialtracker.listener.ui.theme.ThemeMode
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

/** The figures, question rules and labels the redesigned screens are built from. */
class CompanionModelTest {
    @Test fun hubCountsTodaysRowsOfThisPhoneWithoutMoney() {
        val unsent = UiFixtures.unsent()
        val visible = listOf(
            UiFixtures.purpose(),
            UiFixtures.recorded(20, "Kopi Contoh", 32_000, hour = 10),
            UiFixtures.recorded(21, "Gaji Contoh", 250_000, type = "income", category = "Pemasukan"),
            UiFixtures.movement(),
            UiFixtures.recorded(22, "Ojek Contoh", 24_000, date = today.minusDays(1), category = "Transportasi"),
            UiFixtures.recorded(23, "Apotek Contoh", 58_000, date = today.minusDays(2), category = "Kesehatan"),
            UiFixtures.sender(), UiFixtures.pocket(), unsent, UiFixtures.review()
        )
        // The same row listed twice (visible and ignored lists can overlap) is one notification.
        val lists = CompanionLists(visible = visible, local = listOf(UiFixtures.phone()),
            ignored = listOf(UiFixtures.ignored(), UiFixtures.ignored()), loaded = true)
        val counts = HubCounts.of(lists, emptyList(), today, UiFixtures.zone)

        // Today: purpose, two recorded, movement, sender, pocket, unsent, kept and ignored.
        assertEquals(9, counts.capturedToday)
        // Only the unsent row still waits to reach the tracker; nothing here is an amount.
        assertEquals(1, counts.waiting)
        assertEquals(unsent.capturedAt, counts.oldestWaitingAt)
        val latest = (visible + UiFixtures.phone() + UiFixtures.ignored()).filter { Formatting.localDate(it.postTime, UiFixtures.zone) == today }
            .maxBy { it.postTime }
        assertEquals(latest.appLabel, counts.lastCaptureApp)
    }

    @Test fun hubCountsAreUnknownUntilRowsAndCapturesAreRead() {
        assertEquals(HubCounts(null, null, 0, null, null), HubCounts.of(CompanionLists(), emptyList(), today, UiFixtures.zone))
        val empty = HubCounts.of(CompanionLists(loaded = true), emptyList(), today, UiFixtures.zone)
        assertEquals(0, empty.capturedToday)
        assertEquals(0, empty.waiting)

        val capturesUnread = HubCounts.of(CompanionLists(visible = listOf(UiFixtures.unsent()), loaded = true), null, today, UiFixtures.zone)
        assertEquals(1, capturesUnread.capturedToday)
        assertNull("Unknown is never 0", capturesUnread.waiting)
    }

    @Test fun hubWaitingCountsQueuedCapturesAndRepliesButNotSettledOnes() {
        val captures = listOf(CaptureFixtures.queued(), CaptureFixtures.sending(), CaptureFixtures.recorded(), CaptureFixtures.asking(), CaptureFixtures.failed())
        val replies = listOf(
            SenderReplyEntity("r1", 1, "p", "00000000-0000-4000-8000-000000000001", "q1", "name", "Contoh", state = "queued"),
            SenderReplyEntity("r2", 1, "p", "00000000-0000-4000-8000-000000000001", "q2", "name", "Contoh", state = "failed")
        )
        val counts = HubCounts.of(CompanionLists(loaded = true, replies = replies), captures, today, UiFixtures.zone)
        // Queued and sending captures and the queued reply wait; recorded, asking and refused ones do not.
        assertEquals(3, counts.waiting)
    }

    @Test fun everyQuestionTypeIsOpenUntilTheOwnerAnswers() {
        val purpose = UiFixtures.purpose()
        val sender = UiFixtures.sender()
        val pocket = UiFixtures.pocket()
        val mapping = UiFixtures.mapping()
        val visible = listOf(purpose, sender, pocket, mapping, UiFixtures.review(), UiFixtures.recorded(20, "Kopi Contoh", 32_000))

        assertEquals(QuestionKind.PURPOSE, OpenQuestions.kindOf(purpose))
        assertEquals(QuestionKind.SENDER, OpenQuestions.kindOf(sender))
        assertEquals(QuestionKind.SOURCE_POCKET, OpenQuestions.kindOf(pocket))
        assertEquals(QuestionKind.MAPPING, OpenQuestions.kindOf(mapping))
        assertNull(OpenQuestions.kindOf(UiFixtures.review()))

        val open = OpenQuestions.open(visible, emptyList(), needsAnswer = listOf(purpose))
        assertEquals(listOf(purpose.id, sender.id, pocket.id, mapping.id), open.map { it.id })

        // A queued sender reply and a queued pocket choice take those rows out; a rejected one keeps them in.
        val replies = listOf(
            reply(sender.id, UiFixtures.uuid(904), "sender", "queued"),
            reply(pocket.id, UiFixtures.uuid(905), "source_pocket", "stale")
        )
        val afterReplies = OpenQuestions.open(visible, replies, needsAnswer = emptyList())
        assertEquals(listOf(pocket.id, mapping.id), afterReplies.map { it.id })
    }

    @Test fun needsAnswerFilterListsEveryOpenQuestionAndPerlucekKeepsItsRule() {
        val visible = listOf(UiFixtures.sender(), UiFixtures.review(), UiFixtures.unsent(), UiFixtures.recorded(20, "Kopi Contoh", 32_000))
        val questions = OpenQuestions.open(visible, emptyList(), emptyList())
        assertEquals(listOf(UiFixtures.sender().id), ActivityFilter.NEEDS_ANSWER.select(visible, emptyList(), emptyList(), questions).map { it.id })
        assertEquals(
            setOf(UiFixtures.sender().id, UiFixtures.review().id, UiFixtures.unsent().id),
            ActivityFilter.PENDING.select(visible, emptyList(), emptyList(), questions).map { it.id }.toSet()
        )
    }

    @Test fun statusLabelsStayDistinctAndUnknownAmountsAreNotZero() {
        val labels = listOf(
            UiFixtures.recorded(20, "Kopi Contoh", 32_000), UiFixtures.movement(), UiFixtures.purpose(), UiFixtures.sender(),
            UiFixtures.pocket(), UiFixtures.mapping(), UiFixtures.review(), UiFixtures.phone(), UiFixtures.ignored(), UiFixtures.unsent()
        ).map { RowPresentation.of(it).status.label }
        assertEquals(
            listOf("Tercatat", "Pindah saldo", "Apa ini?", "Siapa pengirimnya?", "Dari rekening atau kantong mana?", "Rekening yang mana?",
                "Perlu cek", "Di ponsel", "Diabaikan", "Belum dikirim"),
            labels
        )
        val review = RowPresentation.of(UiFixtures.review())
        assertNull(review.amount)
        assertEquals(MoneyDirection.IN, RowPresentation.of(UiFixtures.sender()).direction)
        assertEquals(112_590L, RowPresentation.of(UiFixtures.sender()).amount)
    }

    @Test fun abilitiesFollowTheDetailSheetRules() {
        val review = RowAbilities.of(RowPresentation.of(UiFixtures.review()), UiFixtures.PAIRING)
        assertTrue(review.canRetry)
        assertTrue(review.canResolve)
        assertTrue(review.reason!!.startsWith("Notifikasi memuat lebih dari satu nominal"))

        val otherPairing = RowAbilities.of(RowPresentation.of(UiFixtures.review()), "another-pairing")
        assertFalse(otherPairing.canRetry)

        val recorded = RowAbilities.of(RowPresentation.of(UiFixtures.recorded(20, "Kopi Contoh", 32_000)), UiFixtures.PAIRING)
        assertTrue(recorded.canEditRecorded)
        assertFalse(RowAbilities.of(RowPresentation.of(UiFixtures.movement()), UiFixtures.PAIRING).canEditRecorded)

        val mapping = RowAbilities.of(RowPresentation.of(UiFixtures.mapping()), UiFixtures.PAIRING)
        assertFalse("A question replaces the review box", mapping.canResolve)
        assertNull(mapping.reason)
        assertTrue(RowAbilities.of(RowPresentation.of(UiFixtures.unsent()), UiFixtures.PAIRING).canSubmit)
    }

    @Test fun storedThemeCodesAreValidatedAndDefaultToTheSystem() {
        assertEquals(ThemeMode.DARK, ThemeMode.fromCode("dark"))
        assertEquals(ThemeMode.LIGHT, ThemeMode.fromCode("light"))
        assertEquals(ThemeMode.SYSTEM, ThemeMode.fromCode(null))
        assertEquals(ThemeMode.SYSTEM, ThemeMode.fromCode("sepia"))
    }

    private fun reply(rowId: Long, questionId: String, kind: String, state: String) = SenderReplyEntity(
        replyId = "fictional-$rowId", rowId = rowId, pairingContext = UiFixtures.PAIRING, eventId = UiFixtures.uuid(rowId.toInt()),
        questionId = questionId, action = "name", name = null, state = state, answerKind = kind
    )
}
