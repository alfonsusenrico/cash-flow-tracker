package com.alfonsusenrico.financialtracker.listener

import android.app.Application
import android.app.KeyguardManager
import android.content.Context
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import com.alfonsusenrico.financialtracker.listener.PurposeFixtures.EVENT
import com.alfonsusenrico.financialtracker.listener.PurposeFixtures.HASH
import com.alfonsusenrico.financialtracker.listener.PurposeFixtures.QUESTION
import com.alfonsusenrico.financialtracker.listener.data.model.SenderReplyEntity
import com.alfonsusenrico.financialtracker.listener.data.sync.NotificationResults
import com.alfonsusenrico.financialtracker.listener.receiver.PurposeAnswerReceiver
import com.alfonsusenrico.financialtracker.listener.ui.shell.CompanionViewModel
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotEquals
import org.junit.Assert.assertNull
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.Shadows.shadowOf
import org.robolectric.annotation.Config

/** "Apa ini?" answered through the prompt's actions and through Tanya, including every refusal. */
@RunWith(AndroidJUnit4::class)
@Config(sdk = [34], application = TestApplication::class)
class PurposeAnswerPathTest {
    private val context = ApplicationProvider.getApplicationContext<Context>()
    private lateinit var harness: AnswerPathHarness
    private var rowId = 0L
    private val recordKey = "notification:$EVENT"
    private val actions = 3
    private lateinit var asked: String

    @Before fun open() = runBlocking {
        harness = AnswerPathHarness(context)
        rowId = harness.insertRow(HASH)
        val results = NotificationResults(context, harness.db, scheduleFollowUp = { _, _ -> })
        results.consume(harness.db.notificationDao().getById(rowId)!!, PurposeFixtures.result(), harness.pairing)
        results.deliverPendingAlerts(harness.pairing)
        assertEquals(actions, prompt()?.actions?.size)
        asked = harness.text(prompt())!!
    }

    @After fun close() = harness.close()

    private fun prompt() = harness.shown(recordKey)

    private fun reply(): SenderReplyEntity? = runBlocking { harness.db.senderReplyDao().forQuestion(harness.pairing, EVENT, QUESTION) }

    private fun answerText(text: String?) = harness.send(prompt()!!.actions[2], PurposeAnswerReceiver.INPUT_TEXT, text)

    private fun refused() = harness.settle { harness.text(prompt()) != asked }

    @Test fun suggestionIsStoredForItsOwnQuestion() {
        prompt()!!.actions[0].actionIntent.send()
        harness.settle { reply() != null }
        assertEquals("purpose", reply()?.answerKind)
        assertEquals("s1", reply()?.suggestionId)
        assertEquals(rowId, reply()?.rowId)
    }

    @Test fun textIsStoredForItsOwnQuestion() {
        answerText("sewa lapangan")
        harness.settle { reply() != null }
        assertEquals("sewa lapangan", reply()?.answerText)
    }

    @Test fun tanyaAnswerIsStoredForItsOwnQuestion() {
        val viewModel = CompanionViewModel(context as Application, harness.db)
        var outcome: Boolean? = null
        viewModel.answerPurpose(rowId, QUESTION, "s2", null) { ok, _ -> outcome = ok }
        harness.settle { outcome != null }
        assertEquals(true, outcome)
        assertEquals("s2", reply()?.suggestionId)
    }

    @Test fun blankTextLeavesThePromptAnswerable() {
        answerText(" ")
        refused()
        assertNull(reply())
        assertNotEquals(asked, harness.text(prompt()))
        assertEquals("A blank answer must stay correctable from the notification", actions, prompt()?.actions?.size ?: 0)
    }

    @Test fun missingTextLeavesThePromptAnswerable() {
        answerText(null)
        refused()
        assertNull(reply())
        assertEquals("Missing input must stay correctable from the notification", actions, prompt()?.actions?.size ?: 0)
    }

    @Test fun lockedDeviceLeavesThePromptAnswerable() {
        shadowOf(context.getSystemService(KeyguardManager::class.java)).setIsDeviceLocked(true)
        prompt()!!.actions[0].actionIntent.send()
        refused()
        assertNull(reply())
        assertEquals("A refused attempt must leave the prompt answerable", actions, prompt()?.actions?.size ?: 0)
    }

    @Test fun answerToAChangedQuestionLeavesTheOpenPromptAnswerable() {
        val old = "00000000-0000-4000-8000-000000000199"
        context.sendBroadcast(PurposeAnswerReceiver.intent(context, rowId, EVENT, old, harness.pairing, "s1"))
        refused()
        assertNull(reply())
        assertEquals("The open question must stay answerable", actions, prompt()?.actions?.size ?: 0)
    }

    @Test fun laterLookupOffersARefusedPromptAgain() = runBlocking {
        answerText(" ")
        refused()
        val results = NotificationResults(context, harness.db, scheduleFollowUp = { _, _ -> })
        results.consume(harness.db.notificationDao().getById(rowId)!!, PurposeFixtures.result(), harness.pairing)
        results.deliverPendingAlerts(harness.pairing)
        assertEquals("A later lookup must still offer the unanswered prompt", actions, prompt()?.actions?.size ?: 0)
    }
}
