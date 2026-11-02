package com.alfonsusenrico.financialtracker.listener

import android.app.Application
import android.content.Context
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import com.alfonsusenrico.financialtracker.listener.PurposeFixtures.EVENT
import com.alfonsusenrico.financialtracker.listener.PurposeFixtures.HASH
import com.alfonsusenrico.financialtracker.listener.PurposeFixtures.SOURCE_QUESTION
import com.alfonsusenrico.financialtracker.listener.data.network.model.AccountChoice
import com.alfonsusenrico.financialtracker.listener.data.network.model.IngestResultItem
import com.alfonsusenrico.financialtracker.listener.data.network.model.SourcePocketOptions
import com.alfonsusenrico.financialtracker.listener.data.sync.NotificationResults
import com.alfonsusenrico.financialtracker.listener.ui.shell.CompanionViewModel
import com.google.gson.Gson
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.Shadows.shadowOf
import org.robolectric.annotation.Config

/**
 * The Jago source-pocket question has no answer inside its notification: its one action opens the app, and the
 * answer is given in Tanya, which reports a refusal in place and leaves the notification as it was.
 */
@RunWith(AndroidJUnit4::class)
@Config(sdk = [34], application = TestApplication::class)
class SourcePocketAnswerPathTest {
    private val context = ApplicationProvider.getApplicationContext<Context>()
    private lateinit var harness: AnswerPathHarness
    private var rowId = 0L
    private val tag = "source-pocket:$EVENT"
    private val pocket = "00000000-0000-4000-8000-000000000708"

    @Before fun open() = runBlocking {
        harness = AnswerPathHarness(context)
        rowId = harness.insertRow(HASH)
        val item = Gson().fromJson(PurposeFixtures.sourcePocketJson(), IngestResultItem::class.java)
        NotificationResults(context, harness.db, scheduleFollowUp = { _, _ -> })
            .consume(harness.db.notificationDao().getById(rowId)!!, item, harness.pairing)
        val row = harness.db.notificationDao().getById(rowId)!!
        val options = SourcePocketOptions(EVENT, SOURCE_QUESTION, listOf(AccountChoice(pocket, "Bank Jago · Kantong Contoh")))
        assertEquals(1, harness.db.notificationDao().saveSourcePocketOptions(rowId, harness.pairing, EVENT, Gson().toJson(options), row.resultJson))
    }

    @After fun close() = harness.close()

    private fun reply() = runBlocking { harness.db.senderReplyDao().forQuestion(harness.pairing, EVENT, SOURCE_QUESTION) }

    private fun choose(question: String): Pair<Boolean, String?> {
        val viewModel = CompanionViewModel(context as Application, harness.db)
        var outcome: Pair<Boolean, String?>? = null
        viewModel.confirmSourcePocket(rowId, question, pocket) { ok, message -> outcome = ok to message }
        harness.settle { outcome != null }
        return outcome!!
    }

    @Test fun notificationActionOnlyOpensTheApp() {
        val action = harness.shown(tag)!!.actions.single()
        assertTrue(shadowOf(action.actionIntent).isActivityIntent)
        assertFalse(shadowOf(action.actionIntent).isBroadcastIntent)
    }

    @Test fun tanyaChoiceIsStoredForItsOwnQuestion() {
        assertEquals(true, choose(SOURCE_QUESTION).first)
        assertEquals("source_pocket", reply()?.answerKind)
        assertEquals(pocket, reply()?.accountId)
    }

    @Test fun refusedTanyaChoiceSaysSoAndLeavesTheNotification() {
        val before = harness.shown(tag)!!
        val (ok, message) = choose("00000000-0000-4000-8000-000000000799")
        assertFalse(ok)
        assertNotNull(message)
        assertNull(reply())
        assertEquals(1, harness.shown(tag)?.actions?.size)
        assertEquals(harness.text(before), harness.text(harness.shown(tag)))
    }
}
