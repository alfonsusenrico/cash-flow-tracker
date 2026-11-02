package com.alfonsusenrico.financialtracker.listener

import android.app.Application
import android.content.Context
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import com.alfonsusenrico.financialtracker.listener.MappingFixtures.ACCOUNT
import com.alfonsusenrico.financialtracker.listener.MappingFixtures.EVENT
import com.alfonsusenrico.financialtracker.listener.MappingFixtures.HASH
import com.alfonsusenrico.financialtracker.listener.MappingFixtures.TAG
import com.alfonsusenrico.financialtracker.listener.MappingFixtures.proposal
import com.alfonsusenrico.financialtracker.listener.MappingFixtures.recorded
import com.alfonsusenrico.financialtracker.listener.data.sync.NotificationResults
import com.alfonsusenrico.financialtracker.listener.receiver.MappingConfirmReceiver
import com.alfonsusenrico.financialtracker.listener.ui.shell.CompanionViewModel
import kotlinx.coroutines.runBlocking
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.annotation.Config

/** An account proposal confirmed from its notification and from Tanya, including every refusal. */
@RunWith(AndroidJUnit4::class)
@Config(sdk = [34], application = TestApplication::class)
class MappingAnswerPathTest {
    private val context = ApplicationProvider.getApplicationContext<Context>()
    private lateinit var server: MockWebServer
    private lateinit var harness: AnswerPathHarness
    private var rowId = 0L
    private val tag = TAG
    private lateinit var asked: String
    private var confirmResponse = MockResponse().setResponseCode(503)
    private val confirmPaths = mutableListOf<String>()

    @Before fun open() = runBlocking {
        // Only confirm-mapping answers; the view model's metadata requests get a 404 and leave it alone.
        server = MockWebServer().apply {
            dispatcher = object : Dispatcher() {
                override fun dispatch(request: RecordedRequest): MockResponse =
                    if (request.path?.endsWith("/confirm-mapping") == true) {
                        synchronized(confirmPaths) { confirmPaths += request.path!! }
                        confirmResponse
                    } else {
                        MockResponse().setResponseCode(404)
                    }
            }
            start()
        }
        harness = AnswerPathHarness(context, server.url("/").toString())
        rowId = harness.insertRow(HASH)
        NotificationResults(context, harness.db, scheduleFollowUp = { _, _ -> })
            .consume(harness.db.notificationDao().getById(rowId)!!, proposal(), harness.pairing)
        assertEquals(2, proposalShown()?.actions?.size)
        asked = harness.text(proposalShown())!!
    }

    @After fun close() {
        harness.close()
        server.shutdown()
    }

    private fun proposalShown() = harness.shown(tag)

    private fun status() = runBlocking { harness.db.notificationDao().getById(rowId)!!.processingStatus }

    private fun refused() = harness.settle { harness.text(proposalShown()) != asked }

    @Test fun confirmationFromTheNotificationIsApplied() {
        confirmResponse = recorded()
        proposalShown()!!.actions[0].actionIntent.send()
        harness.settle { status() == "recorded" }
        assertEquals("recorded", status())
        assertEquals(listOf("/api/ingest/notifications/$EVENT/confirm-mapping"), confirmPaths)
    }

    @Test fun tanyaConfirmationIsApplied() {
        confirmResponse = recorded()
        val viewModel = CompanionViewModel(context as Application, harness.db)
        var outcome: Boolean? = null
        viewModel.confirmMapping(rowId, ACCOUNT) { ok, _ -> outcome = ok }
        harness.settle { outcome != null }
        assertEquals(true, outcome)
        assertEquals("recorded", status())
    }

    @Test fun serverFailureLeavesTheProposalAnswerable() {
        proposalShown()!!.actions[0].actionIntent.send()
        refused()
        assertEquals("needs_confirmation", status())
        assertEquals("A failed confirmation must stay answerable from the notification", 2, proposalShown()?.actions?.size ?: 0)
    }

    @Test fun offlineConfirmationLeavesTheProposalAnswerable() {
        server.shutdown()
        proposalShown()!!.actions[0].actionIntent.send()
        refused()
        assertEquals("needs_confirmation", status())
        assertEquals("An offline confirmation must stay answerable from the notification", 2, proposalShown()?.actions?.size ?: 0)
    }

    @Test fun confirmationOfAChangedQuestionLeavesTheOpenProposalAnswerable() {
        val old = "00000000-0000-4000-8000-000000000699"
        context.sendBroadcast(MappingConfirmReceiver.intent(context, EVENT, ACCOUNT, rowId, old))
        refused()
        assertEquals(emptyList<String>(), confirmPaths)
        assertEquals("The open proposal must stay answerable", 2, proposalShown()?.actions?.size ?: 0)
    }

    @Test fun laterLookupOffersARefusedProposalAgain() = runBlocking {
        proposalShown()!!.actions[0].actionIntent.send()
        refused()
        NotificationResults(context, harness.db, scheduleFollowUp = { _, _ -> })
            .consume(harness.db.notificationDao().getById(rowId)!!, proposal(), harness.pairing)
        assertEquals("A later lookup must still offer the unanswered proposal", 2, proposalShown()?.actions?.size ?: 0)
    }
}
