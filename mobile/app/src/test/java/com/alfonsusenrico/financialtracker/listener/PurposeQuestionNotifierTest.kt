package com.alfonsusenrico.financialtracker.listener

import android.Manifest
import android.app.Application
import android.app.NotificationManager
import android.content.Context
import androidx.core.app.NotificationCompat
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import com.alfonsusenrico.financialtracker.listener.PurposeFixtures.EVENT
import com.alfonsusenrico.financialtracker.listener.PurposeFixtures.questionJson
import com.alfonsusenrico.financialtracker.listener.PurposeFixtures.result
import com.alfonsusenrico.financialtracker.listener.data.model.RawNotificationEntity
import com.alfonsusenrico.financialtracker.listener.receiver.PurposeAnswerReceiver
import com.alfonsusenrico.financialtracker.listener.service.PurposeQuestionNotifier
import com.alfonsusenrico.financialtracker.listener.service.PurposeQuestionNotifier.PromptAction
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.Shadows.shadowOf
import org.robolectric.annotation.Config

@RunWith(AndroidJUnit4::class)
@Config(sdk = [34], application = com.alfonsusenrico.financialtracker.listener.TestApplication::class)
class PurposeQuestionNotifierTest {
    private val context = ApplicationProvider.getApplicationContext<Context>()
    private val manager = context.getSystemService(NotificationManager::class.java)
    private val recordKey = "notification:$EVENT"
    private val row = RawNotificationEntity(
        id = 7, deviceId = "fictional", packageName = "com.bca.mybca.omni.android", appLabel = "myBCA",
        postTime = 1, payloadHash = PurposeFixtures.HASH, backendEventId = EVENT, recordKey = recordKey,
        pairingContext = "00000000-0000-4000-8000-000000000999", processingStatus = "recorded", isSynced = true
    )

    private fun content(json: String) = result(json).let { PurposeQuestionNotifier.content(it, it.question!!) }

    @Test fun titleNamesAmountAndAccountAndTextKeepsItsSlot() {
        val prompt = content(questionJson(suggestions = 5))
        assertEquals("Apa ini? Rp187.500 · BCA Harian", prompt.title)
        assertEquals(PurposeQuestionNotifier.MAX_ACTIONS, prompt.actions.size)
        assertEquals(listOf("s1", "s2", null), prompt.actions.map { it.suggestionId })
        assertEquals("Tulis", prompt.actions.last().label)
        assertTrue(prompt.text.startsWith("Apa ini? Rp187.500 dari BCA Harian"))
        assertTrue(prompt.text.endsWith("3 pilihan lain ada di aplikasi."))
    }

    @Test fun withoutTextAnswersAllThreeSlotsAreSuggestions() {
        val prompt = content(questionJson(suggestions = 4, allowsText = false))
        assertEquals(listOf("s1", "s2", "s3"), prompt.actions.map { it.suggestionId })
        assertTrue(prompt.text.endsWith("1 pilihan lain ada di aplikasi."))
        val few = content(questionJson(suggestions = 1))
        assertEquals(listOf(PromptAction("s1", "makan kafe senja 1"), PromptAction(null, "Tulis")), few.actions)
        assertFalse(few.text.contains("pilihan lain"))
        assertEquals(listOf(PromptAction(null, "Tulis")), content(questionJson(suggestions = 0)).actions)
    }

    @Test fun longLabelsAreShortenedAtAWordBoundary() {
        assertEquals("makan kafe senja", PurposeQuestionNotifier.actionLabel("  makan   kafe\nsenja "))
        assertEquals("x".repeat(24), PurposeQuestionNotifier.actionLabel("x".repeat(24)))
        assertEquals("sewa lapangan futsal…", PurposeQuestionNotifier.actionLabel("sewa lapangan futsal arena senayan"))
        assertEquals("x".repeat(23) + "…", PurposeQuestionNotifier.actionLabel("x".repeat(40)))
        val emoji = PurposeQuestionNotifier.actionLabel("🍜".repeat(30))
        assertEquals(24, emoji.codePointCount(0, emoji.length))
    }

    @Test fun postsOneActionableNotificationInTheRecordSlot() {
        shadowOf(context as Application).grantPermissions(Manifest.permission.POST_NOTIFICATIONS)
        assertTrue(PurposeQuestionNotifier.post(context, row, result(questionJson(suggestions = 3)), activeOnly = false))
        val posted = shadowOf(manager).allNotifications.single()
        assertEquals(PurposeQuestionNotifier.CHANNEL_ID, posted.channelId)
        assertEquals("Apa ini? Rp187.500 · BCA Harian", posted.extras.getString(NotificationCompat.EXTRA_TITLE))
        assertNotNull(shadowOf(manager).getNotification(recordKey, recordKey.hashCode() and Int.MAX_VALUE))
        assertEquals(listOf("makan kafe senja 1", "makan kafe senja 2", "Tulis"), posted.actions.map { it.title.toString() })
        assertNull(posted.actions[0].remoteInputs)
        assertEquals(PurposeAnswerReceiver.INPUT_TEXT, posted.actions[2].remoteInputs.single().resultKey)
        assertEquals(NotificationManager.IMPORTANCE_HIGH, manager.getNotificationChannel(PurposeQuestionNotifier.CHANNEL_ID).importance)
        assertNotNull(posted.publicVersion)
        assertNotNull(posted.contentIntent)

        PurposeQuestionNotifier.showState(context, row, "Jawaban tersimpan; mengirim ke server.")
        val state = shadowOf(manager).allNotifications.single()
        assertEquals(0, state.actions?.size ?: 0)
        assertEquals("Jawaban tersimpan; mengirim ke server.", state.extras.getString(NotificationCompat.EXTRA_TEXT))
    }

    @Test fun deniedPermissionPostsNothing() {
        shadowOf(context as Application).denyPermissions(Manifest.permission.POST_NOTIFICATIONS)
        assertFalse(PurposeQuestionNotifier.post(context, row, result(), activeOnly = false))
        assertTrue(shadowOf(manager).allNotifications.isEmpty())
    }

    @Test fun refreshOnlyUpdatesAPromptThatIsStillShown() {
        shadowOf(context as Application).grantPermissions(Manifest.permission.POST_NOTIFICATIONS)
        assertFalse(PurposeQuestionNotifier.post(context, row, result(), activeOnly = true))
        assertTrue(shadowOf(manager).allNotifications.isEmpty())
        assertFalse(PurposeQuestionNotifier.post(context, row, result(question = null), activeOnly = false))
        assertFalse(PurposeQuestionNotifier.post(context, row.copy(pairingContext = null), result(), activeOnly = false))
    }
}
