package com.alfonsusenrico.financialtracker.listener.data.health

import android.Manifest
import android.app.NotificationChannel
import android.app.NotificationManager
import android.content.Context
import android.content.pm.PackageManager
import android.os.Build
import androidx.core.app.NotificationCompat
import com.alfonsusenrico.financialtracker.listener.R
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.withTimeoutOrNull
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

/**
 * The self-test's marker. The test notification is posted by this app in its own channel, with this tag and
 * a one-time token in its extras; the listener recognises it before any capture code runs, so it is never
 * stored, synced or sent to the tracker as a notification.
 */
object SelfTestMarker {
    const val CHANNEL_ID = "self_test"
    const val TAG = "cft_self_test"
    const val EXTRA_TOKEN = "com.alfonsusenrico.financialtracker.SELF_TEST_TOKEN"
    const val NOTIFICATION_ID = 4_2_4_2

    /** The token of a notification this app posted as a self-test, or null for anything else. */
    fun token(packageName: String?, ownPackage: String, tag: String?, extraToken: String?): String? =
        extraToken?.takeIf { packageName == ownPackage && tag == TAG && it.isNotBlank() }
}

/** Self-tests waiting for their notification to reach the listener; both run in the app's process. */
object SelfTestCatches {
    private val waiting = ConcurrentHashMap<String, CompletableDeferred<Unit>>()

    fun expect(token: String): CompletableDeferred<Unit> = CompletableDeferred<Unit>().also { waiting[token] = it }

    /** Called by the listener; true when a running self-test was waiting for [token]. */
    fun caught(token: String): Boolean = waiting.remove(token)?.complete(Unit) == true

    fun forget(token: String) {
        waiting.remove(token)
    }
}

enum class SelfTestStep(val wire: String) { POST("post"), CAPTURE("capture"), SERVER("server") }

/** How a step of the test is going, as the setup card's three-line checklist shows it. */
enum class StepState { WAITING, RUNNING, DONE, FAILED, SKIPPED }

data class SelfTestOutcome(val passed: Boolean, val failedStep: SelfTestStep?)

/** Posts and removes the test notification; a fake in tests. */
interface SelfTestPoster {
    /** False when Android would not show it: no permission, notifications off, or the channel off. */
    fun post(token: String): Boolean
    fun remove()
}

/**
 * The setup self-test: post a marked notification, catch it with this phone's own listener within 10 s, then
 * tell the tracker in a health report that the phone is ready. Each failure names its step, and the result
 * is kept for the next health reports.
 */
class SelfTestRunner(
    private val poster: SelfTestPoster,
    private val prefs: AppPreferences,
    private val report: suspend () -> HealthReporter.Outcome,
    private val requestLaterReport: () -> Unit,
    private val catchTimeoutMillis: Long = CATCH_TIMEOUT_MILLIS,
    private val newToken: () -> String = { UUID.randomUUID().toString() }
) {
    suspend fun run(onStep: (SelfTestStep, StepState) -> Unit = { _, _ -> }): SelfTestOutcome {
        SelfTestStep.entries.forEach { onStep(it, StepState.WAITING) }
        onStep(SelfTestStep.POST, StepState.RUNNING)
        val token = newToken()
        val caught = SelfTestCatches.expect(token)
        if (!poster.post(token)) {
            SelfTestCatches.forget(token)
            onStep(SelfTestStep.POST, StepState.FAILED)
            return fail(SelfTestStep.POST)
        }
        onStep(SelfTestStep.POST, StepState.DONE)
        onStep(SelfTestStep.CAPTURE, StepState.RUNNING)
        val heard = withTimeoutOrNull(catchTimeoutMillis) { caught.await() } != null
        SelfTestCatches.forget(token)
        poster.remove()
        if (!heard) {
            onStep(SelfTestStep.CAPTURE, StepState.FAILED)
            return fail(SelfTestStep.CAPTURE)
        }
        onStep(SelfTestStep.CAPTURE, StepState.DONE)
        return reportReady(onStep)
    }

    /** The server step alone, after "Coba lagi": the phone already caught its notification. */
    suspend fun reportReady(onStep: (SelfTestStep, StepState) -> Unit = { _, _ -> }): SelfTestOutcome {
        onStep(SelfTestStep.SERVER, StepState.RUNNING)
        prefs.recordSelfTest(SelfTestState.PASSED, null)
        return when (report()) {
            // An older tracker cannot store the report, but the phone itself is ready; nothing to fix here.
            HealthReporter.Outcome.SENT, HealthReporter.Outcome.UNSUPPORTED -> {
                onStep(SelfTestStep.SERVER, StepState.DONE)
                SelfTestOutcome(true, null)
            }
            else -> {
                onStep(SelfTestStep.SERVER, StepState.FAILED)
                prefs.recordSelfTest(SelfTestState.FAILED, SelfTestStep.SERVER.wire)
                requestLaterReport()
                SelfTestOutcome(false, SelfTestStep.SERVER)
            }
        }
    }

    private fun fail(step: SelfTestStep): SelfTestOutcome {
        prefs.recordSelfTest(SelfTestState.FAILED, step.wire)
        requestLaterReport()
        return SelfTestOutcome(false, step)
    }

    companion object {
        const val CATCH_TIMEOUT_MILLIS = 10_000L
    }
}

/** The real test notification, in its own quiet channel. */
class AndroidSelfTestPoster(private val context: Context) : SelfTestPoster {
    private val manager = context.getSystemService(NotificationManager::class.java)

    override fun post(token: String): Boolean {
        if (Build.VERSION.SDK_INT >= 33 &&
            context.checkSelfPermission(Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED
        ) return false
        if (manager.getNotificationChannel(SelfTestMarker.CHANNEL_ID) == null) {
            manager.createNotificationChannel(
                NotificationChannel(SelfTestMarker.CHANNEL_ID, "Tes pendengar", NotificationManager.IMPORTANCE_LOW).apply {
                    description = "Notifikasi tes dari aplikasi ini sendiri; tidak dicatat sebagai transaksi"
                    setShowBadge(false)
                }
            )
        }
        val channelOn = manager.getNotificationChannel(SelfTestMarker.CHANNEL_ID)?.importance != NotificationManager.IMPORTANCE_NONE
        if (!manager.areNotificationsEnabled() || !channelOn) return false
        val notification = NotificationCompat.Builder(context, SelfTestMarker.CHANNEL_ID)
            .setSmallIcon(R.drawable.ic_notification)
            .setContentTitle("Tes Cash Flow Tracker")
            .setContentText("Ini notifikasi tes, bukan transaksi. Nggak bakal kecatat.")
            .setTimeoutAfter(SelfTestRunner.CATCH_TIMEOUT_MILLIS + 5_000)
            .setSilent(true)
            .addExtras(android.os.Bundle().apply { putString(SelfTestMarker.EXTRA_TOKEN, token) })
            .build()
        return try {
            manager.notify(SelfTestMarker.TAG, SelfTestMarker.NOTIFICATION_ID, notification)
            true
        } catch (_: RuntimeException) {
            false
        }
    }

    override fun remove() {
        manager.cancel(SelfTestMarker.TAG, SelfTestMarker.NOTIFICATION_ID)
    }
}

/** The runner as the app uses it: the real poster, reporter and background retry. */
fun selfTestRunner(context: Context): SelfTestRunner {
    val app = context.applicationContext
    return SelfTestRunner(
        poster = AndroidSelfTestPoster(app),
        prefs = AppPreferences(app),
        report = { HealthReporter(app).send() },
        requestLaterReport = { HealthReports.request(app) }
    )
}

/** The self-test as a screen shows it: each step's state, whether it runs, and how it ended. */
data class SelfTestProgress(
    val steps: Map<SelfTestStep, StepState> = SelfTestStep.entries.associateWith { StepState.WAITING },
    val running: Boolean = false,
    val outcome: SelfTestOutcome? = null
)

/** Runs one self-test at a time for a screen and publishes its progress. */
class SelfTestSession(private val runner: () -> SelfTestRunner) {
    private val _progress = kotlinx.coroutines.flow.MutableStateFlow(SelfTestProgress())
    val progress: kotlinx.coroutines.flow.StateFlow<SelfTestProgress> = _progress

    private fun step(step: SelfTestStep, state: StepState) {
        _progress.value = _progress.value.copy(steps = _progress.value.steps + (step to state))
    }

    suspend fun run(): SelfTestOutcome? {
        if (_progress.value.running) return null
        _progress.value = SelfTestProgress(running = true)
        val outcome = runner().run(::step)
        _progress.value = _progress.value.copy(running = false, outcome = outcome)
        return outcome
    }

    /** "Coba lagi" after the server step failed: the notification was already caught. */
    suspend fun retryServer(): SelfTestOutcome? {
        if (_progress.value.running) return null
        _progress.value = _progress.value.copy(running = true, outcome = null)
        val outcome = runner().reportReady(::step)
        _progress.value = _progress.value.copy(running = false, outcome = outcome)
        return outcome
    }

    fun reset() {
        if (!_progress.value.running) _progress.value = SelfTestProgress()
    }
}
