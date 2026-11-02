package com.alfonsusenrico.financialtracker.listener.data.health

import org.json.JSONObject
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

/** The self-test's last result as the health contract names it: `passed`, `failed` or `not_run`. */
data class SelfTestState(val status: String, val failedStep: String?, val at: Long) {
    companion object {
        const val PASSED = "passed"
        const val FAILED = "failed"
        const val NOT_RUN = "not_run"
        val STATUSES = setOf(PASSED, FAILED, NOT_RUN)
        val STEPS = setOf("post", "capture", "server")
    }
}

/**
 * One `POST /api/devices/health` body (device health contract, 2026-11-02). It says how the phone is doing,
 * never who the owner is: no names, amounts or notification text. Times are epoch millis, 0 when unknown.
 */
data class HealthSnapshot(
    val appVersion: String,
    val listenerConnected: Boolean,
    val notificationAccess: Boolean,
    val postNotifications: Boolean,
    val batteryUnrestricted: Boolean,
    val selfTest: SelfTestState,
    val lastCaptureAt: Long,
    val queuePending: Int
) {
    /**
     * The JSON body with every field present, as the tracker validates it: a `failed` self-test names its step
     * and time, `passed` has a time and a null step, `not_run` has neither. A stored result that breaks those
     * rules is reported as `not_run` rather than guessed. Times carry their offset; [appVersion] keeps only the
     * characters the contract allows.
     */
    fun toJson(): String {
        val test = reportedSelfTest()
        val testJson = JSONObject()
            .put("status", test.status)
            .put("failed_step", test.failedStep ?: JSONObject.NULL)
            .put("at", if (test.status == SelfTestState.NOT_RUN) JSONObject.NULL else iso(test.at))
        return JSONObject()
            .put("app_version", version(appVersion))
            .put("listener_connected", listenerConnected)
            .put("notification_access", notificationAccess)
            .put("post_notifications", postNotifications)
            .put("battery_unrestricted", batteryUnrestricted)
            .put("self_test", testJson)
            .put("last_capture_at", if (lastCaptureAt > 0) iso(lastCaptureAt) else JSONObject.NULL)
            .put("queue_pending", queuePending.coerceAtLeast(0))
            .toString()
    }

    /** The self-test as the contract allows it to be reported. */
    fun reportedSelfTest(): SelfTestState {
        val notRun = SelfTestState(SelfTestState.NOT_RUN, null, 0)
        return when (selfTest.status) {
            SelfTestState.PASSED -> if (selfTest.at > 0) selfTest.copy(failedStep = null) else notRun
            SelfTestState.FAILED -> if (selfTest.at > 0 && selfTest.failedStep in SelfTestState.STEPS) selfTest else notRun
            else -> notRun
        }
    }

    /** The contract's `ready`, for the setup screen: battery is advisory and never blocks it. */
    val ready: Boolean get() = notificationAccess && listenerConnected && selfTest.status == SelfTestState.PASSED

    private fun iso(millis: Long): String = ISO_WITH_OFFSET.format(Instant.ofEpochMilli(millis).atOffset(ZoneOffset.UTC))

    companion object {
        /** Always an explicit offset ("+00:00"), never a bare local time. */
        private val ISO_WITH_OFFSET = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSxxx")
        private val VERSION_CHARACTERS = Regex("[^0-9A-Za-z.+_-]")

        /** `^[0-9A-Za-z.+_-]{1,32}$`: other characters dropped, cut to 32, "unknown" when nothing is left. */
        fun version(raw: String): String = raw.replace(VERSION_CHARACTERS, "").take(32).ifEmpty { "unknown" }
    }
}
