package com.alfonsusenrico.financialtracker.listener.data.prefs

import android.content.Context
import android.content.SharedPreferences
import android.os.Build
import com.alfonsusenrico.financialtracker.listener.BuildConfig
import java.util.UUID
import java.security.MessageDigest
import org.json.JSONObject

class AppPreferences(context: Context) {

    private val prefs: SharedPreferences = context.getSharedPreferences(
        PREFS_NAME,
        Context.MODE_PRIVATE
    )

    companion object {
        private const val PREFS_NAME = "financial_tracker_prefs"
        private const val KEY_SERVER_URL = "server_url"
        private const val KEY_API_KEY = "api_key"
        private const val KEY_DEVICE_ID = "device_id"
        private const val KEY_AUTO_SYNC = "auto_sync_enabled"
        private const val KEY_LAST_SYNC_TIME = "last_sync_time"
        private const val KEY_BATTERY_EXEMPT_REQUESTED = "did_request_battery_exempt"
        private const val KEY_PAIRING_CONTEXT = "pairing_context"
        private const val KEY_KNOWN_PAIRINGS = "known_pairings"
        private const val KEY_LISTENER_CONNECTED_AT = "listener_connected_at"
        private const val KEY_LISTENER_DISCONNECTED_AT = "listener_disconnected_at"
        private const val KEY_LISTENER_DISCONNECT_COUNT = "listener_disconnect_count"
        private const val KEY_LAST_CAPTURE_AT = "last_capture_at"
        private const val KEY_LAST_DIRECT_SYNC_AT = "last_direct_sync_at"
        private const val KEY_LAST_DIRECT_SYNC_OUTCOME = "last_direct_sync_outcome"
        private const val KEY_CAPTURE_FAILURE_COUNT = "capture_failure_count"
        private const val KEY_LAST_CAPTURE_FAILURE_AT = "last_capture_failure_at"
        private const val KEY_DROPPED_COUNT = "dropped_count"
        private const val KEY_THEME_MODE = "theme_mode"
        // Written by 2.3.x, which cached the tracker's money summary; removed on start since 2.4.0.
        private const val KEY_LEDGER_SUMMARY = "ledger_summary"
        private const val KEY_LEDGER_SUMMARY_PAIRING = "ledger_summary_pairing"
        private const val KEY_SERVER_CONTACT = "server_contact"
        private const val KEY_SERVER_CONTACT_AT = "server_contact_at"
        private const val KEY_EVENING_REMINDER = "evening_reminder_enabled"
        private const val KEY_EVENING_REMINDER_MINUTE = "evening_reminder_minute"
        private const val KEY_SELF_TEST_STATUS = "self_test_status"
        private const val KEY_SELF_TEST_STEP = "self_test_failed_step"
        private const val KEY_SELF_TEST_AT = "self_test_at"
        private const val KEY_HEALTH_REPORTED_AT = "health_reported_at"
        private const val KEY_HEALTH_UNSUPPORTED_AT = "health_unsupported_at"
        private const val KEY_HEALTH_NOT_BEFORE = "health_not_before"
        private const val KEY_HEALTH_REVOKED_PAIRING = "health_revoked_pairing"
        private const val KEY_REPORT_LABEL = "report_ready_label"
        private const val KEY_REPORT_URL = "report_ready_url"
        private const val KEY_REPORT_START = "report_ready_start"
        private const val KEY_REPORT_PAIRING = "report_ready_pairing"
        private const val KEY_REPORT_NOTIFIED_START = "report_notified_start"
        private const val KEY_REPORT_OPENED_START = "report_opened_start"
        private const val KEY_REPORT_CHECKED_AT = "report_checked_at"

        /** The evening reminder's default time, 20.00, in minutes after midnight. */
        const val DEFAULT_REMINDER_MINUTE = 20 * 60

        // Default production domain
        const val DEFAULT_SERVER_URL = "https://finance.alfonsusenrico.com"
    }

    init {
        synchronized(prefs) {
            val legacyUrl = prefs.getString(KEY_SERVER_URL, DEFAULT_SERVER_URL)
                ?.trim()?.removeSuffix("/") ?: DEFAULT_SERVER_URL
            if (legacyUrl != BuildConfig.SERVER_URL) {
                check(prefs.edit().remove(KEY_API_KEY).remove(KEY_KNOWN_PAIRINGS)
                    .remove(KEY_LEDGER_SUMMARY).remove(KEY_LEDGER_SUMMARY_PAIRING)
                    .remove(KEY_REPORT_LABEL).remove(KEY_REPORT_URL).remove(KEY_REPORT_START).remove(KEY_REPORT_PAIRING)
                    .putString(KEY_PAIRING_CONTEXT, UUID.randomUUID().toString())
                    .putString(KEY_SERVER_URL, BuildConfig.SERVER_URL).commit()) { "pairing_storage_failed" }
            }
            // The phone no longer shows money; a summary cached by an older version is personal data to drop.
            if (prefs.contains(KEY_LEDGER_SUMMARY) || prefs.contains(KEY_LEDGER_SUMMARY_PAIRING)) {
                prefs.edit().remove(KEY_LEDGER_SUMMARY).remove(KEY_LEDGER_SUMMARY_PAIRING).apply()
            }
        }
    }

    val serverUrl: String get() = BuildConfig.SERVER_URL

    var apiKey: String
        get() = prefs.getString(KEY_API_KEY, "") ?: ""
        set(value) {
            val normalized = value.trim()
            updatePairing(normalized)
        }

    private fun pairingFingerprint(url: String, key: String): String =
        MessageDigest.getInstance("SHA-256").digest("$url\u0000$key".toByteArray(Charsets.UTF_8))
            .joinToString("") { "%02x".format(it) }

    private fun updatePairing(value: String) = synchronized(prefs) {
        val currentValue = apiKey
        if (value == currentValue) return@synchronized
        val known = try {
            JSONObject(prefs.getString(KEY_KNOWN_PAIRINGS, "{}") ?: "{}")
        } catch (_: org.json.JSONException) {
            JSONObject()
        }
        // Store only opaque fingerprints in the already backup-excluded preferences.
        known.put(pairingFingerprint(serverUrl, apiKey), pairingContext)
        val fingerprint = pairingFingerprint(serverUrl, value)
        val context = known.optString(fingerprint).takeIf { it.isNotBlank() } ?: UUID.randomUUID().toString()
        known.put(fingerprint, context)
        // Another key may belong to another account: its report notice and server state must not outlive the switch.
        prefs.edit().putString(KEY_API_KEY, value).putString(KEY_PAIRING_CONTEXT, context)
            .remove(KEY_REPORT_LABEL).remove(KEY_REPORT_URL).remove(KEY_REPORT_START).remove(KEY_REPORT_PAIRING)
            .remove(KEY_REPORT_CHECKED_AT).remove(KEY_SERVER_CONTACT).remove(KEY_SERVER_CONTACT_AT)
            .putString(KEY_KNOWN_PAIRINGS, known.toString()).commit().also { check(it) { "pairing_storage_failed" } }
    }

    val pairingContext: String
        get() = synchronized(prefs) {
            val existing = prefs.getString(KEY_PAIRING_CONTEXT, null)
            if (!existing.isNullOrBlank()) return@synchronized existing
            val generated = UUID.randomUUID().toString()
            prefs.edit().putString(KEY_PAIRING_CONTEXT, generated).apply()
            generated
        }

    var deviceId: String
        get() {
            var id = prefs.getString(KEY_DEVICE_ID, null)
            if (id.isNullOrBlank()) {
                val model = Build.MODEL.replace(" ", "_").lowercase()
                id = "${model}_${UUID.randomUUID().toString().take(8)}"
                prefs.edit().putString(KEY_DEVICE_ID, id).apply()
            }
            return id
        }
        set(value) = prefs.edit().putString(KEY_DEVICE_ID, value.trim()).apply()

    var isAutoSyncEnabled: Boolean
        get() = prefs.getBoolean(KEY_AUTO_SYNC, true)
        set(value) = prefs.edit().putBoolean(KEY_AUTO_SYNC, value).apply()

    var lastSyncTime: Long
        get() = prefs.getLong(KEY_LAST_SYNC_TIME, 0L)
        set(value) = prefs.edit().putLong(KEY_LAST_SYNC_TIME, value).apply()

    /** The stored code of the owner's theme choice; absent until the owner picks one. */
    var themeMode: String?
        get() = prefs.getString(KEY_THEME_MODE, null)
        set(value) = prefs.edit().putString(KEY_THEME_MODE, value).apply()

    var setupCompleted: Boolean
        get() = prefs.getBoolean("setup_completed", false)
        set(value) = prefs.edit().putBoolean("setup_completed", value).apply()

    var didRequestBatteryExempt: Boolean
        get() = prefs.getBoolean(KEY_BATTERY_EXEMPT_REQUESTED, false)
        set(value) = prefs.edit().putBoolean(KEY_BATTERY_EXEMPT_REQUESTED, value).apply()

    val listenerConnectedAt: Long get() = prefs.getLong(KEY_LISTENER_CONNECTED_AT, 0L)
    val listenerDisconnectedAt: Long get() = prefs.getLong(KEY_LISTENER_DISCONNECTED_AT, 0L)
    val listenerDisconnectCount: Int get() = prefs.getInt(KEY_LISTENER_DISCONNECT_COUNT, 0)
    val lastCaptureAt: Long get() = prefs.getLong(KEY_LAST_CAPTURE_AT, 0L)
    val lastDirectSyncAt: Long get() = prefs.getLong(KEY_LAST_DIRECT_SYNC_AT, 0L)
    val lastDirectSyncOutcome: String? get() = prefs.getString(KEY_LAST_DIRECT_SYNC_OUTCOME, null)
    val captureFailureCount: Int get() = prefs.getInt(KEY_CAPTURE_FAILURE_COUNT, 0)
    val lastCaptureFailureAt: Long get() = prefs.getLong(KEY_LAST_CAPTURE_FAILURE_AT, 0L)
    val droppedCount: Int get() = prefs.getInt(KEY_DROPPED_COUNT, 0)

    fun recordListenerConnected(now: Long = System.currentTimeMillis()) =
        prefs.edit().putLong(KEY_LISTENER_CONNECTED_AT, now).apply()

    fun recordListenerDisconnected(now: Long = System.currentTimeMillis()) =
        prefs.edit().putLong(KEY_LISTENER_DISCONNECTED_AT, now)
            .putInt(KEY_LISTENER_DISCONNECT_COUNT, listenerDisconnectCount + 1).apply()

    fun recordCapture(now: Long = System.currentTimeMillis()) = prefs.edit().putLong(KEY_LAST_CAPTURE_AT, now).apply()

    fun recordCaptureFailure(now: Long = System.currentTimeMillis()) = synchronized(prefs) {
        prefs.edit().putInt(KEY_CAPTURE_FAILURE_COUNT, captureFailureCount + 1)
            .putLong(KEY_LAST_CAPTURE_FAILURE_AT, now).apply()
    }

    fun recordDrop() = synchronized(prefs) {
        prefs.edit().putInt(KEY_DROPPED_COUNT, droppedCount + 1).apply()
    }

    fun recordDirectSync(outcome: String, now: Long = System.currentTimeMillis()) =
        prefs.edit().putLong(KEY_LAST_DIRECT_SYNC_AT, now).putString(KEY_LAST_DIRECT_SYNC_OUTCOME, outcome).apply()

    /** The latest answer from the tracker: [ServerContact.OK], [ServerContact.UNREACHABLE] or [ServerContact.REFUSED]. */
    val serverContact: String? get() = prefs.getString(KEY_SERVER_CONTACT, null)
    val serverContactAt: Long get() = prefs.getLong(KEY_SERVER_CONTACT_AT, 0L)

    fun recordServerContact(outcome: String, now: Long = System.currentTimeMillis()) =
        prefs.edit().putString(KEY_SERVER_CONTACT, outcome).putLong(KEY_SERVER_CONTACT_AT, now).apply()

    /** The evening reminder for light questions; on by default (owner decision of 2026-11-02). */
    var isEveningReminderEnabled: Boolean
        get() = prefs.getBoolean(KEY_EVENING_REMINDER, true)
        set(value) = prefs.edit().putBoolean(KEY_EVENING_REMINDER, value).apply()

    /** Minutes after local midnight; an out-of-range stored value falls back to 20.00. */
    var eveningReminderMinute: Int
        get() = prefs.getInt(KEY_EVENING_REMINDER_MINUTE, DEFAULT_REMINDER_MINUTE).takeIf { it in 0 until 24 * 60 }
            ?: DEFAULT_REMINDER_MINUTE
        set(value) = prefs.edit().putInt(KEY_EVENING_REMINDER_MINUTE, value.coerceIn(0, 24 * 60 - 1)).apply()

    /** The last self-test: `passed`, `failed` or `not_run`, with the failed step and its time (0 when never run). */
    val selfTestStatus: String get() = prefs.getString(KEY_SELF_TEST_STATUS, null) ?: "not_run"
    val selfTestFailedStep: String? get() = prefs.getString(KEY_SELF_TEST_STEP, null)
    val selfTestAt: Long get() = prefs.getLong(KEY_SELF_TEST_AT, 0L)

    fun recordSelfTest(status: String, failedStep: String?, now: Long = System.currentTimeMillis()) =
        prefs.edit().putString(KEY_SELF_TEST_STATUS, status).putString(KEY_SELF_TEST_STEP, failedStep)
            .putLong(KEY_SELF_TEST_AT, now).apply()

    var healthReportedAt: Long
        get() = prefs.getLong(KEY_HEALTH_REPORTED_AT, 0L)
        set(value) = prefs.edit().putLong(KEY_HEALTH_REPORTED_AT, value).apply()

    /** When a tracker last answered 404 to the health report: an older server, asked again later. */
    var healthUnsupportedAt: Long
        get() = prefs.getLong(KEY_HEALTH_UNSUPPORTED_AT, 0L)
        set(value) = prefs.edit().putLong(KEY_HEALTH_UNSUPPORTED_AT, value).apply()

    /** After a 429, no health report is sent before this time (epoch millis). */
    var healthNotBefore: Long
        get() = prefs.getLong(KEY_HEALTH_NOT_BEFORE, 0L)
        set(value) = prefs.edit().putLong(KEY_HEALTH_NOT_BEFORE, value).apply()

    /** True once the tracker refused this pairing's key for health reports; a re-pair starts reporting again. */
    val isHealthRevoked: Boolean get() = prefs.getString(KEY_HEALTH_REVOKED_PAIRING, null) == pairingContext

    fun markHealthRevoked() = prefs.edit().putString(KEY_HEALTH_REVOKED_PAIRING, pairingContext).apply()

    /** The tracker accepted this key again (Tes Koneksi), so health reports resume. */
    fun clearHealthRevoked() = prefs.edit().remove(KEY_HEALTH_REVOKED_PAIRING).apply()

    /** The ready monthly report the tracker named for [pairing], or null for another pairing or none. */
    fun readyReport(pairing: String): StoredReport? {
        if (prefs.getString(KEY_REPORT_PAIRING, null) != pairing) return null
        val label = prefs.getString(KEY_REPORT_LABEL, null) ?: return null
        val url = prefs.getString(KEY_REPORT_URL, null) ?: return null
        val start = prefs.getString(KEY_REPORT_START, null) ?: return null
        return StoredReport(label, url, start, opened = prefs.getString(KEY_REPORT_OPENED_START, null) == start)
    }

    /** Stores the ready report only while [pairing] is still current, so a late answer cannot follow a re-pair. */
    fun saveReadyReport(pairing: String, label: String, url: String, start: String): Boolean = synchronized(prefs) {
        if (pairing != pairingContext) return@synchronized false
        prefs.edit().putString(KEY_REPORT_LABEL, label).putString(KEY_REPORT_URL, url).putString(KEY_REPORT_START, start)
            .putString(KEY_REPORT_PAIRING, pairing).apply()
        true
    }

    fun clearReadyReport() = prefs.edit().remove(KEY_REPORT_LABEL).remove(KEY_REPORT_URL).remove(KEY_REPORT_START)
        .remove(KEY_REPORT_PAIRING).apply()

    var reportNotifiedStart: String?
        get() = prefs.getString(KEY_REPORT_NOTIFIED_START, null)
        set(value) = prefs.edit().putString(KEY_REPORT_NOTIFIED_START, value).apply()

    fun markReportOpened(start: String) = prefs.edit().putString(KEY_REPORT_OPENED_START, start).apply()

    var reportCheckedAt: Long
        get() = prefs.getLong(KEY_REPORT_CHECKED_AT, 0L)
        set(value) = prefs.edit().putLong(KEY_REPORT_CHECKED_AT, value).apply()

    val isConfigured: Boolean
        get() = serverUrl.isNotBlank() && apiKey.isNotBlank()

    fun isPackageEnabled(packageName: String): Boolean {
        val lower = packageName.lowercase()
        val defaultEnabled = when (lower) {
            "com.bca" -> false
            "com.shopee.id" -> false
            else -> true
        }
        return prefs.getBoolean("pkg_enabled_$lower", defaultEnabled)
    }

    fun setPackageEnabled(packageName: String, enabled: Boolean) {
        prefs.edit().putBoolean("pkg_enabled_${packageName.lowercase()}", enabled).apply()
    }
}

/** A ready monthly report: its label ("Oktober 2026"), web path, local start date, and whether it was opened. */
data class StoredReport(val label: String, val url: String, val start: String, val opened: Boolean)

/** Values of [AppPreferences.serverContact]. */
object ServerContact {
    const val OK = "ok"
    const val UNREACHABLE = "unreachable"
    const val REFUSED = "refused"
}
