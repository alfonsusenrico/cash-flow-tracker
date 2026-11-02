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
        private const val KEY_LEDGER_SUMMARY = "ledger_summary"
        private const val KEY_LEDGER_SUMMARY_PAIRING = "ledger_summary_pairing"

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
                    .putString(KEY_PAIRING_CONTEXT, UUID.randomUUID().toString())
                    .putString(KEY_SERVER_URL, BuildConfig.SERVER_URL).commit()) { "pairing_storage_failed" }
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
        // Another key may belong to another account, so its ledger figures must not outlive the switch.
        prefs.edit().putString(KEY_API_KEY, value).putString(KEY_PAIRING_CONTEXT, context)
            .remove(KEY_LEDGER_SUMMARY).remove(KEY_LEDGER_SUMMARY_PAIRING)
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

    /** The last complete server summary fetched with [pairing], as JSON; null for any other pairing. */
    fun ledgerSummaryJson(pairing: String): String? = synchronized(prefs) {
        if (prefs.getString(KEY_LEDGER_SUMMARY_PAIRING, null) != pairing) return@synchronized null
        prefs.getString(KEY_LEDGER_SUMMARY, null)
    }

    /** Stores the summary only while [pairing] is still current, so a late response cannot follow a re-pair. */
    fun saveLedgerSummary(pairing: String, json: String): Boolean = synchronized(prefs) {
        if (pairing != pairingContext) return@synchronized false
        prefs.edit().putString(KEY_LEDGER_SUMMARY, json).putString(KEY_LEDGER_SUMMARY_PAIRING, pairing).apply()
        true
    }

    fun clearLedgerSummary(pairing: String) = synchronized(prefs) {
        if (prefs.getString(KEY_LEDGER_SUMMARY_PAIRING, null) != pairing) return@synchronized
        prefs.edit().remove(KEY_LEDGER_SUMMARY).remove(KEY_LEDGER_SUMMARY_PAIRING).apply()
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
