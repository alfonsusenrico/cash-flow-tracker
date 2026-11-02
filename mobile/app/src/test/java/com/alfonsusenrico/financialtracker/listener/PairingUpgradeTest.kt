package com.alfonsusenrico.financialtracker.listener

import android.content.Context
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import org.junit.Assert.*
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.annotation.Config

@RunWith(AndroidJUnit4::class)
@Config(sdk = [34], application = TestApplication::class)
class PairingUpgradeTest {
    private val context = ApplicationProvider.getApplicationContext<Context>()
    private val raw get() = context.getSharedPreferences("financial_tracker_prefs", Context.MODE_PRIVATE)

    @Test fun equalServerLeavesContextAndFingerprintsByteIdentical() {
        val known = "{\"synthetic-fingerprint\":\"synthetic-context\"}"
        raw.edit().putString("server_url", BuildConfig.SERVER_URL + "/")
            .putString("api_key", "synthetic-upgrade-key").putString("pairing_context", "synthetic-context")
            .putString("known_pairings", known).putString("device_id", "synthetic-device").commit()
        val before = raw.all.toMap()
        val prefs = AppPreferences(context)
        assertEquals(BuildConfig.SERVER_URL, prefs.serverUrl)
        assertEquals("synthetic-context", prefs.pairingContext)
        assertEquals("synthetic-device", prefs.deviceId)
        assertEquals(before, raw.all)
    }
    @Test fun differentServerClearsCredentialsAndCannotReusePendingAnswerContext() {
        raw.edit().putString("server_url", "https://another.example.invalid")
            .putString("api_key", "synthetic-upgrade-key").putString("pairing_context", "old-context")
            .putString("device_id", "same-device").commit()
        val prefs = AppPreferences(context)
        assertEquals("", prefs.apiKey)
        assertNotEquals("old-context", prefs.pairingContext)
        assertEquals("same-device", prefs.deviceId)
        assertFalse(prefs.isConfigured)
    }
    @Test fun changingOnlyKeyNeverReusesThePreviousAccountContext() {
        val prefs = AppPreferences(context)
        prefs.apiKey = "synthetic-first-key"
        val first = prefs.pairingContext
        prefs.apiKey = "synthetic-second-key"
        assertNotEquals(first, prefs.pairingContext)
        prefs.apiKey = "synthetic-first-key"
        assertEquals(first, prefs.pairingContext)
    }
}
