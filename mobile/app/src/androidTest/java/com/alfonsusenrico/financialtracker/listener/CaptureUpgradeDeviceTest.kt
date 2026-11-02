package com.alfonsusenrico.financialtracker.listener

import androidx.test.platform.app.InstrumentationRegistry
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Assume.assumeTrue
import org.junit.Test

/**
 * After installing 2.3.0 over a 2.2.0 verification app seeded by [SyntheticDeviceSeedTest]: every row, the
 * key, the pairing and the settings are kept, and Room is at version 8 with an empty capture table. Runs
 * only with `-e captureUpgrade <pairing context recorded before the upgrade>`.
 */
class CaptureUpgradeDeviceTest {
    @Test fun upgradeKeepsRowsKeyAndPairingAndAddsCaptures() {
        val expectedPairing = InstrumentationRegistry.getArguments().getString("captureUpgrade")
        assumeTrue(expectedPairing != null)
        val context = InstrumentationRegistry.getInstrumentation().targetContext
        check(context.packageName.endsWith(".verification"))
        val prefs = AppPreferences(context)
        assertTrue(prefs.isConfigured)
        assertEquals(expectedPairing, prefs.pairingContext)
        assertFalse(prefs.isAutoSyncEnabled)
        assertTrue(prefs.isPackageEnabled("com.bca"))
        assertFalse(prefs.isPackageEnabled("com.stockbit.android"))
        val database = AppDatabase.getDatabase(context).openHelper.readableDatabase
        assertEquals(8, database.version)
        fun count(sql: String) = database.query(sql).use { it.moveToFirst(); it.getInt(0) }
        assertEquals(13, count("SELECT COUNT(*) FROM raw_notifications"))
        assertEquals(13, count("SELECT COUNT(*) FROM raw_notifications WHERE payload_hash LIKE 'synthetic-%'"))
        assertEquals(0, count("SELECT COUNT(*) FROM text_captures"))
    }
}
