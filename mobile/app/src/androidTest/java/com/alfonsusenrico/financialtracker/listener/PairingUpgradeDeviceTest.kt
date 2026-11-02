package com.alfonsusenrico.financialtracker.listener

import androidx.compose.ui.test.junit4.createAndroidComposeRule
import androidx.compose.ui.test.onNodeWithContentDescription
import androidx.compose.ui.test.onNodeWithText
import androidx.test.platform.app.InstrumentationRegistry
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import kotlinx.coroutines.runBlocking
import org.junit.Assert.*
import org.junit.Rule
import org.junit.Test

class PairingUpgradeDeviceTest {
    @get:Rule val compose = createAndroidComposeRule<MainActivity>()
    @Test fun configuredUpgradeOpensHomeWithSyntheticRecordsIntact(): Unit = runBlocking {
        compose.activityRule.scenario.onActivity {
            it.setTurnScreenOn(true)
            it.window.addFlags(android.view.WindowManager.LayoutParams.FLAG_KEEP_SCREEN_ON)
        }
        val context = InstrumentationRegistry.getInstrumentation().targetContext
        check(context.packageName.endsWith(".verification"))
        assertEquals("http://127.0.0.1:9", BuildConfig.SERVER_URL)
        val prefs = AppPreferences(context)
        assertTrue(prefs.isConfigured)
        assertFalse(prefs.isAutoSyncEnabled)
        val database = AppDatabase.getDatabase(context)
        database.openHelper.readableDatabase.query("SELECT COUNT(*) FROM raw_notifications").use { cursor ->
            assertTrue(cursor.moveToFirst())
            assertEquals(13, cursor.getInt(0))
        }
        compose.onNodeWithContentDescription("Beranda").assertExists()
        compose.onNodeWithText("Hubungkan ke tracker").assertDoesNotExist()
    }
}
