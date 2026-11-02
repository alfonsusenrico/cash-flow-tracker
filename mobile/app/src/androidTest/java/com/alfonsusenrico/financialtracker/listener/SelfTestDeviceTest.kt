package com.alfonsusenrico.financialtracker.listener

import android.Manifest
import android.content.pm.PackageManager
import android.os.Build
import androidx.test.platform.app.InstrumentationRegistry
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.health.AndroidSelfTestPoster
import com.alfonsusenrico.financialtracker.listener.data.health.HealthReporter
import com.alfonsusenrico.financialtracker.listener.data.health.SelfTestRunner
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.service.FinancialNotificationListenerService
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Assume.assumeTrue
import org.junit.Test

/**
 * The setup self-test on a real phone: the verification app posts its marked notification and its own listener
 * catches it within 10 s. The health report is faked, so nothing reaches a server, and the test checks that
 * the notification left no row in the capture database. Runs only with `-e selfTest true` in the `.verification`
 * app, once notification access and the notification permission are granted to it on the test phone.
 */
class SelfTestDeviceTest {
    private val context = InstrumentationRegistry.getInstrumentation().targetContext

    @Test fun theListenerCatchesTheTestNotificationAndStoresNothing() = runBlocking {
        assumeTrue(InstrumentationRegistry.getArguments().getString("selfTest") == "true")
        check(context.packageName.endsWith(".verification"))
        assumeTrue("notification access is granted to the verification app", FinancialNotificationListenerService.isPermissionGranted(context))
        assumeTrue(Build.VERSION.SDK_INT < 33 || context.checkSelfPermission(Manifest.permission.POST_NOTIFICATIONS) == PackageManager.PERMISSION_GRANTED)
        fun ownRows() = AppDatabase.getDatabase(context).openHelper.readableDatabase
            .query("SELECT COUNT(*) FROM raw_notifications WHERE package_name = ?", arrayOf(context.packageName))
            .use { it.moveToFirst(); it.getInt(0) }
        val before = ownRows()
        var reports = 0
        val outcome = SelfTestRunner(
            poster = AndroidSelfTestPoster(context), prefs = AppPreferences(context),
            report = { reports++; HealthReporter.Outcome.SENT }, requestLaterReport = {}
        ).run()
        assertNull("failed at ${outcome.failedStep}", outcome.failedStep)
        assertTrue(outcome.passed)
        assertEquals(1, reports)
        Thread.sleep(2_000)
        assertEquals("the test notification was never stored", before, ownRows())
    }
}
