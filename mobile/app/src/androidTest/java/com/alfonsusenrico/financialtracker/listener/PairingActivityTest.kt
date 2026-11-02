package com.alfonsusenrico.financialtracker.listener

import android.Manifest
import android.hardware.camera2.CameraManager
import android.hardware.camera2.CameraCharacteristics
import androidx.core.content.ContextCompat
import java.util.concurrent.ConcurrentHashMap
import android.content.Intent
import android.net.Uri
import androidx.compose.ui.test.assertIsDisplayed
import androidx.compose.ui.test.junit4.createEmptyComposeRule
import androidx.compose.ui.test.onNodeWithContentDescription
import androidx.compose.ui.test.onNodeWithText
import androidx.compose.ui.test.performClick
import androidx.compose.ui.test.performScrollTo
import androidx.test.core.app.ActivityScenario
import androidx.test.platform.app.InstrumentationRegistry
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import okhttp3.mockwebserver.MockWebServer
import org.junit.Assert.assertEquals
import org.junit.Before
import org.junit.After
import org.junit.Rule
import org.junit.Test
import java.net.URLEncoder

/** Explicit target activities and Compose events only; no global input, Back, Home or launcher taps. */
class PairingActivityTest {
    @get:Rule val compose = createEmptyComposeRule()
    private val context get() = InstrumentationRegistry.getInstrumentation().targetContext
    private lateinit var server: MockWebServer
    @Before fun setup() {
        check(context.packageName.endsWith(".verification"))
        InstrumentationRegistry.getInstrumentation().uiAutomation.grantRuntimePermission(context.packageName, Manifest.permission.CAMERA)
        server = MockWebServer().apply { start() }
        TestEndpoint.set(context, server.url("/").toString())
        AppPreferences(context).apiKey = ""
    }
    @After fun close() { server.shutdown() }

    private fun intent(raw: String) = Intent(context, PairingActivity::class.java).setAction(Intent.ACTION_VIEW).setData(Uri.parse(raw))
    private val qr get() = "cashflow://pair?v=1&server=${URLEncoder.encode(BuildConfig.SERVER_URL, "UTF-8")}&code=0123456789"

    @Test fun externalLinkRequiresConfirmationAndCancelSendsNothing() {
        ActivityScenario.launch<PairingActivity>(intent(qr)).use { scenario ->
            scenario.onActivity {
                it.setTurnScreenOn(true)
                it.window.addFlags(android.view.WindowManager.LayoutParams.FLAG_KEEP_SCREEN_ON)
            }
            compose.onNodeWithText("QR terbaca").assertIsDisplayed()
            compose.onNodeWithText("Hubungkan").performScrollTo().assertIsDisplayed()
            assertEquals(0, server.requestCount)
            compose.onNodeWithText("Batal").performScrollTo().performClick()
            compose.onNodeWithText("Hubungkan ke tracker").assertIsDisplayed()
            assertEquals(0, server.requestCount)
        }
    }
    @Test fun foreignLinkNeverReachesNetwork() {
        ActivityScenario.launch<PairingActivity>(intent(qr.replace("server=", "server=foreign"))).use { scenario ->
            scenario.onActivity {
                it.setTurnScreenOn(true)
                it.window.addFlags(android.view.WindowManager.LayoutParams.FLAG_KEEP_SCREEN_ON)
            }
            compose.onNodeWithText("Belum terhubung").assertIsDisplayed()
            compose.onNodeWithText("QR tidak valid. Pindai kode dari Pengaturan web tracker.").performScrollTo().assertIsDisplayed()
            assertEquals(0, server.requestCount)
        }
    }
    @Test fun cameraOpensAndClosesWithinTheVerificationActivity() {
        val manager = context.getSystemService(CameraManager::class.java)
        val backCameras = manager.cameraIdList.filter { manager.getCameraCharacteristics(it).get(CameraCharacteristics.LENS_FACING) == CameraCharacteristics.LENS_FACING_BACK }.toSet()
        val unavailable = ConcurrentHashMap.newKeySet<String>()
        val callback = object : CameraManager.AvailabilityCallback() {
            override fun onCameraUnavailable(cameraId: String) { unavailable.add(cameraId) }
            override fun onCameraAvailable(cameraId: String) { unavailable.remove(cameraId) }
        }
        manager.registerAvailabilityCallback(ContextCompat.getMainExecutor(context), callback)
        try {
        ActivityScenario.launch<PairingActivity>(Intent(context, PairingActivity::class.java)).use { scenario ->
            scenario.onActivity {
                it.setTurnScreenOn(true)
                it.window.addFlags(android.view.WindowManager.LayoutParams.FLAG_KEEP_SCREEN_ON)
            }
            compose.onNodeWithText("Pindai QR dari Pengaturan web").performScrollTo().performClick()
            compose.onNodeWithText("Pindai QR").assertIsDisplayed()
            compose.onNodeWithContentDescription("Senter").assertExists()
            compose.waitUntil(15_000) { unavailable.any { it in backCameras } }
            compose.onNodeWithContentDescription("Kembali").performClick()
            compose.waitUntil(10_000) { unavailable.none { it in backCameras } }
            compose.onNodeWithText("Hubungkan ke tracker").assertIsDisplayed()
            compose.onNodeWithText("Masukkan API key manual").performScrollTo().performClick()
            compose.onNodeWithText("Masukkan API key").assertIsDisplayed()
            assertEquals(0, server.requestCount)
        }
        } finally { manager.unregisterAvailabilityCallback(callback) }
    }
}
