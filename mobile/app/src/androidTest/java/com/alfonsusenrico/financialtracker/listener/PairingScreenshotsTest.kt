package com.alfonsusenrico.financialtracker.listener

import android.graphics.Bitmap
import androidx.compose.runtime.CompositionLocalProvider
import androidx.compose.runtime.mutableStateOf
import androidx.compose.ui.graphics.asAndroidBitmap
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.test.captureToImage
import androidx.compose.ui.test.junit4.createAndroidComposeRule
import androidx.activity.ComponentActivity
import androidx.compose.ui.test.onRoot
import androidx.compose.ui.unit.Density
import androidx.test.platform.app.InstrumentationRegistry
import com.alfonsusenrico.financialtracker.listener.data.pairing.PairingOutcome
import com.alfonsusenrico.financialtracker.listener.ui.pairing.PairingActions
import com.alfonsusenrico.financialtracker.listener.ui.pairing.PairingContent
import com.alfonsusenrico.financialtracker.listener.ui.pairing.PairingScreen
import com.alfonsusenrico.financialtracker.listener.ui.pairing.PairingUiState
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionTheme
import org.junit.Rule
import org.junit.Test
import java.io.File

/** Capture only the app composition; status bar and launcher content never enter the artifact. */
class PairingScreenshotsTest {
    @get:Rule val compose = createAndroidComposeRule<ComponentActivity>()
    @Test fun syntheticPairingScreensInBothThemesAtNormalAndLargeText() {
        compose.activityRule.scenario.onActivity {
            it.setTurnScreenOn(true)
            it.window.addFlags(android.view.WindowManager.LayoutParams.FLAG_KEEP_SCREEN_ON)
        }
        val context = InstrumentationRegistry.getInstrumentation().targetContext
        check(context.packageName.endsWith(".verification"))
        val mode = mutableStateOf(false to 1f)
        val current = mutableStateOf(PairingUiState(PairingScreen.CHOOSE, "Synthetic Realme"))
        val actions = PairingActions({}, {}, {}, {}, {}, {}, {}, {}, {})
        compose.setContent {
            val density = LocalDensity.current.density
            CompositionLocalProvider(LocalDensity provides Density(density, mode.value.second)) {
                CompanionTheme(mode.value.first) { PairingContent(current.value, actions) }
            }
        }
        val states = listOf(
            "listener" to PairingUiState(PairingScreen.LISTENER, "Synthetic Realme"),
            "battery" to PairingUiState(PairingScreen.BATTERY, "Synthetic Realme"),
            "choose" to PairingUiState(PairingScreen.CHOOSE, "Synthetic Realme"),
            "confirm" to PairingUiState(PairingScreen.CONFIRM, "Synthetic Realme", replacing = true),
            "manual" to PairingUiState(PairingScreen.MANUAL, "Synthetic Realme"),
            "expired" to PairingUiState(PairingScreen.RESULT, "Synthetic Realme", outcome = PairingOutcome(false, "Kode QR sudah kedaluwarsa atau dipakai. Buat kode baru di web lalu Pindai lagi.")),
            "retry" to PairingUiState(PairingScreen.RESULT, "Synthetic Realme", outcome = PairingOutcome(false, "Kunci sudah tersimpan. Periksa internet lalu coba Tes Koneksi lagi.", true)),
            "connected" to PairingUiState(PairingScreen.RESULT, "Synthetic Realme", outcome = PairingOutcome(true, "Terhubung. Kunci diterima oleh tracker.", true))
        )
        val output = File(context.filesDir, "pairing-screens").apply { mkdirs() }
        for (dark in listOf(false, true)) for (scale in listOf(1f, 2f)) for ((name, state) in states) {
            compose.runOnIdle { mode.value = dark to scale; current.value = state }
            compose.waitForIdle()
            val image = compose.onRoot().captureToImage().asAndroidBitmap()
            val file = File(output, "${if (dark) "dark" else "light"}-${scale}-$name.png")
            file.outputStream().use { image.compress(Bitmap.CompressFormat.PNG, 100, it) }
        }
    }
}
