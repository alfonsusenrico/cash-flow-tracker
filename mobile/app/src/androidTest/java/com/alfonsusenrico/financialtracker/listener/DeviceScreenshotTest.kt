package com.alfonsusenrico.financialtracker.listener

import android.accessibilityservice.AccessibilityServiceInfo
import android.app.UiAutomation
import android.graphics.Bitmap
import android.graphics.Rect
import android.view.accessibility.AccessibilityNodeInfo
import androidx.compose.ui.semantics.SemanticsActions
import androidx.compose.ui.semantics.SemanticsProperties
import androidx.compose.ui.test.SemanticsMatcher
import androidx.compose.ui.test.hasContentDescription
import androidx.compose.ui.test.hasScrollToNodeAction
import androidx.compose.ui.test.hasText
import androidx.compose.ui.test.junit4.createEmptyComposeRule
import androidx.compose.ui.test.onNodeWithContentDescription
import androidx.compose.ui.test.onNodeWithText
import androidx.compose.ui.test.performClick
import androidx.compose.ui.test.performScrollTo
import androidx.compose.ui.test.performScrollToNode
import androidx.test.core.app.ActivityScenario
import androidx.test.platform.app.InstrumentationRegistry
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.ui.components.DELETE_ACTION_LABEL
import org.junit.Assume.assumeTrue
import org.junit.Rule
import org.junit.Test
import java.io.File

/**
 * Walks every screen of the verification app over the synthetic rows of [SyntheticDeviceSeedTest] and saves
 * a screenshot of each, without the status bar, plus the labels TalkBack reads from the app's accessibility
 * tree (`labels.txt`). Runs only with `-e deviceScreenshots light|dark` in the `.verification` app. Every
 * input goes to the app's own composition through the test framework.
 */
class DeviceScreenshotTest {
    @get:Rule val compose = createEmptyComposeRule()

    private val instrumentation = InstrumentationRegistry.getInstrumentation()
    private val context = instrumentation.targetContext
    private lateinit var folder: File
    private val labels = StringBuilder()
    private val automation by lazy {
        instrumentation.getUiAutomation(UiAutomation.FLAG_DONT_SUPPRESS_ACCESSIBILITY_SERVICES).apply {
            serviceInfo = serviceInfo.apply { flags = flags or AccessibilityServiceInfo.FLAG_RETRIEVE_INTERACTIVE_WINDOWS }
        }
    }

    @Test fun walksEveryScreen() {
        val theme = InstrumentationRegistry.getArguments().getString("deviceScreenshots")
        assumeTrue(theme == "light" || theme == "dark")
        check(context.packageName.endsWith(".verification"))
        val scale = context.resources.configuration.fontScale
        folder = File(context.getExternalFilesDir(null), "screens/$theme-font${"%.1f".format(scale)}").apply { mkdirs() }
        AppPreferences(context).themeMode = theme
        ActivityScenario.launch(MainActivity::class.java).use {
            if (exists(hasText("Lewati & buka aplikasi"))) {
                shot("01-onboarding")
                compose.onNodeWithText("Lewati & buka aplikasi").performScrollTo().performClick()
            }
            waitFor(hasContentDescription("Aktivitas"))
            shot("02-home")
            scrollTo(hasText("berasal dari notifikasi dan catatan di ponsel ini", substring = true))
            shot("03-home-bottom")

            tab("Aktivitas")
            shot("04-activity")
            scrollTo(hasContentDescription("Apotek Sehat", substring = true))
            shot("05-activity-bottom")
            row("Refund Toko Contoh").let { node ->
                val delete = node.fetchSemanticsNode().config[SemanticsActions.CustomActions].single { it.label == DELETE_ACTION_LABEL }
                compose.runOnIdle { delete.action() }
            }
            waitFor(hasText("Urungkan"))
            shot("06-activity-delete-undo")
            compose.onNodeWithText("Urungkan").performClick()
            waitFor(hasContentDescription("Refund Toko Contoh", substring = true))

            filter("Diabaikan, 1")
            shot("07-activity-ignored")
            compose.onNodeWithText("Hapus semua").performClick()
            waitFor(hasText("Hapus semua yang diabaikan?"))
            shot("08-activity-clear-ignored-dialog")
            compose.onNodeWithText("Batal").performClick()
            row("Info pasar").performClick()
            shot("09-detail-ignored")
            back()

            filter("Di ponsel, 1")
            shot("10-activity-on-phone")
            row("Promo GoFood").performClick()
            shot("11-detail-on-phone")
            back()

            filter("Perlu cek, 5")
            row("Pesanan Shopee").performClick()
            shot("12-detail-review")
            compose.onNodeWithText("Catat manual").performClick()
            shot("13-detail-manual-record")
            compose.onNodeWithText("Batal").performScrollTo().performClick()
            back()

            filter("Semua")
            row("Kopi Senja").performClick()
            shot("14-detail-recorded")
            compose.onNodeWithText("Ubah transaksi").performScrollTo().performClick()
            shot("15-detail-edit")
            compose.onNodeWithText("Batal").performScrollTo().performClick()
            back()
            row("Pindah ke Dana Darurat").performClick()
            shot("16-detail-movement")
            back()

            tab("Tanya")
            waitFor(hasText("1 dari 4 menunggu"))
            shot("17-tanya-purpose")
            compose.onNodeWithText("Tulis sendiri").performScrollTo().performClick()
            shot("18-tanya-purpose-write")
            compose.onNodeWithText("Batal").performScrollTo().performClick()
            listOf("19-tanya-sender", "20-tanya-pocket", "21-tanya-mapping").forEach { name ->
                compose.onNodeWithContentDescription("Pertanyaan berikutnya").performScrollTo().performClick()
                compose.waitForIdle()
                shot(name)
            }

            tab("Atur")
            shot("22-atur")
            scrollTo(hasText("Aplikasi yang dipantau"))
            shot("23-atur-apps")
            scrollTo(hasText("Koneksi"))
            shot("24-atur-connection")
            scrollTo(hasText("Tampilan"))
            scrollTo(hasText("Gelap"))
            shot("25-atur-android-and-theme")
        }
        File(folder, "labels.txt").writeText(labels.toString())
    }

    private fun exists(matcher: SemanticsMatcher) = compose.onAllNodes(matcher).fetchSemanticsNodes().isNotEmpty()

    private fun waitFor(matcher: SemanticsMatcher) = compose.waitUntil(10_000) { exists(matcher) }

    private fun tab(label: String) {
        compose.onNode(hasContentDescription(label, substring = true) and SemanticsMatcher.keyIsDefined(SemanticsProperties.Selected))
            .performClick()
        compose.waitForIdle()
    }

    private fun filter(label: String) {
        compose.onNodeWithContentDescription(label).performScrollTo().performClick()
        compose.waitForIdle()
    }

    /** Composed nodes scroll their column; rows of the lazy feed are first brought into the composition. */
    private fun scrollTo(matcher: SemanticsMatcher) {
        if (exists(matcher)) {
            compose.onNode(matcher).performScrollTo()
        } else {
            compose.onNode(hasScrollToNodeAction() and SemanticsMatcher.keyIsDefined(SemanticsProperties.VerticalScrollAxisRange))
                .performScrollToNode(matcher)
        }
        compose.waitForIdle()
    }

    private fun row(title: String) = hasContentDescription("$title,", substring = true).let { matcher ->
        scrollTo(matcher)
        compose.onNode(matcher)
    }

    private fun back() {
        compose.onNodeWithContentDescription("Kembali").performClick()
        compose.waitForIdle()
    }

    /** The whole window below the status bar, so phone notifications never appear in the evidence. */
    private fun shot(name: String) {
        compose.waitForIdle()
        Thread.sleep(400)
        val screen = automation.takeScreenshot() ?: error("No screenshot")
        val id = context.resources.getIdentifier("status_bar_height", "dimen", "android")
        val top = if (id > 0) context.resources.getDimensionPixelSize(id) else 0
        val cropped = Bitmap.createBitmap(screen, 0, top, screen.width, screen.height - top)
        File(folder, "$name.png").outputStream().use { cropped.compress(Bitmap.CompressFormat.PNG, 100, it) }
        recordLabels(name)
    }

    /** What an accessibility service gets from the app's windows: label, text, role and touch height. */
    private fun recordLabels(name: String) {
        val density = context.resources.displayMetrics.density
        labels.append("=== ").append(name).append('\n')
        fun walk(node: AccessibilityNodeInfo) {
            val description = node.contentDescription
            val text = node.text
            if (description != null || text != null) {
                val bounds = Rect().also(node::getBoundsInScreen)
                labels.append(description ?: "-").append(" | ").append(text?.toString()?.replace('\n', ' ') ?: "-")
                    .append(" | ").append(node.className?.toString()?.substringAfterLast('.'))
                    .append(if (node.isClickable) " | clickable" else "").append(if (node.isHeading) " | heading" else "")
                    .append(if (node.isSelected) " | selected" else "")
                    .append(" | ").append((bounds.height() / density).toInt()).append(" dp\n")
            }
            for (i in 0 until node.childCount) node.getChild(i)?.let(::walk)
        }
        automation.windows.mapNotNull { it.root }.filter { it.packageName == context.packageName }.forEach(::walk)
    }
}
