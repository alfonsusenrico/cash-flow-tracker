package com.alfonsusenrico.financialtracker.listener.registry

import android.content.Context
import android.content.pm.ApplicationInfo
import android.content.pm.PackageInfo
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import com.alfonsusenrico.financialtracker.listener.TestApplication
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.service.NotificationGate
import java.io.File
import javax.xml.parsers.DocumentBuilderFactory
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.Shadows.shadowOf
import org.robolectric.annotation.Config

@RunWith(AndroidJUnit4::class)
@Config(sdk = [34], application = TestApplication::class)
class InstalledPackagesTest {
    private val context: Context = ApplicationProvider.getApplicationContext()
    private val installed = InstalledPackages(context)

    private fun install(packageName: String) {
        shadowOf(context.packageManager).installPackage(PackageInfo().apply {
            this.packageName = packageName
            applicationInfo = ApplicationInfo().apply { this.packageName = packageName }
        })
    }

    @Test fun onlyInstalledFinanceAppsAreListed() {
        install("com.gojek.app")
        install("com.jago.digitalBanking")
        install("com.whatsapp")
        assertTrue(installed.isInstalled("com.gojek.app"))
        assertFalse(installed.isInstalled("com.gojek.gopay"))
        assertEquals(listOf("Bank Jago", "Gojek"), installed.financeApps().map { it.appLabel })
    }

    @Test fun noFinanceAppInstalledListsNothing() {
        install("com.whatsapp")
        assertTrue(installed.financeApps().isEmpty())
    }

    @Test fun anAppInstalledLaterIsListedOnTheNextRead() {
        assertTrue(installed.financeApps().isEmpty())
        install("com.stockbit.android")
        assertEquals(listOf("com.stockbit.android"), installed.financeApps().map { it.packageName })
    }

    @Test fun captureFollowsTheSavedToggleWhateverIsInstalled() {
        val prefs = AppPreferences(context)
        val own = context.packageName
        fun route(packageName: String) = NotificationGate.route(packageName, own, null, null, prefs::isPackageEnabled)

        assertTrue(installed.financeApps().isEmpty())
        val defaultOff = setOf("com.bca", "com.shopee.id")
        FinancialSourceRegistry.REGISTERED_PACKAGES.forEach {
            val expected = if (it in defaultOff) NotificationGate.Route.Drop else NotificationGate.Route.Capture
            assertEquals(it, expected, route(it))
        }

        prefs.setPackageEnabled("com.gojek.app", false)
        install("com.gojek.app")
        assertEquals(NotificationGate.Route.Drop, route("com.gojek.app"))
        assertEquals(NotificationGate.Route.Capture, route("com.gojek.gopay"))
        assertEquals(NotificationGate.Route.Drop, route("com.whatsapp"))
    }

    /** Android 11+ hides every package the manifest does not query, so a missing entry would hide that app. */
    @Test fun manifestQueriesNameEveryRegisteredPackage() {
        val manifest = DocumentBuilderFactory.newInstance().apply { isNamespaceAware = true }
            .newDocumentBuilder().parse(File("src/main/AndroidManifest.xml"))
        val queries = manifest.getElementsByTagName("queries")
        assertEquals(1, queries.length)
        val packages = manifest.getElementsByTagName("package")
        val queried = (0 until packages.length).map {
            packages.item(it).attributes.getNamedItemNS("http://schemas.android.com/apk/res/android", "name").nodeValue
        }
        assertEquals(FinancialSourceRegistry.REGISTERED_PACKAGES.sorted(), queried.sorted())
        assertTrue(manifest.getElementsByTagName("uses-permission").let { permissions ->
            (0 until permissions.length).none {
                permissions.item(it).attributes.getNamedItemNS("http://schemas.android.com/apk/res/android", "name")
                    .nodeValue == "android.permission.QUERY_ALL_PACKAGES"
            }
        })
    }
}
