package com.alfonsusenrico.financialtracker.listener

import com.alfonsusenrico.financialtracker.listener.registry.FinancialSourceRegistry
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class RegistryTest {

    @Test
    fun testTargetFinancialAppsAccepted() {
        assertTrue(FinancialSourceRegistry.isTargetFinancialApp("id.co.bca.mybca.omni.android"))
        assertTrue(FinancialSourceRegistry.isTargetFinancialApp("com.bca.mybca.omni.android"))
        assertTrue(FinancialSourceRegistry.isTargetFinancialApp("com.bca"))
        assertTrue(FinancialSourceRegistry.isTargetFinancialApp("com.stockbit.android"))
        assertTrue(FinancialSourceRegistry.isTargetFinancialApp("com.jago.digitalBanking"))
        assertTrue(FinancialSourceRegistry.isTargetFinancialApp("com.jago.digitalbanking"))
        assertTrue(FinancialSourceRegistry.isTargetFinancialApp("com.gojek.gopay"))
        assertTrue(FinancialSourceRegistry.isTargetFinancialApp("com.gojek.app"))
        assertTrue(FinancialSourceRegistry.isTargetFinancialApp("com.gopay.wallet"))
        assertTrue(FinancialSourceRegistry.isTargetFinancialApp("com.shopeepay.id"))
        assertTrue(FinancialSourceRegistry.isTargetFinancialApp("com.shopee.id"))
    }

    @Test
    fun testNonFinancialAppsBlocked() {
        assertFalse(FinancialSourceRegistry.isTargetFinancialApp("com.whatsapp"))
        assertFalse(FinancialSourceRegistry.isTargetFinancialApp("org.telegram.messenger"))
        assertFalse(FinancialSourceRegistry.isTargetFinancialApp("com.instagram.android"))
        assertFalse(FinancialSourceRegistry.isTargetFinancialApp("com.google.android.youtube"))
        assertFalse(FinancialSourceRegistry.isTargetFinancialApp("com.bcadigital.blu"))
    }

    @Test
    fun testAppLabels() {
        assertEquals("myBCA", FinancialSourceRegistry.getAppLabel("id.co.bca.mybca.omni.android"))
        assertEquals("BCA mobile", FinancialSourceRegistry.getAppLabel("com.bca"))
        assertEquals("Bank Jago", FinancialSourceRegistry.getAppLabel("com.jago.digitalbanking"))
        assertEquals("GoPay", FinancialSourceRegistry.getAppLabel("com.gojek.app"))
        assertEquals("ShopeePay", FinancialSourceRegistry.getAppLabel("com.shopeepay.id"))
        assertEquals("Shopee", FinancialSourceRegistry.getAppLabel("com.shopee.id"))
        assertEquals("Stockbit", FinancialSourceRegistry.getAppLabel("com.stockbit.android"))
    }

    @Test
    fun testEveryRegisteredPackageIsSelectableOnce() {
        val packages = FinancialSourceRegistry.SELECTABLE_APPS.map { it.packageName.lowercase() }
        assertEquals(packages.distinct(), packages)
        assertEquals(
            setOf(
                "com.bca.mybca.omni.android", "id.co.bca.mybca.omni.android", "com.bca",
                "com.jago.digitalbanking", "com.gojek.gopay", "com.gojek.app", "com.gopay.wallet",
                "com.shopeepay.id", "com.shopee.id", "com.stockbit.android"
            ),
            packages.toSet()
        )
        FinancialSourceRegistry.SELECTABLE_APPS.forEach {
            assertEquals(FinancialSourceRegistry.getAppLabel(it.packageName), it.appLabel)
            assertTrue(it.description.isNotBlank())
        }
    }

    @Test
    fun testSiblingPackagesFollowTheInstitution() {
        assertEquals(
            setOf("com.gojek.app", "com.gopay.wallet"),
            FinancialSourceRegistry.siblingPackages("com.gojek.gopay").toSet()
        )
        assertEquals(
            setOf("com.bca", "id.co.bca.mybca.omni.android"),
            FinancialSourceRegistry.siblingPackages("com.bca.mybca.omni.android").toSet()
        )
        assertEquals(setOf("com.shopee.id"), FinancialSourceRegistry.siblingPackages("com.shopeepay.id").toSet())
        assertTrue(FinancialSourceRegistry.siblingPackages("com.jago.digitalBanking").isEmpty())
        assertTrue(FinancialSourceRegistry.siblingPackages("com.whatsapp").isEmpty())
    }
}
