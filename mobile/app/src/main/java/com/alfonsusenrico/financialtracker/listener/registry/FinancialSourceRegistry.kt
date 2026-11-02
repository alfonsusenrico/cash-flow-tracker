package com.alfonsusenrico.financialtracker.listener.registry

/**
 * Registry defining supported banking and e-wallet applications.
 * Acts as the strict privacy firewall: notifications from packages NOT in this registry
 * are dropped immediately before any body text or extras are inspected.
 */
object FinancialSourceRegistry {

    /**
     * [institution] mirrors the backend's SUPPORTED_PACKAGES mapping. Apps of one institution are
     * fed by the same payment event, so identical text from two of them is one transaction.
     */
    data class TargetApp(
        val packageName: String,
        val appLabel: String,
        val description: String,
        val institution: String,
        val defaultColor: Long // Compose Color hex (0xFF...)
    )

    private val APPS_LIST = listOf(
        TargetApp("com.bca.mybca.omni.android", "myBCA", "Aplikasi modern BCA (rekomendasi)", "bca", 0xFF0060AF),
        TargetApp("id.co.bca.mybca.omni.android", "myBCA", "myBCA, varian paket id.co.bca", "bca", 0xFF0060AF),
        TargetApp("com.bca", "BCA mobile", "Aplikasi BCA mobile klasik", "bca", 0xFF003882),
        // Bank Jago has shipped under both spellings; lookups are case-insensitive.
        TargetApp("com.jago.digitalBanking", "Bank Jago", "Kantong & transaksi Bank Jago", "jago", 0xFFFF7A00),
        TargetApp("com.jago.digitalbanking", "Bank Jago", "Kantong & transaksi Bank Jago", "jago", 0xFFFF7A00),
        TargetApp("com.gojek.gopay", "GoPay", "Aplikasi GoPay: transfer, QRIS & Tabungan by Jago", "gopay", 0xFF00AA13),
        TargetApp("com.gojek.app", "Gojek", "GoPay di aplikasi Gojek", "gopay", 0xFF00AA13),
        TargetApp("com.gopay.wallet", "GoPay", "GoPay, varian paket com.gopay.wallet", "gopay", 0xFF00AA13),
        TargetApp("com.shopeepay.id", "ShopeePay", "Dompet digital ShopeePay standalone", "shopeepay", 0xFFEE4D2D),
        TargetApp("com.shopee.id", "Shopee", "ShopeePay di aplikasi utama Shopee", "shopeepay", 0xFFEE4D2D),
        TargetApp("com.stockbit.android", "Stockbit", "Transaksi saham & RDN Stockbit", "stockbit", 0xFF00C076)
    )

    // Lowercase lookup map for case-insensitive matching
    private val APPS_MAP = APPS_LIST.associateBy { it.packageName.lowercase() }

    /** Every registered package once (case variants share one toggle), in registry order. */
    val SELECTABLE_APPS: List<TargetApp> = APPS_LIST.distinctBy { it.packageName.lowercase() }

    /** Every registered spelling; the manifest's package-visibility queries must name each one. */
    val REGISTERED_PACKAGES: List<String> = APPS_LIST.map { it.packageName }

    /**
     * The selectable apps installed on this phone, in registry order. An entry with case variants counts as
     * installed when any spelling is. Only what is shown depends on this; capture matching never does, so an
     * app installed later is captured by its saved toggle or default.
     */
    fun installedSelectableApps(isInstalled: (String) -> Boolean): List<TargetApp> =
        SELECTABLE_APPS.filter { selectable ->
            APPS_LIST.any { it.packageName.equals(selectable.packageName, ignoreCase = true) && isInstalled(it.packageName) }
        }

    fun isTargetFinancialApp(packageName: String): Boolean {
        return APPS_MAP.containsKey(packageName.lowercase())
    }

    fun getAppLabel(packageName: String): String {
        return APPS_MAP[packageName.lowercase()]?.appLabel ?: packageName
    }

    fun getAppColor(packageName: String): Long {
        return APPS_MAP[packageName.lowercase()]?.defaultColor ?: 0xFF6B7280
    }

    /** Lowercase packages of the same institution, excluding [packageName] and its case variants. */
    fun siblingPackages(packageName: String): List<String> {
        val lower = packageName.lowercase()
        val institution = APPS_MAP[lower]?.institution ?: return emptyList()
        return APPS_MAP.values.filter { it.institution == institution }
            .map { it.packageName.lowercase() }
            .filter { it != lower }
            .distinct()
    }
}
