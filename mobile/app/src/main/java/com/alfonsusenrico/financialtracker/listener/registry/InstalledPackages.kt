package com.alfonsusenrico.financialtracker.listener.registry

import android.content.Context
import android.content.pm.PackageManager
import android.os.Build

/**
 * Whether a package is installed. From Android 11 the answer covers only packages the manifest's `<queries>`
 * names, so the registered finance apps are declared there instead of asking for every installed package.
 */
class InstalledPackages(private val packageManager: PackageManager) {

    constructor(context: Context) : this(context.packageManager)

    fun isInstalled(packageName: String): Boolean = try {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
            packageManager.getPackageInfo(packageName, PackageManager.PackageInfoFlags.of(0))
        } else {
            @Suppress("DEPRECATION")
            packageManager.getPackageInfo(packageName, 0)
        }
        true
    } catch (_: PackageManager.NameNotFoundException) {
        false
    }

    /** The registered finance apps installed on this phone, as Pengaturan and setup list them. */
    fun financeApps(): List<FinancialSourceRegistry.TargetApp> =
        FinancialSourceRegistry.installedSelectableApps(::isInstalled)
}
