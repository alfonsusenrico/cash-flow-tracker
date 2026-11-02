package com.alfonsusenrico.financialtracker.listener.service

import com.alfonsusenrico.financialtracker.listener.data.health.SelfTestMarker
import com.alfonsusenrico.financialtracker.listener.registry.FinancialSourceRegistry

/**
 * The first decision about every posted notification, before any of its text is read for capture: this app's
 * own self-test, an app the owner does not want captured, or a capture.
 */
object NotificationGate {
    sealed interface Route {
        /** The self-test's marked notification: answered to the waiting test, never captured. */
        data class SelfTest(val token: String) : Route
        data object Drop : Route
        data object Capture : Route
    }

    fun route(
        packageName: String?,
        ownPackage: String,
        tag: String?,
        selfTestToken: String?,
        isEnabled: (String) -> Boolean
    ): Route {
        SelfTestMarker.token(packageName, ownPackage, tag, selfTestToken)?.let { return Route.SelfTest(it) }
        if (packageName == null || !FinancialSourceRegistry.isTargetFinancialApp(packageName)) return Route.Drop
        return if (isEnabled(packageName)) Route.Capture else Route.Drop
    }
}
