package com.alfonsusenrico.financialtracker.listener.ui.theme

import android.provider.Settings
import androidx.compose.foundation.isSystemInDarkTheme
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.darkColorScheme
import androidx.compose.material3.lightColorScheme
import androidx.compose.runtime.Composable
import androidx.compose.runtime.CompositionLocalProvider
import androidx.compose.runtime.ReadOnlyComposable
import androidx.compose.runtime.remember
import androidx.compose.ui.platform.LocalContext

/** The owner's theme choice under Atur → Tampilan; [code] is the stored preference value. */
enum class ThemeMode(val code: String, val label: String) {
    SYSTEM("system", "Ikuti sistem"),
    LIGHT("light", "Terang"),
    DARK("dark", "Gelap");

    companion object {
        /** Unknown or missing values follow the system, the default before the owner chooses. */
        fun fromCode(code: String?): ThemeMode = entries.firstOrNull { it.code == code } ?: SYSTEM
    }
}

@Composable
fun ThemeMode.isDark(): Boolean = when (this) {
    ThemeMode.SYSTEM -> isSystemInDarkTheme()
    ThemeMode.LIGHT -> false
    ThemeMode.DARK -> true
}

@Composable
fun CompanionTheme(dark: Boolean, content: @Composable () -> Unit) {
    val colors = if (dark) DarkCompanionColors else LightCompanionColors
    val scheme = remember(colors) {
        val base = if (colors.isDark) darkColorScheme() else lightColorScheme()
        base.copy(
            primary = colors.primary, onPrimary = colors.onPrimary,
            secondary = colors.accent, onSecondary = colors.onAccent,
            background = colors.bg, onBackground = colors.text,
            surface = colors.surface, onSurface = colors.text,
            surfaceVariant = colors.surface2, onSurfaceVariant = colors.text2,
            surfaceContainer = colors.surface, surfaceContainerHigh = colors.surface,
            surfaceContainerHighest = colors.surface2, surfaceContainerLow = colors.surface,
            outline = colors.borderStrong, outlineVariant = colors.border,
            error = colors.danger, onError = colors.onDanger,
            inverseSurface = colors.snackbarBg, inverseOnSurface = colors.snackbarFg,
            inversePrimary = colors.snackbarAction
        )
    }
    CompositionLocalProvider(LocalCompanionColors provides colors) {
        MaterialTheme(colorScheme = scheme, typography = CompanionTypography, content = content)
    }
}

object CompanionTheme {
    val colors: CompanionColors
        @Composable @ReadOnlyComposable get() = LocalCompanionColors.current
}

/** True when the owner turned animations off (Settings › Accessibility › Remove animations). */
@Composable
fun rememberReducedMotion(): Boolean {
    val resolver = LocalContext.current.contentResolver
    return remember(resolver) {
        Settings.Global.getFloat(resolver, Settings.Global.ANIMATOR_DURATION_SCALE, 1f) == 0f
    }
}
