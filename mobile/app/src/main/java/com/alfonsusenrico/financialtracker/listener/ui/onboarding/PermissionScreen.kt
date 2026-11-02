package com.alfonsusenrico.financialtracker.listener.ui.onboarding

import android.content.Intent
import android.provider.Settings
import androidx.compose.foundation.background
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.safeDrawingPadding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.outlined.Shield
import androidx.compose.material3.Icon
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.semantics.heading
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.text.style.TextAlign
import androidx.compose.ui.unit.dp
import com.alfonsusenrico.financialtracker.listener.ui.components.ButtonKind
import com.alfonsusenrico.financialtracker.listener.ui.components.CompanionButton
import com.alfonsusenrico.financialtracker.listener.ui.components.CompanionCard
import com.alfonsusenrico.financialtracker.listener.ui.components.SectionLabel
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionTheme
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionType

/** First run without listener access: what is read, then Android's access page, or skip for now. */
@Composable
fun PermissionScreen(
    onContinue: () -> Unit,
    modifier: Modifier = Modifier,
    permissionGranted: Boolean = false,
    onOpenAccess: (() -> Unit)? = null
) {
    val context = LocalContext.current
    val colors = CompanionTheme.colors
    Column(
        modifier = modifier
            .fillMaxWidth(),
        verticalArrangement = Arrangement.spacedBy(14.dp)
    ) {
        Text(
            "Baca notifikasi keuangan", style = CompanionType.onboardingTitle, color = colors.text,
            modifier = Modifier.semantics { heading() }
        )
        Text(
            "Hanya dari aplikasi bank dan dompet digital yang kamu pilih: myBCA, BCA mobile, Bank Jago, GoPay, " +
                "ShopeePay dan Stockbit. Notifikasi lain tidak dibaca dan tidak dikirim.",
            style = CompanionType.body, color = colors.text2
        )
        CompanionCard(spacing = 14.dp) {
            Guarantee(
                "Privasi terfilter di tingkat aplikasi",
                "Hanya aplikasi keuangan terdaftar yang diperiksa. Chat, SMS, dan notifikasi lain langsung dibuang di memori."
            )
            Guarantee(
                "Tersimpan di ponsel dulu",
                "Data tersimpan di basis data lokal ponsel sebelum dikirim ke tracker pribadimu."
            )
        }
        if (permissionGranted) {
            Text("Akses notifikasi aktif", style = CompanionType.body, color = colors.okFg)
        } else {
            CompanionButton(
                "Buka Akses Notifikasi",
                { if (onOpenAccess != null) onOpenAccess() else context.startActivity(Intent(Settings.ACTION_NOTIFICATION_LISTENER_SETTINGS)) },
                Modifier.fillMaxWidth(), ButtonKind.ACCENT
            )
        }
        CompanionButton(if (permissionGranted) "Lanjut" else "Lewati dulu", onContinue, Modifier.fillMaxWidth(), ButtonKind.GHOST)
        Text(
            "Akses ini bisa diaktifkan nanti dari Atur → Pengaturan Android.",
            style = CompanionType.meta, color = colors.text2, textAlign = TextAlign.Start
        )
    }
}

@Composable
private fun Guarantee(title: String, description: String) {
    val colors = CompanionTheme.colors
    Row(horizontalArrangement = Arrangement.spacedBy(10.dp), modifier = Modifier.semantics(mergeDescendants = true) {}) {
        Icon(Icons.Outlined.Shield, contentDescription = null, tint = colors.okFg, modifier = Modifier.padding(top = 2.dp).size(20.dp))
        Column(verticalArrangement = Arrangement.spacedBy(2.dp)) {
            Text(title, style = CompanionType.bodyStrong, color = colors.text)
            Text(description, style = CompanionType.small, color = colors.text2)
        }
    }
}
