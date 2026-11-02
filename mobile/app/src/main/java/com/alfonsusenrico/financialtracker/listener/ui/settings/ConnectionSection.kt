package com.alfonsusenrico.financialtracker.listener.ui.settings

import android.content.Intent
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.outlined.Link
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.semantics.LiveRegionMode
import androidx.compose.ui.semantics.liveRegion
import androidx.compose.ui.semantics.semantics
import com.alfonsusenrico.financialtracker.listener.PairingActivity
import com.alfonsusenrico.financialtracker.listener.data.pairing.DeviceName
import com.alfonsusenrico.financialtracker.listener.data.pairing.PairingOutcome
import com.alfonsusenrico.financialtracker.listener.data.pairing.PairingRepository
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.ui.components.ButtonKind
import com.alfonsusenrico.financialtracker.listener.ui.components.CardTitle
import com.alfonsusenrico.financialtracker.listener.ui.components.CompanionButton
import com.alfonsusenrico.financialtracker.listener.ui.components.CompanionCard
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionTheme
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionType
import kotlinx.coroutines.launch

@Composable
fun ConnectionSection(onSaved: () -> Unit) {
    val context = LocalContext.current
    val scope = rememberCoroutineScope()
    val repository = remember { PairingRepository(context) }
    var testing by remember { mutableStateOf(false) }
    var result by remember { mutableStateOf<PairingOutcome?>(null) }
    val colors = CompanionTheme.colors
    CompanionCard {
        CardTitle("Koneksi", Icons.Outlined.Link)
        Text(DeviceName.detect(context), style = CompanionType.bodyStrong, color = colors.text)
        Text("Nama perangkat · terdeteksi otomatis", style = CompanionType.small, color = colors.text2)
        Text(if (AppPreferences(context).isConfigured) "Kunci API tersimpan" else "Belum dipasangkan", style = CompanionType.small, color = colors.text2)
        CompanionButton("Tes Koneksi", {
            scope.launch {
                testing = true
                try {
                    result = repository.verify()
                    if (result?.connected == true) onSaved()
                } finally { testing = false }
            }
        }, Modifier.fillMaxWidth(), ButtonKind.GHOST, busy = testing, enabled = !testing)
        result?.let { outcome ->
            Text(outcome.message, style = CompanionType.small, color = if (outcome.connected) colors.okFg else colors.danger,
                modifier = Modifier.semantics { liveRegion = LiveRegionMode.Polite })
        }
        CompanionButton(if (AppPreferences(context).isConfigured) "Sambungin ulang" else "Sambungin ke akunmu", {
            context.startActivity(Intent(context, PairingActivity::class.java))
        }, Modifier.fillMaxWidth(), enabled = !testing)
        CompanionButton("Pakai kunci API", {
            context.startActivity(Intent(context, PairingActivity::class.java).putExtra(PairingActivity.EXTRA_MANUAL, true))
        }, Modifier.fillMaxWidth(), ButtonKind.GHOST, enabled = !testing)
        Text(
            "Cadangan kalau QR nggak bisa. Kunci lama baru aku hapus setelah yang baru berhasil masuk ke akun yang sama.",
            style = CompanionType.meta, color = colors.text2
        )
    }
}
