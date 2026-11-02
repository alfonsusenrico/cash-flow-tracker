package com.alfonsusenrico.financialtracker.listener.ui.pairing

import androidx.camera.core.Camera
import androidx.camera.core.CameraSelector
import androidx.camera.core.ImageAnalysis
import androidx.camera.core.Preview
import androidx.camera.lifecycle.ProcessCameraProvider
import androidx.camera.view.PreviewView
import androidx.compose.foundation.Canvas
import androidx.compose.foundation.background
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.safeDrawingPadding
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.automirrored.outlined.ArrowBack
import androidx.compose.material.icons.outlined.FlashlightOn
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.DisposableEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberUpdatedState
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.geometry.Offset
import androidx.compose.ui.platform.LocalContext
import androidx.lifecycle.compose.LocalLifecycleOwner
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.semantics.stateDescription
import androidx.compose.ui.unit.dp
import androidx.compose.ui.viewinterop.AndroidView
import androidx.core.content.ContextCompat
import com.alfonsusenrico.financialtracker.listener.ui.components.ButtonKind
import com.alfonsusenrico.financialtracker.listener.ui.components.CompanionButton
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionTheme
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionType
import com.google.mlkit.vision.barcode.BarcodeScannerOptions
import com.google.mlkit.vision.barcode.BarcodeScanning
import com.google.mlkit.vision.barcode.common.Barcode
import com.google.mlkit.vision.common.InputImage
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean

@Composable
fun CameraScanner(onCode: (String) -> Unit, onClose: () -> Unit, onManual: () -> Unit, onError: (String) -> Unit) {
    CompanionTheme(dark = true) {
        val colors = CompanionTheme.colors
        var camera by remember { mutableStateOf<Camera?>(null) }
        var torch by remember { mutableStateOf(false) }
        val context = LocalContext.current
        DisposableEffect(camera, torch) {
            var active = true
            val control = camera?.cameraControl
            if (control != null) {
                val future = control.enableTorch(torch)
                future.addListener({
                    if (active) {
                        try {
                            future.get()
                        } catch (_: Exception) {
                            onError("Senter tidak tersedia. Gunakan API key manual atau pindai lagi.")
                        }
                    }
                }, ContextCompat.getMainExecutor(context))
            }
            onDispose { active = false }
        }
        Column(Modifier.fillMaxSize().background(colors.bg).safeDrawingPadding().padding(18.dp), verticalArrangement = Arrangement.spacedBy(16.dp)) {
            Row(Modifier.fillMaxWidth(), verticalAlignment = Alignment.CenterVertically) {
                IconButton(onClick = onClose) { Icon(Icons.AutoMirrored.Outlined.ArrowBack, "Kembali", tint = colors.text) }
                Text("Pindai QR", style = CompanionType.cardTitle, color = colors.text, modifier = Modifier.weight(1f))
                IconButton(onClick = { torch = !torch }, enabled = camera?.cameraInfo?.hasFlashUnit() == true,
                    modifier = Modifier.semantics { stateDescription = if (torch) "Senter aktif" else "Senter mati" }) {
                    Icon(Icons.Outlined.FlashlightOn, "Senter", tint = colors.text)
                }
            }
            Box(Modifier.fillMaxWidth().weight(1f)) {
                CameraPreview(onCode, { camera = it }, onError)
                Canvas(Modifier.fillMaxSize().padding(28.dp)) {
                    val side = size.minDimension * 0.8f
                    val left = (size.width - side) / 2
                    val top = (size.height - side) / 2
                    val length = 28.dp.toPx()
                    val stroke = 3.dp.toPx()
                    for ((x, y, dx, dy) in listOf(
                        listOf(left, top, 1f, 1f), listOf(left + side, top, -1f, 1f),
                        listOf(left, top + side, 1f, -1f), listOf(left + side, top + side, -1f, -1f)
                    )) {
                        drawLine(colors.accent, Offset(x, y), Offset(x + length * dx, y), stroke)
                        drawLine(colors.accent, Offset(x, y), Offset(x, y + length * dy), stroke)
                    }
                }
            }
            Text("Arahkan ke kode QR", style = CompanionType.cardTitle, color = colors.text)
            Text("Buka Tracker Web → Pengaturan → Hubungkan ponsel. Kode berlaku 5 menit dan hanya sekali pakai.", style = CompanionType.body, color = colors.text2)
            CompanionButton("Masukkan API key manual", onManual, Modifier.fillMaxWidth(), ButtonKind.GHOST)
        }
    }
}

@androidx.annotation.OptIn(androidx.camera.core.ExperimentalGetImage::class)
@Composable
private fun CameraPreview(onCode: (String) -> Unit, onCamera: (Camera) -> Unit, onError: (String) -> Unit) {
    val context = LocalContext.current
    val lifecycleOwner = LocalLifecycleOwner.current
    val codeCallback by rememberUpdatedState(onCode)
    val errorCallback by rememberUpdatedState(onError)
    val cameraCallback by rememberUpdatedState(onCamera)
    val previewView = remember { PreviewView(context).apply { implementationMode = PreviewView.ImplementationMode.COMPATIBLE } }
    AndroidView(factory = { previewView }, modifier = Modifier.fillMaxSize())
    DisposableEffect(lifecycleOwner, previewView) {
        val main = ContextCompat.getMainExecutor(context)
        val executor = Executors.newSingleThreadExecutor()
        val scanner = BarcodeScanning.getClient(BarcodeScannerOptions.Builder().setBarcodeFormats(Barcode.FORMAT_QR_CODE).build())
        val delivered = AtomicBoolean(false)
        val future = ProcessCameraProvider.getInstance(context)
        var disposed = false
        var provider: ProcessCameraProvider? = null
        val preview = Preview.Builder().build().also { it.setSurfaceProvider(previewView.surfaceProvider) }
        val analysis = ImageAnalysis.Builder().setBackpressureStrategy(ImageAnalysis.STRATEGY_KEEP_ONLY_LATEST).build()
        analysis.setAnalyzer(executor) { frame ->
            val image = frame.image
            if (image == null || delivered.get()) {
                frame.close()
            } else {
                scanner.process(InputImage.fromMediaImage(image, frame.imageInfo.rotationDegrees))
                    .addOnSuccessListener(main) { results ->
                        val raw = results.firstOrNull()?.rawValue
                        if (!disposed && raw != null && delivered.compareAndSet(false, true)) codeCallback(raw)
                    }
                    .addOnFailureListener(main) {
                        if (!disposed && delivered.compareAndSet(false, true)) errorCallback("Kode belum bisa dibaca. Coba pindai lagi atau masukkan API key manual.")
                    }
                    .addOnCompleteListener { frame.close() }
            }
        }
        future.addListener({
            if (!disposed) {
                try {
                    provider = future.get()
                    val camera = provider!!.bindToLifecycle(lifecycleOwner, CameraSelector.DEFAULT_BACK_CAMERA, preview, analysis)
                    cameraCallback(camera)
                } catch (_: Exception) {
                    errorCallback("Kamera tidak tersedia. Masukkan API key manual atau coba pindai lagi.")
                }
            }
        }, main)
        onDispose {
            disposed = true
            analysis.clearAnalyzer()
            provider?.unbind(preview, analysis)
            scanner.close()
            executor.shutdown()
        }
    }
}
