package com.alfonsusenrico.financialtracker.listener

import android.content.Context
import android.graphics.BitmapFactory
import androidx.test.platform.app.InstrumentationRegistry
import com.alfonsusenrico.financialtracker.listener.data.pairing.PairingCode
import com.alfonsusenrico.financialtracker.listener.data.pairing.PairingRepository
import com.alfonsusenrico.financialtracker.listener.data.network.ApiClient
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.google.gson.JsonParser
import com.google.mlkit.vision.barcode.BarcodeScannerOptions
import com.google.mlkit.vision.barcode.BarcodeScanning
import com.google.mlkit.vision.barcode.common.Barcode
import com.google.mlkit.vision.common.InputImage
import com.google.android.gms.tasks.Tasks
import kotlinx.coroutines.runBlocking
import okhttp3.Cookie
import okhttp3.CookieJar
import okhttp3.HttpUrl
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.junit.Assert.*
import org.junit.Assume.assumeTrue
import org.junit.Test
import java.util.concurrent.TimeUnit

class PairingBackendIntegrationTest {
    private val context get() = InstrumentationRegistry.getInstrumentation().targetContext
    private val base = "http://127.0.0.1:18000"

    @Test fun pairAuthenticateRePairAndRevokeAgainstLocalBackend(): Unit = runBlocking {
        assumeTrue(InstrumentationRegistry.getArguments().getString("localBackend") == "true")
        check(context.packageName.endsWith(".verification"))
        check(BuildConfig.SERVER_URL == base)
        TestEndpoint.set(context, base)
        val prefs = AppPreferences(context)
        prefs.isAutoSyncEnabled = false
        prefs.apiKey = ""
        val suffix = System.nanoTime().toString()
        prefs.deviceId = "synthetic-pairing-$suffix"
        val cookies = mutableListOf<Cookie>()
        val web = OkHttpClient.Builder().cookieJar(object : CookieJar {
            override fun saveFromResponse(url: HttpUrl, values: List<Cookie>) { cookies.clear(); cookies.addAll(values) }
            override fun loadForRequest(url: HttpUrl) = cookies.toList()
        }).build()
        fun post(path: String, body: String): com.google.gson.JsonObject {
            val request = Request.Builder().url(base + path).header("Origin", base)
                .post(body.toRequestBody("application/json".toMediaType())).build()
            web.newCall(request).execute().use { response ->
                assertTrue("Local fixture endpoint status ${response.code}", response.isSuccessful)
                return JsonParser.parseString(response.body!!.string()).asJsonObject
            }
        }
        val registration = post("/api/auth/register", """{"username":"pairing-$suffix","password":"synthetic-local-password","invite_code":"SYNTHETIC-PAIRING"}""")
        val manualKey = registration["user"].asJsonObject["api_key"].asString
        fun qr(): PairingCode {
            val payload = post("/api/devices/pairing-codes", "{}")["qr_payload"].asString
            return PairingCode.parse(payload, BuildConfig.SERVER_URL) ?: error("Local QR rejected")
        }
        try {
            val repository = PairingRepository(context)
            assertTrue(repository.exchange(qr(), "Synthetic Realme").connected)
            val firstKey = prefs.apiKey
            val firstContext = prefs.pairingContext
            assertTrue(repository.verify().connected)
            assertTrue(repository.exchange(qr(), "Synthetic Realme").connected)
            assertTrue(prefs.apiKey != firstKey)
            assertNotEquals(firstContext, prefs.pairingContext)
            assertEquals(401, ApiClient(context).pairingService(firstKey).identity().code())
            val service = ApiClient(context).pairingService(prefs.apiKey)
            val devices = service.devices().body()!!.devices!!
            // Registration also issues a manual key; only the phone's paired key is revoked here.
            assertEquals(2, devices.size)
            val paired = devices.single { it.prefix == prefs.apiKey.take(8) }
            assertTrue(service.revoke(paired.id!!).isSuccessful)
            assertEquals(401, service.identity().code())
            assertFalse(repository.verify().connected)
            assertTrue(ApiClient(context).pairingService(manualKey).identity().isSuccessful)
        } finally { prefs.apiKey = "" }
    }

    @Test fun bundledReaderDecodesTheWebQrFixtureWithoutAModelDownload() {
        check(context.packageName.endsWith(".verification"))
        val assets = InstrumentationRegistry.getInstrumentation().context.assets
        val bitmap = assets.open("synthetic-web-pairing-qr.png").use(BitmapFactory::decodeStream)
        val scanner = BarcodeScanning.getClient(BarcodeScannerOptions.Builder().setBarcodeFormats(Barcode.FORMAT_QR_CODE).build())
        try {
            val results = Tasks.await(scanner.process(InputImage.fromBitmap(bitmap, 0)), 20, TimeUnit.SECONDS)
            val expected = "cashflow://pair?v=1&server=https%3A%2F%2Ftracker.example.invalid&code=0123456789"
            assertTrue(results.any { it.rawValue == expected })
            assertNotNull(PairingCode.parse(expected, "https://tracker.example.invalid"))
        } finally { scanner.close(); bitmap.recycle() }
    }
}
