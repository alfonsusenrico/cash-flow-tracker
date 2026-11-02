package com.alfonsusenrico.financialtracker.listener.data.network

import android.content.Context
import com.alfonsusenrico.financialtracker.listener.BuildConfig
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import okhttp3.OkHttpClient
import retrofit2.Retrofit
import retrofit2.converter.gson.GsonConverterFactory
import java.io.IOException
import java.util.concurrent.TimeUnit

/** Test applications supply isolated transport without changing the shipped server setting. */
interface ApiServiceProvider {
    fun <T> service(type: Class<T>, key: String): T
}

class ApiClient(private val context: Context) {
    private val prefs = AppPreferences(context)

    fun createService(expectedPairing: String? = null): IngestionApiService {
        if (expectedPairing != null && expectedPairing != prefs.pairingContext) throw IOException("pairing_changed")
        val key = prefs.apiKey
        if (expectedPairing != null && expectedPairing != prefs.pairingContext) throw IOException("pairing_changed")
        return service(IngestionApiService::class.java, key)
    }

    fun pairingService(key: String = ""): PairingApiService = service(PairingApiService::class.java, key)

    private fun <T> service(type: Class<T>, key: String): T =
        (context.applicationContext as? ApiServiceProvider)?.service(type, key)
            ?: buildService(prefs.serverUrl, key, type)

    companion object {
        fun <T> buildService(server: String, key: String, type: Class<T>): T {
            val client = OkHttpClient.Builder()
                // Never forward a pairing code, key or financial payload through redirects.
                .followRedirects(false).followSslRedirects(false)
                .retryOnConnectionFailure(type != PairingApiService::class.java)
                .addInterceptor { chain ->
                    val request = chain.request().newBuilder()
                        .header("Accept", "application/json")
                        .header("Content-Type", "application/json")
                        .header("X-Companion-Version", BuildConfig.VERSION_NAME)
                    if (key.isNotBlank()) request.header("Authorization", "Bearer $key")
                    chain.proceed(request.build())
                }
                .connectTimeout(15, TimeUnit.SECONDS)
                .readTimeout(30, TimeUnit.SECONDS)
                .writeTimeout(30, TimeUnit.SECONDS)
                .build()
            return Retrofit.Builder().baseUrl(server.trimEnd('/') + "/").client(client)
                .addConverterFactory(GsonConverterFactory.create()).build().create(type)
        }
    }
}
