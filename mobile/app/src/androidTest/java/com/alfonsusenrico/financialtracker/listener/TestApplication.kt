package com.alfonsusenrico.financialtracker.listener

import android.app.Application
import android.content.Context
import com.alfonsusenrico.financialtracker.listener.data.network.ApiClient
import com.alfonsusenrico.financialtracker.listener.data.network.ApiServiceProvider

/** Synthetic tests can never send a fixture key to the build's real server. */
class TestApplication : Application(), ApiServiceProvider {
    var endpoint = "http://127.0.0.1:1"
    override fun <T> service(type: Class<T>, key: String): T = ApiClient.buildService(endpoint, key, type)
}

object TestEndpoint {
    fun set(context: Context, url: String) {
        require(java.net.URI(url).host in setOf("localhost", "127.0.0.1", "tracker.example.invalid"))
        (context.applicationContext as TestApplication).endpoint = url.trimEnd('/')
    }
}
