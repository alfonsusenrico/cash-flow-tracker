package com.alfonsusenrico.financialtracker.listener.data.network

import com.alfonsusenrico.financialtracker.listener.ui.components.ProcessingReasons
import com.google.gson.JsonParser
import retrofit2.Response

/** Human-readable message from a FastAPI error body: `detail` text, `detail.message`, or `detail.code`. */
fun serverMessage(response: Response<*>): String {
    val detail = try {
        JsonParser.parseString(response.errorBody()?.string().orEmpty()).asJsonObject.get("detail")
    } catch (_: Exception) {
        null
    }
    val message = when {
        detail == null || detail.isJsonNull -> null
        detail.isJsonPrimitive -> detail.asString
        detail.isJsonObject -> {
            val body = detail.asJsonObject
            body.get("message")?.takeIf { it.isJsonPrimitive }?.asString
                ?: body.get("code")?.takeIf { it.isJsonPrimitive }?.asString?.let(ProcessingReasons::describe)
        }
        else -> null
    }
    return message ?: "HTTP ${response.code()}"
}

/** The machine code of a FastAPI error body (`detail.code`), or null when the body carries none. */
fun serverErrorCode(response: Response<*>): String? = try {
    val detail = JsonParser.parseString(response.errorBody()?.string().orEmpty()).asJsonObject.get("detail")
    detail?.takeIf { it.isJsonObject }?.asJsonObject?.get("code")?.takeIf { it.isJsonPrimitive }?.asString
} catch (_: Exception) {
    null
}
