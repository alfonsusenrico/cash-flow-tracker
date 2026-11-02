package com.alfonsusenrico.financialtracker.listener.data.pairing

import java.net.URI
import java.net.URLDecoder

/** A pairing code is a credential: its value must never appear in diagnostic output. */
class PairingCode private constructor(val value: String) {
    override fun toString() = "PairingCode(redacted)"

    companion object {
        fun parse(raw: String, server: String): PairingCode? {
            if (raw.length > 2048) return null
            return try {
                val uri = URI(raw)
                if (uri.isOpaque || uri.scheme != "cashflow" || uri.rawAuthority != "pair" ||
                    !uri.rawPath.isNullOrEmpty() || uri.rawFragment != null) return null
                val parts = uri.rawQuery?.split('&') ?: return null
                if (parts.size != 3) return null
                val parameters = mutableMapOf<String, String>()
                for (part in parts) {
                    val pieces = part.split('=', limit = 2)
                    if (pieces.size != 2) return null
                    val key = URLDecoder.decode(pieces[0], "UTF-8")
                    val value = URLDecoder.decode(pieces[1], "UTF-8")
                    if (parameters.put(key, value) != null) return null
                }
                if (parameters.keys != setOf("v", "server", "code") || parameters["v"] != "1" ||
                    parameters["server"] != server) return null
                val code = parameters["code"] ?: return null
                if (!Regex("[0-9A-HJKMNP-TV-Z]{10}").matches(code)) return null
                PairingCode(code)
            } catch (_: IllegalArgumentException) {
                null
            } catch (_: java.net.URISyntaxException) {
                null
            }
        }
    }
}
