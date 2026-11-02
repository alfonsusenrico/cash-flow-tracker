package com.alfonsusenrico.financialtracker.listener.data.pairing

import android.content.Context
import android.os.Build
import android.provider.Settings

object DeviceName {
    fun detect(context: Context): String {
        val system = try {
            Settings.Global.getString(context.contentResolver, Settings.Global.DEVICE_NAME)
        } catch (_: SecurityException) {
            null
        }
        return normalize(system, Build.MANUFACTURER, Build.MODEL)
    }

    fun normalize(system: String?, manufacturer: String, model: String): String {
        fun clean(value: String): String = buildString {
            value.codePoints().forEach { point ->
                if (Character.isWhitespace(point) || Character.isSpaceChar(point)) {
                    append(' ')
                } else if (Character.getType(point) !in setOf(
                    Character.CONTROL.toInt(), Character.FORMAT.toInt(), Character.SURROGATE.toInt(),
                    Character.PRIVATE_USE.toInt(), Character.UNASSIGNED.toInt()
                )) {
                    appendCodePoint(point)
                }
            }
        }.trim().replace(Regex(" +"), " ")
        val maker = clean(manufacturer)
        val hardware = clean(model)
        val fallback = if (hardware.startsWith(maker, ignoreCase = true)) hardware else "$maker $hardware".trim()
        val result = clean(system.orEmpty()).ifBlank { fallback }.ifBlank { "Android" }
        val points = result.codePoints().limit(80).toArray()
        return String(points, 0, points.size)
    }
}
