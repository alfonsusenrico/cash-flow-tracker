package com.alfonsusenrico.financialtracker.listener

import com.alfonsusenrico.financialtracker.listener.data.pairing.DeviceName
import com.alfonsusenrico.financialtracker.listener.data.pairing.PairingCode
import org.junit.Assert.*
import org.junit.Test
import java.net.URLEncoder

class PairingCodeTest {
    private val server = "https://tracker.example.invalid"
    private val qr = "cashflow://pair?v=1&server=${URLEncoder.encode(server, "UTF-8")}&code=0123456789"
    @Test fun acceptsOnlyExactServerAndOneOfEachParameter() {
        assertNotNull(PairingCode.parse(qr, server))
        assertNotNull(PairingCode.parse(qr.replace("0123456789", "ABCDEFGHJK"), server))
        val invalid = listOf(
            qr.replace("v=1", "v=2"), qr.replace("v=1", "v=01"), qr + "&extra=1",
            qr + "&code=0123456789", qr + "&v=1", qr + "&server=other", qr + "&%76=1",
            qr.replace("pair?", "pair/?"), qr + "#fragment", qr.replace("pair?", "pair:80?"),
            qr.replace("pair?", "user@pair?"), qr.replace("cashflow:", "https:"),
            qr.replace("0123456789", "abcdefghjk"), qr.replace("0123456789", "012345678I"),
            qr.replace("0123456789", "012345678L"), qr.replace("0123456789", "012345678O"),
            qr.replace("0123456789", "012345678U"), qr.replace("0123456789", "012345678"),
            qr.replace("0123456789", "01234567890"), qr.replace("server=", "server=%"),
            "cashflow:pair?v=1", qr + "&", " ", "x".repeat(2049)
        )
        invalid.forEachIndexed { index, raw -> assertNull("Invalid case $index", PairingCode.parse(raw, server)) }
        assertNull(PairingCode.parse(qr, server + "/"))
        assertNull(PairingCode.parse(qr, "https://foreign.example.invalid"))
        assertFalse(PairingCode.parse(qr, server).toString().contains("0123456789"))
    }
    @Test fun normalizesDetectedNamesWithoutInventingAUserDevice() {
        assertEquals("Desk phone", DeviceName.normalize(" Desk   phone ", "Maker", "Model"))
        assertEquals("TECNO CN7c", DeviceName.normalize(null, "TECNO", "CN7c"))
        assertEquals("TECNO CN7c", DeviceName.normalize("", "tecno", "TECNO CN7c"))
        assertEquals("Android", DeviceName.normalize(null, "", ""))
        assertEquals("Desk phone", DeviceName.normalize("Desk\nphone", "A", "B"))
        assertEquals("phone", DeviceName.normalize("phone\u200b", "A", "B"))
        assertEquals(80, DeviceName.normalize("📱".repeat(90), "A", "B").codePointCount(0, 160))
        assertEquals(80, DeviceName.normalize("x".repeat(100), "A", "B").length)
        assertFalse(DeviceName.normalize("one\u0000two", "A", "B").contains('\u0000'))
    }
}
