package com.alfonsusenrico.financialtracker.listener

import com.alfonsusenrico.financialtracker.listener.service.NotificationProcessor
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotEquals
import org.junit.Test

class PayloadHashTest {

    @Test
    fun testDeterministicPayloadHash() {
        val hash1 = NotificationProcessor.computePayloadHash(
            packageName = "id.co.bca.mybca.omni.android",
            title = "Transaksi Berhasil",
            bodyText = "QRIS Rp 50.000 ke Kopi Kenangan",
            postTime = 1726300000000L
        )

        val hash2 = NotificationProcessor.computePayloadHash(
            packageName = "id.co.bca.mybca.omni.android",
            title = "Transaksi Berhasil",
            bodyText = "QRIS Rp 50.000 ke Kopi Kenangan",
            postTime = 1726300000000L
        )

        assertEquals(64, hash1.length)
        assertEquals(hash1, hash2)
    }

    @Test
    fun testFormulaStaysCompatibleWithStoredBackendHashes() {
        // The backend deduplicates on payload_hash, so SHA-256(package|title|text|postTime) must not change.
        val hash = NotificationProcessor.computePayloadHash(
            packageName = "com.gojek.gopay",
            title = "Pembayaran berhasil",
            bodyText = "Kamu baru aja bayar Rp45.000 ke Contoh pake GoPay",
            postTime = 1726300000000L
        )
        assertEquals("04268950e8161d68ff2e8dd2f47560cf04939fc5df77d70b71d1f79dd028c4e9", hash)
    }

    @Test
    fun testDifferingPayloadProducesDifferentHash() {
        val hash1 = NotificationProcessor.computePayloadHash(
            packageName = "com.jago.digitalbanking",
            title = "Kantong Utama berkurang",
            bodyText = "Rp 25.000",
            postTime = 1726300000000L
        )

        val hash2 = NotificationProcessor.computePayloadHash(
            packageName = "com.jago.digitalbanking",
            title = "Kantong Utama berkurang",
            bodyText = "Rp 35.000",
            postTime = 1726300000000L
        )

        assertNotEquals(hash1, hash2)
    }
}
