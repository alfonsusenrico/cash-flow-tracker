package com.alfonsusenrico.financialtracker.listener

import com.alfonsusenrico.financialtracker.listener.service.NotificationProcessor
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class NotificationProcessorTest {

    @Test
    fun testBankJagoSinglePocketOut() {
        val summary = NotificationProcessor.extractDetectionSummary(
            packageName = "com.jago.digitalBanking",
            title = "Jago",
            bodyText = "You've moved Rp500.000 out of your My Emergency Fund Pocket. Need help? Contact Tanya Jago at 1500 746.",
            bigText = "You've moved Rp500.000 out of your My Emergency Fund Pocket. Need help? Contact Tanya Jago at 1500 746."
        )

        assertTrue(summary.isFinancial)
        assertEquals("transfer", summary.eventClass)
        assertEquals(500000.0, summary.expectedAmount ?: 0.0, 0.01)
        assertEquals("internal", summary.expectedDirection)
        assertEquals("My Emergency Fund → Kantong Utama", summary.expectedCounterparty)
        assertEquals("My Emergency Fund", summary.sourceAccount)
        assertEquals("Bank Jago", summary.targetAccount)
        assertEquals("Internal Movement", summary.categoryName)
    }

    @Test
    fun testBankJagoSinglePocketIn() {
        val summary = NotificationProcessor.extractDetectionSummary(
            packageName = "com.jago.digitalBanking",
            title = "Jago",
            bodyText = "You've moved Rp250.000 into your Tabungan Pocket.",
            bigText = "You've moved Rp250.000 into your Tabungan Pocket."
        )

        assertTrue(summary.isFinancial)
        assertEquals("transfer", summary.eventClass)
        assertEquals(250000.0, summary.expectedAmount ?: 0.0, 0.01)
        assertEquals("internal", summary.expectedDirection)
        assertEquals("Kantong Utama → Tabungan", summary.expectedCounterparty)
        assertEquals("Bank Jago", summary.sourceAccount)
        assertEquals("Tabungan", summary.targetAccount)
        assertEquals("Internal Movement", summary.categoryName)
    }

    @Test
    fun testBankJagoDualPocketMovement() {
        val summary = NotificationProcessor.extractDetectionSummary(
            packageName = "com.jago.digitalBanking",
            title = "Jago",
            bodyText = "Rp500.000 has been moved from your Main Pocket Pocket to your GoPay Tabungan Pocket.",
            bigText = "Rp500.000 has been moved from your Main Pocket Pocket to your GoPay Tabungan Pocket."
        )

        assertTrue(summary.isFinancial)
        assertEquals("transfer", summary.eventClass)
        assertEquals(500000.0, summary.expectedAmount ?: 0.0, 0.01)
        assertEquals("internal", summary.expectedDirection)
        assertEquals("Main Pocket → GoPay Tabungan", summary.expectedCounterparty)
        assertEquals("Main Pocket", summary.sourceAccount)
        assertEquals("GoPay Tabungan", summary.targetAccount)
        assertEquals("Internal Movement", summary.categoryName)
    }

    @Test
    fun testBankJagoSinglePocketOutIndonesian() {
        val summary = NotificationProcessor.extractDetectionSummary(
            packageName = "com.jago.digitalBanking",
            title = "Jago",
            bodyText = "Kamu telah memindahkan Rp500.000 keluar dari Dana Darurat Kantong.",
            bigText = null
        )

        assertTrue(summary.isFinancial)
        assertEquals("transfer", summary.eventClass)
        assertEquals(500000.0, summary.expectedAmount ?: 0.0, 0.01)
        assertEquals("Dana Darurat → Kantong Utama", summary.expectedCounterparty)
        assertEquals("Dana Darurat", summary.sourceAccount)
        assertEquals("Bank Jago", summary.targetAccount)
        assertEquals("Internal Movement", summary.categoryName)
    }

    @Test
    fun testStockbitTradeBuy() {
        val summary = NotificationProcessor.extractDetectionSummary(
            packageName = "com.stockbit.android",
            title = "Stockbit",
            bodyText = "Pembelian 10 lot BBCA match di harga Rp9.500",
            bigText = null
        )

        assertTrue(summary.isFinancial)
        assertEquals("transfer", summary.eventClass)
        assertEquals(9500000.0, summary.expectedAmount ?: 0.0, 0.01)
        assertEquals("RDN BCA", summary.sourceAccount)
        assertEquals("BBCA", summary.targetAccount)
        assertEquals("Investasi", summary.categoryName)
        assertEquals("buy", summary.investmentAction)
        assertEquals(1000.0, summary.units ?: 0.0, 0.01)
        assertEquals(9500.0, summary.pricePerUnit ?: 0.0, 0.01)
    }

    @Test
    fun testGopayTabunganInbound() {
        val summary = NotificationProcessor.extractDetectionSummary(
            packageName = "com.gojek.gopay",
            title = "GoPay",
            bodyText = "Anda telah menerima Tabungan by Jago sebanyak Rp150.000.",
            bigText = null
        )

        assertTrue(summary.isFinancial)
        assertEquals("income", summary.eventClass)
        assertEquals(150000.0, summary.expectedAmount ?: 0.0, 0.01)
        assertEquals("in", summary.expectedDirection)
        assertEquals("GoPay", summary.sourceAccount)
        assertEquals("Internal Movement", summary.categoryName)
    }

    @Test
    fun testNoiseRejection() {
        val summary = NotificationProcessor.extractDetectionSummary(
            packageName = "com.gojek.gopay",
            title = "Hadiah miliaran, cek!",
            bodyText = "Transferan ke sesama GoPay buat dapetin puzzle & klaim hadiah miliaran. Yuk transferan💸",
            bigText = null
        )

        assertFalse(summary.isFinancial)
    }

    @Test
    fun testBcaInbound() {
        val summary = NotificationProcessor.extractDetectionSummary(
            packageName = "com.bca",
            title = "BCA mobile",
            bodyText = "Pemasukan sebesar Rp 1.500.000 dari PT TEKNOLOGI KARYA di kategori Gaji.",
            bigText = null
        )

        assertTrue(summary.isFinancial)
        assertEquals("income", summary.eventClass)
        assertEquals(1500000.0, summary.expectedAmount ?: 0.0, 0.01)
        assertEquals("in", summary.expectedDirection)
        assertEquals("PT TEKNOLOGI KARYA", summary.expectedCounterparty)
        assertEquals("BCA", summary.sourceAccount)
        assertEquals("Gaji", summary.categoryName)
    }

    @Test
    fun testBcaInboundSalaryWithoutSender() {
        val summary = NotificationProcessor.extractDetectionSummary(
            packageName = "com.bca.mybca.omni.android",
            title = "Financial Diary",
            bodyText = "You received IDR 7,250,000.00 at Salary category.",
            bigText = "You received IDR 7,250,000.00 at Salary category."
        )

        assertTrue(summary.isFinancial)
        assertEquals("income", summary.eventClass)
        assertEquals(7250000.0, summary.expectedAmount ?: 0.0, 0.01)
        assertEquals("in", summary.expectedDirection)
        assertEquals("BCA", summary.sourceAccount)
        assertEquals("Salary", summary.categoryName)
    }

    @Test
    fun testBcaInboundIndonesianWithoutSender() {
        val summary = NotificationProcessor.extractDetectionSummary(
            packageName = "id.co.bca.mybca.omni.android",
            title = "Financial Diary",
            bodyText = "Pemasukan sebesar IDR 7,250,000.00 di kategori Gaji.",
            bigText = null
        )

        assertTrue(summary.isFinancial)
        assertEquals("income", summary.eventClass)
        assertEquals(7250000.0, summary.expectedAmount ?: 0.0, 0.01)
        assertEquals("in", summary.expectedDirection)
        assertEquals("BCA", summary.sourceAccount)
        assertEquals("Gaji", summary.categoryName)
    }

    @Test
    fun testBcaOutbound() {
        val summary = NotificationProcessor.extractDetectionSummary(
            packageName = "com.bca",
            title = "BCA mobile",
            bodyText = "Pengeluaran sebesar Rp 75.000 di kategori Makanan.",
            bigText = null
        )

        assertTrue(summary.isFinancial)
        assertEquals("expense", summary.eventClass)
        assertEquals(750000.0, (summary.expectedAmount ?: 0.0) * 10, 0.01)
        assertEquals(75000.0, summary.expectedAmount ?: 0.0, 0.01)
        assertEquals("out", summary.expectedDirection)
        assertEquals("BCA", summary.sourceAccount)
        assertEquals("Makanan", summary.categoryName)
    }

    @Test
    fun testBankJagoPocketMovementWithoutYour() {
        val summary = NotificationProcessor.extractDetectionSummary(
            packageName = "com.jago.digitalBanking",
            title = "Jago",
            bodyText = "You've moved Rp350.000 from My Emergency Fund Pocket to your Subscriptions Pocket. Click here to see how to add money to Jago. Need help? Contact Tanya Jago at 1500 746.",
            bigText = null
        )

        assertTrue(summary.isFinancial)
        assertEquals("transfer", summary.eventClass)
        assertEquals(350000.0, summary.expectedAmount ?: 0.0, 0.01)
        assertEquals("internal", summary.expectedDirection)
        assertEquals("My Emergency Fund → Subscriptions", summary.expectedCounterparty)
        assertEquals("My Emergency Fund", summary.sourceAccount)
        assertEquals("Subscriptions", summary.targetAccount)
    }

    @Test
    fun testBankJagoPocketMovementReturn() {
        val summary = NotificationProcessor.extractDetectionSummary(
            packageName = "com.jago.digitalBanking",
            title = "Jago",
            bodyText = "You've moved Rp22.292 from Subscriptions Pocket to your My Emergency Fund Pocket.",
            bigText = null
        )

        assertTrue(summary.isFinancial)
        assertEquals("transfer", summary.eventClass)
        assertEquals(22292.0, summary.expectedAmount ?: 0.0, 0.01)
        assertEquals("internal", summary.expectedDirection)
        assertEquals("Subscriptions", summary.sourceAccount)
        assertEquals("My Emergency Fund", summary.targetAccount)
    }

    @Test
    fun testBankJagoPaymentSimple() {
        val summary = NotificationProcessor.extractDetectionSummary(
            packageName = "com.jago.digitalBanking",
            title = "Jago",
            bodyText = "You've paid Rp327.708 to Netflix. Need help? Contact Tanya Jago at 1500 746.",
            bigText = null
        )

        assertTrue(summary.isFinancial)
        assertEquals("expense", summary.eventClass)
        assertEquals(327708.0, summary.expectedAmount ?: 0.0, 0.01)
        assertEquals("out", summary.expectedDirection)
        assertEquals("Netflix", summary.expectedCounterparty)
    }

    @Test
    fun testBankJagoPaymentWithPocket() {
        val summary = NotificationProcessor.extractDetectionSummary(
            packageName = "com.jago.digitalBanking",
            title = "Jago",
            bodyText = "Payment of Rp327.708 to Netflix using Subscriptions Pocket was successful.",
            bigText = null
        )

        assertTrue(summary.isFinancial)
        assertEquals("expense", summary.eventClass)
        assertEquals(327708.0, summary.expectedAmount ?: 0.0, 0.01)
        assertEquals("out", summary.expectedDirection)
        assertEquals("Netflix", summary.expectedCounterparty)
        assertEquals("Subscriptions", summary.sourceAccount)
    }

    @Test
    fun testShopeePayTopUpIndonesian() {
        val summary = NotificationProcessor.extractDetectionSummary(
            packageName = "com.shopeepay.id",
            title = "Isi Saldo Berhasil",
            bodyText = "Pengisian saldo sebesar Rp7.490.557 telah ditambahkan ke ShopeePay-mu. Saldo saat ini sebesar Rp7.491.557.",
            bigText = null
        )

        assertTrue(summary.isFinancial)
        assertEquals("income", summary.eventClass)
        assertEquals(7490557.0, summary.expectedAmount ?: 0.0, 0.01)
        assertEquals("in", summary.expectedDirection)
        assertEquals("ShopeePay", summary.sourceAccount)
        assertEquals("Transfer Masuk", summary.categoryName)
    }

    @Test
    fun testShopeePayTopUpEnglish() {
        val summary = NotificationProcessor.extractDetectionSummary(
            packageName = "com.shopee.id",
            title = "Top-up Completed",
            bodyText = "Your Top up request of Rp7.490.557 is successful and your current balance is Rp7.491.557. Transaction No.  UWSNVXUMQY52OYEWKYRSCQWVWHVIE",
            bigText = null
        )

        assertTrue(summary.isFinancial)
        assertEquals("income", summary.eventClass)
        assertEquals(7490557.0, summary.expectedAmount ?: 0.0, 0.01)
        assertEquals("in", summary.expectedDirection)
        assertEquals("ShopeePay", summary.sourceAccount)
        assertEquals("Transfer Masuk", summary.categoryName)
    }

    @Test
    fun testShopeePayTransferInbound() {
        val summary = NotificationProcessor.extractDetectionSummary(
            packageName = "com.shopeepay.id",
            title = "Saldo ShopeePay diterima!",
            bodyText = "RAKA PURNAMA SENTOSA mengirimkan dana sebesar Rp500.000 ke ShopeePay-mu melalui BI-Fast.",
            bigText = null
        )

        // Owner identity is resolved server-side from configured aliases, not hardcoded names.
        assertTrue(summary.isFinancial)
        assertEquals("income", summary.eventClass)
        assertEquals(500000.0, summary.expectedAmount ?: 0.0, 0.01)
        assertEquals("in", summary.expectedDirection)
        assertEquals("ShopeePay", summary.sourceAccount)
    }

    @Test
    fun testUnknownBankWordingYieldsNoHints() {
        val summary = NotificationProcessor.extractDetectionSummary(
            packageName = "com.jago.digitalBanking",
            title = "Jago",
            bodyText = "You've sent Rp1.500.000 to RAKA PURNAMA SENTOSA. Need help? Contact Tanya Jago at 1500 746.",
            bigText = null
        )

        assertFalse(summary.isFinancial)
        assertNull(summary.expectedAmount)
        assertNull(summary.eventClass)
    }

    @Test
    fun testOneTimeCodesAreDetected() {
        assertTrue(NotificationProcessor.isOneTimeCode("Kode OTP", "Kode OTP 123456 untuk transfer Rp50.000", null))
        assertTrue(NotificationProcessor.isOneTimeCode(null, "Jangan berikan kode verifikasi 998877 kepada siapa pun", null))
        assertTrue(NotificationProcessor.isOneTimeCode("Your verification code", null, null))
        assertFalse(NotificationProcessor.isOneTimeCode("Jago", "You've moved Rp100.000 from Subscriptions Pocket to your My Emergency Fund Pocket.", null))
        assertFalse(NotificationProcessor.isOneTimeCode("Coins GRATIS", "Yuk, ambil GRATIS 11,200 GoPay Coins & voucher kamu.", null))
        assertFalse(NotificationProcessor.isOneTimeCode("Pindah saldo", "Kamu telah memindahkan Rp50.000", null))
    }

    @Test
    fun testCodeInBodyIsDropped() {
        assertTrue(NotificationProcessor.isOneTimeCode("BCA", "Kode OTP 123456 untuk transfer Rp50.000", null))
    }

    @Test
    fun testCodeBeforePhraseIsDropped() {
        assertTrue(
            NotificationProcessor.isOneTimeCode(
                "Jago",
                null,
                "998877 adalah kode verifikasi Anda. Jangan berikan kepada siapa pun"
            )
        )
    }

    @Test
    fun testOneTimeCodeTitleIsDropped() {
        assertTrue(NotificationProcessor.isOneTimeCode("Your verification code", "Use it within 5 minutes.", null))
    }

    @Test
    fun testPinWithCodeIsDropped() {
        assertTrue(NotificationProcessor.isOneTimeCode("myBCA", "PIN sementara Anda 4821. Segera ganti", null))
    }

    @Test
    fun testTransactionWithSafetyFooterIsKept() {
        assertFalse(
            NotificationProcessor.isOneTimeCode(
                "Pembayaran berhasil",
                "Pembayaran Rp45.000 ke Kedai Contoh berhasil. Jangan berikan PIN/OTP kepada siapa pun",
                null
            )
        )
    }

    @Test
    fun testAmountNearPhraseIsNotACode() {
        assertFalse(NotificationProcessor.isOneTimeCode("BCA", "Transfer Rp1500000 berhasil. Jangan bagikan kode OTP", null))
        assertFalse(NotificationProcessor.isOneTimeCode("BCA", "Transfer Rp 1500000 berhasil. Jangan bagikan kode OTP", null))
        assertFalse(NotificationProcessor.isOneTimeCode("BCA", "Transfer IDR 250000 berhasil. Jangan bagikan kode OTP", null))
        assertFalse(NotificationProcessor.isOneTimeCode("BCA", "Total 1500000,00 berhasil. Jangan bagikan kode OTP", null))
    }

    @Test
    fun testSeparatedCodeIsNotDetected() {
        // Accepted gap: banks in the sample send contiguous codes, and the backend still ignores
        // one-time-code text, so a separated code is stored but never reaches the ledger.
        assertFalse(NotificationProcessor.isOneTimeCode("BCA", "Kode OTP Anda 123 456. Jangan berikan kepada siapa pun", null))
    }

    @Test
    fun testFooterWithDistantReferenceNumberIsKept() {
        assertFalse(
            NotificationProcessor.isOneTimeCode(
                "Pembayaran berhasil",
                "Pembayaran Rp45.000 ke Kedai Contoh berhasil. No. Ref 48213377. " +
                    "Simpan bukti transaksi ini sebagai arsip Anda. Jangan berikan PIN/OTP kepada siapa pun",
                null
            )
        )
    }
}
