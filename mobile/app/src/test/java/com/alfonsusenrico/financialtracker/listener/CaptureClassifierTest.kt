package com.alfonsusenrico.financialtracker.listener

import com.alfonsusenrico.financialtracker.listener.service.CaptureClassifier
import com.alfonsusenrico.financialtracker.listener.service.CaptureDecision
import com.alfonsusenrico.financialtracker.listener.service.CaptureInput
import com.alfonsusenrico.financialtracker.listener.service.CaptureOutcome
import com.alfonsusenrico.financialtracker.listener.service.CaptureReason
import com.alfonsusenrico.financialtracker.listener.service.NotificationProcessor
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test

/** Synthetic texts shaped like the device sample; no real notification content. */
class CaptureClassifierTest {

    private fun classify(
        packageName: String,
        title: String?,
        body: String?,
        bigText: String? = null,
        ongoing: Boolean = false,
        groupSummary: Boolean = false,
        own: Boolean = false
    ): CaptureDecision = CaptureClassifier.classify(
        CaptureInput(
            packageName = packageName, title = title, bodyText = body, bigText = bigText,
            isOwnPackage = own, isOngoing = ongoing, isGroupSummary = groupSummary
        )
    )

    private fun assertDecision(outcome: CaptureOutcome, reason: String, decision: CaptureDecision) {
        assertEquals(reason, decision.reason)
        assertEquals(outcome, decision.outcome)
    }

    // Transactions are sent.

    @Test fun gopayPaymentIsSent() {
        assertDecision(
            CaptureOutcome.SEND, CaptureReason.RECOGNISED_TRANSACTION,
            classify("com.gojek.gopay", "Pembayaran berhasil", "Kamu baru aja bayar Rp45.000 ke Contoh pake GoPay")
        )
    }

    @Test fun gojekSuperAppPaymentIsSent() {
        assertDecision(
            CaptureOutcome.SEND, CaptureReason.RECOGNISED_TRANSACTION,
            classify("com.gojek.app", "Pembayaran berhasil", "Kamu baru aja bayar Rp45.000 ke Contoh pake GoPay")
        )
    }

    @Test fun myBcaSpendingIsSent() {
        val decision = classify(
            "com.bca.mybca.omni.android", "Financial Diary", "You spent IDR 25,000.00 at Groceries.",
            "You spent IDR 25,000.00 at Groceries."
        )
        assertDecision(CaptureOutcome.SEND, CaptureReason.RECOGNISED_TRANSACTION, decision)
        assertEquals(25000.0, decision.summary.expectedAmount ?: 0.0, 0.01)
    }

    @Test fun jagoDebitCardPaymentIsSent() {
        assertDecision(
            CaptureOutcome.SEND, CaptureReason.RECOGNISED_TRANSACTION,
            classify("com.jago.digitalbanking", "Jago", "You've paid Rp103.440 using your debit card")
        )
    }

    @Test fun jagoPocketMovesAreSent() {
        listOf(
            "You've moved Rp350.000 from My Emergency Fund Pocket to your Subscriptions Pocket.",
            "You've moved Rp500.000 out of your Tabungan Pocket.",
            "You've moved Rp250.000 into your Tabungan Pocket.",
            "Rp500.000 has been moved from your Main Pocket Pocket to your Travel Pocket."
        ).forEach { text ->
            assertDecision(
                CaptureOutcome.SEND, CaptureReason.RECOGNISED_TRANSACTION,
                classify("com.jago.digitalBanking", "Jago", text)
            )
        }
    }

    @Test fun shopeePayTopUpIsSent() {
        assertDecision(
            CaptureOutcome.SEND, CaptureReason.RECOGNISED_TRANSACTION,
            classify(
                "com.shopeepay.id", "Isi Saldo Berhasil",
                "Pengisian saldo sebesar Rp150.000 telah ditambahkan ke ShopeePay-mu. Saldo saat ini sebesar Rp151.000."
            )
        )
    }

    @Test fun stockbitOrderMatchIsSent() {
        assertDecision(
            CaptureOutcome.SEND, CaptureReason.RECOGNISED_TRANSACTION,
            classify("com.stockbit.android", "Order Matched", "Pembelian 2 lot ABCD match di harga Rp1.250")
        )
    }

    @Test fun recognisedPaymentMentioningCashbackIsSent() {
        assertDecision(
            CaptureOutcome.SEND, CaptureReason.RECOGNISED_TRANSACTION,
            classify(
                "com.gojek.gopay", "Pembayaran berhasil",
                "Kamu baru aja bayar Rp45.000 ke Contoh pake GoPay. Dapat cashback 10 GoPay Coins!"
            )
        )
    }

    @Test fun severalAmountsAreSentForReview() {
        assertDecision(
            CaptureOutcome.SEND, CaptureReason.MONEY_EVENT,
            classify("com.bca", "m-Transfer", "Transfer Rp100.000 ke CONTOH berhasil. Biaya admin Rp2.500")
        )
    }

    @Test fun foreignCurrencyIsSentForReview() {
        assertDecision(
            CaptureOutcome.SEND, CaptureReason.MONEY_EVENT,
            classify("com.jago.digitalbanking", "Jago", "You spent USD 12.00 at Contoh Store with your debit card.")
        )
    }

    @Test fun safetyFootersDoNotHideTransactions() {
        assertDecision(
            CaptureOutcome.SEND, CaptureReason.RECOGNISED_TRANSACTION,
            classify(
                "com.bca", "Pembayaran berhasil",
                "Pembayaran Rp45.000 ke Kedai Contoh berhasil. Jangan berikan PIN/OTP kepada siapa pun"
            )
        )
        assertEquals(
            CaptureOutcome.SEND,
            classify("com.bca", "BCA", "Transfer Rp1500000 berhasil. Jangan bagikan kode OTP").outcome
        )
    }

    // Noise is kept on the phone.

    @Test fun gopayPromotionIsKept() {
        assertDecision(
            CaptureOutcome.KEEP_LOCAL, CaptureReason.PROMOTIONAL,
            classify("com.gojek.gopay", "Diskon 50% GoFood", "Pesan sekarang dan klaim vouchernya hari ini!")
        )
        assertDecision(
            CaptureOutcome.KEEP_LOCAL, CaptureReason.PROMOTIONAL,
            classify("com.gojek.app", "Coins GRATIS", "Yuk, ambil GRATIS 11,200 GoPay Coins & voucher kamu.")
        )
        assertDecision(
            CaptureOutcome.KEEP_LOCAL, CaptureReason.PROMOTIONAL,
            classify(
                "com.gojek.gopay", "Hadiah miliaran, cek!",
                "Transferan ke sesama GoPay buat dapetin puzzle & klaim hadiah miliaran. Yuk transferan"
            )
        )
    }

    @Test fun stockbitMarketNewsIsKept() {
        assertDecision(
            CaptureOutcome.KEEP_LOCAL, CaptureReason.MARKET_INFO,
            classify("com.stockbit.android", "IHSG ditutup menguat", "Asing catat Net Sell Rp1,2 T di pasar reguler")
        )
        assertDecision(
            CaptureOutcome.KEEP_LOCAL, CaptureReason.MARKET_INFO,
            classify("com.stockbit.android", "ABCD lagi trending", "Lihat diskusi terbaru tentang ABCD di Stream")
        )
    }

    @Test fun headlineFiguresAreKept() {
        assertDecision(
            CaptureOutcome.KEEP_LOCAL, CaptureReason.HEADLINE_FIGURES,
            classify("com.stockbit.android", "Reksa dana", "Dana kelolaan Contoh naik jadi Rp500 M bulan ini")
        )
    }

    @Test fun shopeePayPaydayOfferIsKept() {
        assertDecision(
            CaptureOutcome.KEEP_LOCAL, CaptureReason.PROMOTIONAL,
            classify("com.shopeepay.id", "GAJIAN!", "Cashback saldo s.d. Rp100.000 buat kamu")
        )
        assertDecision(
            CaptureOutcome.KEEP_LOCAL, CaptureReason.PROMOTIONAL,
            classify("com.shopeepay.id", "GAJIAN!", "Ambil saldo s.d. Rp100.000 sekarang")
        )
    }

    @Test fun jagoAdvertisementIsKept() {
        assertDecision(
            CaptureOutcome.KEEP_LOCAL, CaptureReason.PROMOTIONAL,
            classify("com.jago.digitalbanking", "Invite friends", "Get up to 30K BONUS when you invite friends")
        )
    }

    @Test fun amountLessTextIsKept() {
        assertDecision(
            CaptureOutcome.KEEP_LOCAL, CaptureReason.NO_AMOUNT,
            classify("com.gojek.gopay", "GoPay", "Transfer kamu sudah sampai")
        )
    }

    @Test fun pendingOrFailedTextIsKept() {
        assertDecision(
            CaptureOutcome.KEEP_LOCAL, CaptureReason.HARD_NOISE,
            classify("com.gojek.gopay", "Transfer", "Transfer Rp100.000 kamu sedang diproses")
        )
    }

    @Test fun unsettledSingleAmountWithoutDirectionIsKept() {
        assertDecision(
            CaptureOutcome.KEEP_LOCAL, CaptureReason.SETTLEMENT_UNPROVEN,
            classify("com.gojek.gopay", "Tagihan", "Tagihan listrik kamu bulan ini Rp150.000")
        )
    }

    // Self-evident drops.

    @Test fun oneTimeCodeIsDropped() {
        assertDecision(
            CaptureOutcome.DROP, CaptureReason.ONE_TIME_CODE,
            classify("com.bca", "BCA", "Kode OTP 123456 untuk transfer Rp50.000")
        )
    }

    @Test fun ongoingNotificationIsDropped() {
        assertDecision(
            CaptureOutcome.DROP, CaptureReason.ONGOING,
            classify("com.gojek.gopay", "Transfer berjalan", "Mengirim Rp100.000", ongoing = true)
        )
    }

    @Test fun emptyGroupSummaryIsDropped() {
        assertDecision(
            CaptureOutcome.DROP, CaptureReason.GROUP_SUMMARY,
            classify("com.gojek.gopay", null, null, groupSummary = true)
        )
    }

    @Test fun ownNotificationIsDropped() {
        assertDecision(
            CaptureOutcome.DROP, CaptureReason.OWN_NOTIFICATION,
            classify("com.alfonsusenrico.financialtracker.listener", "Financial Tracker", "Monitoring transactions...", own = true)
        )
    }

    @Test fun textlessNotificationIsDropped() {
        assertDecision(CaptureOutcome.DROP, CaptureReason.EMPTY, classify("com.bca", " ", null))
    }

    @Test fun dropsCarryNoSummary() {
        assertNull(classify("com.bca", "Your verification code", null).summary.expectedAmount)
    }

    // Amount parsing mirrors the backend's parse_idr_amount.

    @Test fun amountsParseLikeTheBackend() {
        assertEquals(25000L, CaptureClassifier.parseIdrAmount("25,000.00"))
        assertEquals(103440L, CaptureClassifier.parseIdrAmount("103.440"))
        assertEquals(1500000L, CaptureClassifier.parseIdrAmount("1.500.000"))
        assertEquals(45000L, CaptureClassifier.parseIdrAmount("45000"))
        assertEquals(7250000L, CaptureClassifier.parseIdrAmount("7.250.000,00"))
        assertNull(CaptureClassifier.parseIdrAmount("1,5"))
        assertNull(CaptureClassifier.parseIdrAmount("1.2345"))
        assertNull(CaptureClassifier.parseIdrAmount("12345.678"))
        assertNull(CaptureClassifier.parseIdrAmount("0"))
    }

    @Test fun balancePhraseIsNotACapturedAmount() {
        // The balance after a top-up is not a second amount, so the top-up stays a single-amount event.
        val decision = classify(
            "com.shopee.id", "Top-up Completed",
            "Your Top up request of Rp50.000 is successful and your current balance is Rp75.000."
        )
        assertEquals(CaptureOutcome.SEND, decision.outcome)
    }

    // Inbox lines and messages extend the captured text.

    @Test fun inboxLinesAreAppendedToBigText() {
        val expanded = NotificationProcessor.expandedBigText(
            "GoPay", "2 notifikasi", null,
            listOf("Pembayaran berhasil", "Kamu baru aja bayar Rp45.000 ke Contoh pake GoPay", "GoPay")
        )
        assertEquals("Pembayaran berhasil\nKamu baru aja bayar Rp45.000 ke Contoh pake GoPay", expanded)
        assertEquals(
            CaptureOutcome.SEND,
            classify("com.gojek.gopay", "GoPay", "2 notifikasi", expanded).outcome
        )
    }

    @Test fun bigTextIsKeptWhenThereAreNoExtraLines() {
        assertEquals("Body", NotificationProcessor.expandedBigText("T", "Body", "Body", listOf("Body", " ")))
        assertNull(NotificationProcessor.expandedBigText("T", "Body", null, emptyList()))
    }
}
