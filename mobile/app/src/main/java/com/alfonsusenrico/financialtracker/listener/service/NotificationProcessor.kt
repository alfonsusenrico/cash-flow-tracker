package com.alfonsusenrico.financialtracker.listener.service

import android.content.Context
import android.util.Log
import com.alfonsusenrico.financialtracker.listener.BuildConfig
import com.alfonsusenrico.financialtracker.listener.data.capture.CaptureFingerprint
import com.alfonsusenrico.financialtracker.listener.data.capture.CaptureStore
import com.alfonsusenrico.financialtracker.listener.data.capture.retryingWrite
import com.alfonsusenrico.financialtracker.listener.data.db.AppDatabase
import com.alfonsusenrico.financialtracker.listener.data.model.RawNotificationEntity
import com.alfonsusenrico.financialtracker.listener.data.prefs.AppPreferences
import com.alfonsusenrico.financialtracker.listener.data.sync.AutoSync
import com.alfonsusenrico.financialtracker.listener.registry.FinancialSourceRegistry
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import java.security.MessageDigest

/** One notification as read from the status bar, before classification. */
data class CapturedNotification(
    val packageName: String,
    val notificationKey: String?,
    val notificationId: Int?,
    val notificationWhen: Long?,
    val channelId: String?,
    val category: String?,
    val title: String?,
    val bodyText: String?,
    val bigText: String?,
    val subText: String?,
    val summaryText: String?,
    val postTime: Long,
    val rawExtrasJson: String?,
    val isOngoing: Boolean = false,
    val isGroupSummary: Boolean = false
)

class NotificationProcessor(private val context: Context) {

    private val prefs = AppPreferences(context)
    private val store = CaptureStore(AppDatabase.getDatabase(context))

    /** Queues a capture; captures are classified and stored one at a time, in arrival order. */
    fun enqueue(notification: CapturedNotification) {
        ensureConsumer(context.applicationContext)
        captureQueue.trySend(notification)
    }

    suspend fun capture(notification: CapturedNotification) {
        // Privacy firewall verification
        if (!FinancialSourceRegistry.isTargetFinancialApp(notification.packageName)) {
            Log.w(TAG, "Blocked non-financial package")
            return
        }

        val decision = CaptureClassifier.classify(
            CaptureInput(
                packageName = notification.packageName,
                title = notification.title,
                bodyText = notification.bodyText,
                bigText = notification.bigText,
                subText = notification.subText,
                summaryText = notification.summaryText,
                isOwnPackage = notification.packageName.equals(context.packageName, ignoreCase = true),
                isOngoing = notification.isOngoing,
                isGroupSummary = notification.isGroupSummary
            )
        )
        if (decision.outcome == CaptureOutcome.DROP) {
            // One-time codes and the other self-evident drops never reach storage.
            prefs.recordDrop()
            Log.d(TAG, "Dropped notification: ${decision.reason}")
            return
        }

        val row = toEntity(notification, decision)
        val result = retryingWrite(
            STORE_RETRY_DELAYS_MILLIS,
            onFailure = {
                prefs.recordCaptureFailure()
                Log.e(TAG, "Capture not stored: store_failed (${it.javaClass.simpleName})")
            }
        ) { store.store(row, notification.isGroupSummary) } ?: return

        when (result) {
            is CaptureStore.Result.Inserted -> {
                prefs.recordCapture()
                Log.i(TAG, "Captured notification: outcome=${result.outcome} reason=${decision.reason}")
                if (result.outcome == CaptureOutcome.SEND.code) AutoSync.sendPending(context)
            }
            is CaptureStore.Result.Reposted -> Log.d(TAG, "Re-post recorded on the existing row")
            CaptureStore.Result.AlreadyStored -> Log.d(TAG, "Deduplicated notification")
        }
    }

    private fun toEntity(notification: CapturedNotification, decision: CaptureDecision): RawNotificationEntity {
        val summary = decision.summary
        val recognized = summary.isFinancial && summary.expectedAmount != null && summary.expectedAmount > 0.0
        return RawNotificationEntity(
            deviceId = prefs.deviceId,
            packageName = notification.packageName,
            appLabel = FinancialSourceRegistry.getAppLabel(notification.packageName),
            notificationKey = notification.notificationKey,
            notificationId = notification.notificationId,
            channelId = notification.channelId,
            category = notification.category,
            title = notification.title,
            bodyText = notification.bodyText,
            bigText = notification.bigText,
            subText = notification.subText,
            summaryText = notification.summaryText,
            postTime = notification.postTime,
            capturedAt = System.currentTimeMillis(),
            payloadHash = computePayloadHash(
                notification.packageName, notification.title, notification.bodyText, notification.postTime
            ),
            rawExtrasJson = notification.rawExtrasJson,
            sourceVersion = BuildConfig.VERSION_NAME,
            isSynced = false,
            captureOutcome = decision.outcome.code,
            captureReason = decision.reason,
            notificationWhen = notification.notificationWhen,
            contentFingerprint = CaptureFingerprint.content(notification.title, notification.bodyText, notification.bigText),
            normalizedFingerprint = CaptureFingerprint.normalized(notification.title, notification.bodyText, notification.bigText),
            isFinancial = if (recognized) true else null,
            eventClass = summary.eventClass.takeIf { recognized },
            expectedAmount = summary.expectedAmount.takeIf { recognized },
            expectedDirection = summary.expectedDirection.takeIf { recognized },
            expectedCounterparty = summary.expectedCounterparty.takeIf { recognized },
            labelNotes = summary.labelNotes.takeIf { recognized },
            labelledAt = if (recognized) System.currentTimeMillis() else null
        )
    }

    companion object {
        const val TAG = "NotificationProcessor"

        private val STORE_RETRY_DELAYS_MILLIS = listOf(200L, 800L)

        // Process-wide so every capture path (listener, rescan, debug receiver) shares one order.
        private val captureQueue = Channel<CapturedNotification>(Channel.UNLIMITED)
        private val captureScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
        @Volatile private var consumerStarted = false

        private fun ensureConsumer(appContext: Context) {
            if (consumerStarted) return
            synchronized(this) {
                if (consumerStarted) return
                consumerStarted = true
                val processor = NotificationProcessor(appContext)
                val prefs = AppPreferences(appContext)
                captureScope.launch {
                    for (notification in captureQueue) {
                        // The consumer must outlive any single capture, so a failure is counted, not fatal.
                        try {
                            processor.capture(notification)
                        } catch (cancelled: CancellationException) {
                            throw cancelled
                        } catch (error: Exception) {
                            prefs.recordCaptureFailure()
                            Log.e(TAG, "Capture failed: capture_error (${error.javaClass.simpleName})")
                        }
                    }
                }
            }
        }

        private const val ONE_TIME_CODE_PHRASE =
            """OTP|one[- ]time password|verification code|security code|kode (?:verifikasi|otp|akses|keamanan|rahasia)"""

        // Bare PIN is excluded: transaction titles never use these phrases, but PIN alone is
        // only a code signal when a code sits next to it.
        private val ONE_TIME_CODE_TITLE = Regex("""\b(?:$ONE_TIME_CODE_PHRASE)\b""", RegexOption.IGNORE_CASE)

        private const val CODE_PHRASE = """\b(?:$ONE_TIME_CODE_PHRASE|PIN)\b"""

        // A standalone 4-8 digit token that is not part of a Rupiah amount: no Rp/IDR prefix and
        // no adjacent ./, digit group.
        private const val CODE_TOKEN = """(?<!\w)(?<!\d[.,])(?<!(?:Rp|IDR)\s{0,2})\d{4,8}(?!\w)(?![.,]\d)"""

        // Safety footers such as "Jangan berikan PIN/OTP kepada siapa pun" mention a phrase without
        // a code, so a code must sit within 30 characters of the phrase, in either order.
        private val CODE_NEAR_PHRASE = Regex(
            """$CODE_PHRASE[\s\S]{0,30}$CODE_TOKEN|$CODE_TOKEN[\s\S]{0,30}$CODE_PHRASE""",
            RegexOption.IGNORE_CASE
        )

        fun isOneTimeCode(title: String?, bodyText: String?, bigText: String?): Boolean {
            if (!title.isNullOrBlank() && ONE_TIME_CODE_TITLE.containsMatchIn(title)) {
                return true
            }
            return listOf(title, bodyText, bigText).any { !it.isNullOrBlank() && CODE_NEAR_PHRASE.containsMatchIn(it) }
        }

        /**
         * The big text with inbox-style lines and messaging-style messages appended, so text an app
         * shows only that way is stored and classified. Pieces equal to the title, text or big text
         * are skipped; the payload hash does not read big text, so it is unaffected.
         */
        fun expandedBigText(title: String?, text: String?, bigText: String?, extraLines: List<String>): String? {
            val known = setOfNotNull(title, text, bigText)
            val extra = extraLines.map { it.trim() }.filter { it.isNotEmpty() && it !in known }.distinct()
            if (extra.isEmpty()) return bigText
            return (listOfNotNull(bigText?.takeIf { it.isNotBlank() }) + extra).joinToString("\n")
        }

        fun computePayloadHash(
            packageName: String,
            title: String?,
            bodyText: String?,
            postTime: Long
        ): String {
            val input = "$packageName|${title.orEmpty()}|${bodyText.orEmpty()}|$postTime"
            val digest = MessageDigest.getInstance("SHA-256")
            val hashBytes = digest.digest(input.toByteArray(Charsets.UTF_8))
            return hashBytes.joinToString("") { "%02x".format(it) }
        }

        fun extractDetectionSummary(
        packageName: String,
        title: String?,
        bodyText: String?,
        bigText: String?
    ): DetectionSummary {
        val text = (if (!bigText.isNullOrBlank()) bigText else bodyText).orEmpty()

        // 1. Bank Jago Pocket Transfers & Payments
        // 1a. Dual pocket move: "Rp500.000 has been moved from your Main Pocket Pocket to your GoPay Tabungan Pocket."
        // "You've moved Rp350.000 from My Emergency Fund Pocket to your Subscriptions Pocket."
        val jagoDualPocketRegex = Regex(
            """(?:You(?:'ve|\s+have)\s+moved|Kamu(?:\s+telah)?\s+memindahkan\s+)?(?:Rp|IDR)?\s*([\d\.,]+)(?:\s+has been moved|\s+telah dipindahkan)?\s+(?:from(?:\s+your)?|dari)\s+(.+?)\s+(?:Pocket|Kantong)\s+(?:to(?:\s+your)?|ke)\s+(.+?)\s+(?:Pocket|Kantong)""",
            RegexOption.IGNORE_CASE
        )
        val jagoDualMatch = jagoDualPocketRegex.find(text)
        if (jagoDualMatch != null) {
            val amount = jagoDualMatch.groupValues[1].replace(".", "").replace(",", "").toDoubleOrNull()
            val source = jagoDualMatch.groupValues[2].trim()
            val target = jagoDualMatch.groupValues[3].trim()
            return DetectionSummary(
                isFinancial = true,
                eventClass = "transfer",
                expectedAmount = amount,
                expectedDirection = "internal",
                expectedCounterparty = "$source → $target",
                labelNotes = "Internal Movement",
                sourceAccount = source,
                targetAccount = target,
                categoryName = "Internal Movement"
            )
        }

        // 1b. Single pocket OUT: "You've moved Rp500.000 out of your My Emergency Fund Pocket."
        val jagoOutPocketRegex = Regex(
            """(?:You(?:'ve|\s+have)\s+moved|Kamu(?:\s+telah)?\s+memindahkan\s+)?(?:Rp|IDR)?\s*([\d\.,]+)(?:\s+has been moved|\s+telah dipindahkan)?\s+(?:out of(?:\s+your)?|keluar dari)\s+(.+?)\s+(?:Pocket|Kantong)""",
            RegexOption.IGNORE_CASE
        )
        val jagoOutMatch = jagoOutPocketRegex.find(text)
        if (jagoOutMatch != null) {
            val amount = jagoOutMatch.groupValues[1].replace(".", "").replace(",", "").toDoubleOrNull()
            val source = jagoOutMatch.groupValues[2].trim()
            return DetectionSummary(
                isFinancial = true,
                eventClass = "transfer",
                expectedAmount = amount,
                expectedDirection = "internal",
                expectedCounterparty = "$source → Kantong Utama",
                labelNotes = "Internal Movement",
                sourceAccount = source,
                targetAccount = "Bank Jago",
                categoryName = "Internal Movement"
            )
        }

        // 1c. Single pocket INTO: "You've moved Rp500.000 into your Tabungan Pocket."
        val jagoInPocketRegex = Regex(
            """(?:You(?:'ve|\s+have)\s+moved|Kamu(?:\s+telah)?\s+memindahkan\s+)?(?:Rp|IDR)?\s*([\d\.,]+)(?:\s+has been moved|\s+telah dipindahkan)?\s+(?:into(?:\s+your)?|ke dalam|ke)\s+(.+?)\s+(?:Pocket|Kantong)""",
            RegexOption.IGNORE_CASE
        )
        val jagoInPocketMatch = jagoInPocketRegex.find(text)
        if (jagoInPocketMatch != null) {
            val amount = jagoInPocketMatch.groupValues[1].replace(".", "").replace(",", "").toDoubleOrNull()
            val target = jagoInPocketMatch.groupValues[2].trim()
            return DetectionSummary(
                isFinancial = true,
                eventClass = "transfer",
                expectedAmount = amount,
                expectedDirection = "internal",
                expectedCounterparty = "Kantong Utama → $target",
                labelNotes = "Internal Movement",
                sourceAccount = "Bank Jago",
                targetAccount = target,
                categoryName = "Internal Movement"
            )
        }

        // 1d. Bank Jago Payment / Card Expense with pocket
        if (packageName.contains("jago", ignoreCase = true)) {
            val jagoPayWithPocketRegex = Regex(
                """(?:You(?:'ve|\s+have)\s+(?:paid|spent)|Kamu(?:\s+telah)?\s+membayar|Payment of|Pembayaran sebesar)\s+(?:Rp|IDR)?\s*([\d\.,]+)\s+(?:to|at|ke)\s+(.+?)\s+(?:using|from|menggunakan|dari)\s+(?:your\s+)?(?:kantong\s+)?(.+?)(?:\s+(?:pocket|kantong))?(?:\s+was\s+successful|\s+berhasil|\.|\s*$)""",
                RegexOption.IGNORE_CASE
            )
            val jagoPayPocketMatch = jagoPayWithPocketRegex.find(text)
            if (jagoPayPocketMatch != null) {
                val amount = jagoPayPocketMatch.groupValues[1].replace(".", "").replace(",", "").toDoubleOrNull()
                val merchantRaw = jagoPayPocketMatch.groupValues[2].trim()
                val pocket = jagoPayPocketMatch.groupValues[3].trim()
                val cleanMerchant = merchantRaw.replace(Regex("""(?i)\s*(?:Need help\?.*|Contact Tanya Jago.*|was successful|berhasil|\.).*$"""), "").trim()
                return DetectionSummary(
                    isFinancial = true,
                    eventClass = "expense",
                    expectedAmount = amount,
                    expectedDirection = "out",
                    expectedCounterparty = cleanMerchant.ifBlank { "Bank Jago" },
                    labelNotes = "Tagihan & Utilitas",
                    sourceAccount = pocket,
                    targetAccount = null,
                    categoryName = "Tagihan & Utilitas"
                )
            }

            // 1e. Bank Jago Simple Payment / Card Expense
            val jagoPaySimpleRegex = Regex(
                """(?:You(?:'ve|\s+have)\s+(?:paid|spent)|Kamu(?:\s+telah)?\s+membayar|Payment of|Pembayaran sebesar)\s+(?:Rp|IDR)?\s*([\d\.,]+)(?:\s+(?:to|at|ke)\s+([^\.\n]+))?""",
                RegexOption.IGNORE_CASE
            )
            val jagoPaySimpleMatch = jagoPaySimpleRegex.find(text)
            if (jagoPaySimpleMatch != null) {
                val amount = jagoPaySimpleMatch.groupValues[1].replace(".", "").replace(",", "").toDoubleOrNull()
                val merchantRaw = jagoPaySimpleMatch.groupValues[2].trim()
                val cleanMerchant = merchantRaw.replace(Regex("""(?i)\s*(?:Need help\?.*|Contact Tanya Jago.*|was successful|berhasil|\.).*$"""), "").trim()
                return DetectionSummary(
                    isFinancial = true,
                    eventClass = "expense",
                    expectedAmount = amount,
                    expectedDirection = "out",
                    expectedCounterparty = cleanMerchant.ifBlank { "Bank Jago" },
                    labelNotes = "Tagihan & Utilitas",
                    sourceAccount = "Bank Jago",
                    targetAccount = null,
                    categoryName = "Tagihan & Utilitas"
                )
            }
        }

        // 2. Bank Jago incoming transfer: "Raka Purnama has sent Rp500.050 to you."
        val jagoInboundRegex = Regex("""(.+?)\s+has sent Rp([\d\.]+)\s+to you""", RegexOption.IGNORE_CASE)
        val jagoInboundMatch = jagoInboundRegex.find(text)
        if (jagoInboundMatch != null) {
            val sender = jagoInboundMatch.groupValues[1].trim()
            val amount = jagoInboundMatch.groupValues[2].replace(".", "").toDoubleOrNull()
            return DetectionSummary(
                isFinancial = true,
                eventClass = "income",
                expectedAmount = amount,
                expectedDirection = "in",
                expectedCounterparty = sender,
                labelNotes = "Transfer Masuk",
                sourceAccount = "Bank Jago",
                categoryName = "Transfer Masuk"
            )
        }

        // 3. GoPay outbound transfer: "Rp500.000 udah dikirim ke BCA RAKA PURNAMA."
        val gopayOutRegex = Regex("""Rp([\d\.]+)\s+udah dikirim ke\s+(\w+)\s+(.+?)\.""", RegexOption.IGNORE_CASE)
        val gopayOutMatch = gopayOutRegex.find(text)
        if (gopayOutMatch != null) {
            val amount = gopayOutMatch.groupValues[1].replace(".", "").toDoubleOrNull()
            val bank = gopayOutMatch.groupValues[2].trim()
            val recipient = gopayOutMatch.groupValues[3].trim()
            return DetectionSummary(
                isFinancial = true,
                eventClass = "expense",
                expectedAmount = amount,
                expectedDirection = "out",
                expectedCounterparty = "$bank $recipient",
                labelNotes = "Transfer Keluar",
                sourceAccount = "GoPay",
                categoryName = "Transfer Keluar"
            )
        }

        // 4. GoPay inbound: "Anda telah menerima Tabungan by Jago sebanyak Rp500.000."
        val gopayInRegex = Regex("""menerima\s+(.+?)\s+sebanyak\s+Rp([\d\.]+)""", RegexOption.IGNORE_CASE)
        val gopayInMatch = gopayInRegex.find(text)
        if (gopayInMatch != null) {
            val source = gopayInMatch.groupValues[1].trim()
            val amount = gopayInMatch.groupValues[2].replace(".", "").toDoubleOrNull()
            return DetectionSummary(
                isFinancial = true,
                eventClass = "income",
                expectedAmount = amount,
                expectedDirection = "in",
                expectedCounterparty = source,
                labelNotes = "Internal Movement",
                sourceAccount = "GoPay",
                targetAccount = null,
                categoryName = "Internal Movement"
            )
        }

        // 5. BCA Mobile / myBCA Inbound
        val bcaInRegex = Regex(
            """(?:received|Pemasukan sebesar)\s+(?:IDR|Rp)\s*([\d\.,]+?)(?:\s+(?:from|dari)\s+(.+?))?\s+(?:at|di kategori)\s+(.+?)(?:\s+category)?(?:\.|\s*$)""",
            RegexOption.IGNORE_CASE
        )
        val bcaInMatch = bcaInRegex.find(text)
        if (bcaInMatch != null) {
            val amountRaw = bcaInMatch.groupValues[1].trim()
            val noCents = amountRaw.replace(Regex("""[\.,]00$"""), "")
            val amount = noCents.replace(".", "").replace(",", "").toDoubleOrNull()
            val sender = bcaInMatch.groupValues[2].trim().ifBlank { "BCA" }
            val cat = bcaInMatch.groupValues[3].trim()
            return DetectionSummary(
                isFinancial = true,
                eventClass = "income",
                expectedAmount = amount,
                expectedDirection = "in",
                expectedCounterparty = sender,
                labelNotes = cat,
                sourceAccount = "BCA",
                categoryName = cat
            )
        }

        // 6. BCA Mobile / myBCA Outbound
        val bcaOutRegex = Regex(
            """(?:spent|Pengeluaran sebesar)\s+(?:IDR|Rp)\s*([\d\.,]+?)\s+(?:at|di kategori)\s+(.+?)(?:\s+category)?(?:\.|\s*$)""",
            RegexOption.IGNORE_CASE
        )
        val bcaOutMatch = bcaOutRegex.find(text)
        if (bcaOutMatch != null) {
            val amountRaw = bcaOutMatch.groupValues[1].trim()
            val noCents = amountRaw.replace(Regex("""[\.,]00$"""), "")
            val amount = noCents.replace(".", "").replace(",", "").toDoubleOrNull()
            val cat = bcaOutMatch.groupValues[2].trim()
            return DetectionSummary(
                isFinancial = true,
                eventClass = "expense",
                expectedAmount = amount,
                expectedDirection = "out",
                expectedCounterparty = "BCA",
                labelNotes = cat,
                sourceAccount = "BCA",
                categoryName = cat
            )
        }

        // 7. ShopeePay Inbound
        val shopeeInRegex = Regex("""(.+?)\s+mengirimkan dana sebesar Rp([\d\.]+)\s+ke ShopeePay-mu""", RegexOption.IGNORE_CASE)
        val shopeeInMatch = shopeeInRegex.find(text)
        if (shopeeInMatch != null) {
            val sender = shopeeInMatch.groupValues[1].trim()
            val amount = shopeeInMatch.groupValues[2].replace(".", "").replace(",", "").toDoubleOrNull()
            return DetectionSummary(
                isFinancial = true,
                eventClass = "income",
                expectedAmount = amount,
                expectedDirection = "in",
                expectedCounterparty = sender,
                labelNotes = "Transfer Masuk",
                sourceAccount = "ShopeePay",
                categoryName = "Transfer Masuk"
            )
        }

        // 7b. ShopeePay Top-Up Inbound (Indonesian & English)
        // "Pengisian saldo sebesar Rp7.490.557 telah ditambahkan ke ShopeePay-mu. Saldo saat ini sebesar Rp7.491.557."
        // "Your Top up request of Rp7.490.557 is successful and your current balance is Rp7.491.557."
        val shopeeTopUpRegex = Regex(
            """(?:Pengisian saldo sebesar|Top\s*up request of)\s+(?:Rp|IDR)?\s*([\d\.,]+)""",
            RegexOption.IGNORE_CASE
        )
        val shopeeTopUpMatch = shopeeTopUpRegex.find(text)
        if (shopeeTopUpMatch != null) {
            val amount = shopeeTopUpMatch.groupValues[1].replace(".", "").replace(",", "").toDoubleOrNull()
            return DetectionSummary(
                isFinancial = true,
                eventClass = "income",
                expectedAmount = amount,
                expectedDirection = "in",
                expectedCounterparty = "ShopeePay Top Up",
                labelNotes = "Transfer Masuk",
                sourceAccount = "ShopeePay",
                categoryName = "Transfer Masuk"
            )
        }


        // 8. Stockbit Stock Order Match (Buying / Selling)
        val stockbitRegex = Regex(
            """(Pembelian|Penjualan|Beli|Jual)\s+([\d\.,]+)\s+lot\s+([A-Z0-9]+)\s+match\s+di\s+harga\s+(?:Rp|IDR)?\s*([\d\.,]+)""",
            RegexOption.IGNORE_CASE
        )
        val stockbitMatch = stockbitRegex.find(text)
        if (stockbitMatch != null) {
            val action = stockbitMatch.groupValues[1].lowercase()
            val lotsStr = stockbitMatch.groupValues[2].replace(".", "").replace(",", "")
            val symbol = stockbitMatch.groupValues[3].uppercase()
            val priceStr = stockbitMatch.groupValues[4].replace(".", "").replace(",", "")

            val lots = lotsStr.toDoubleOrNull() ?: 0.0
            val price = priceStr.toDoubleOrNull() ?: 0.0
            val totalAmount = lots * 100.0 * price

            val isBuy = action.startsWith("pembelian") || action.startsWith("beli")
            return DetectionSummary(
                isFinancial = true,
                eventClass = if (isBuy) "transfer" else "transfer",
                expectedAmount = totalAmount,
                expectedDirection = if (isBuy) "out" else "in",
                expectedCounterparty = "Stockbit $symbol (${stockbitMatch.groupValues[2]} lot @ Rp${stockbitMatch.groupValues[4]})",
                labelNotes = "Investasi",
                sourceAccount = if (isBuy) "RDN BCA" else symbol,
                targetAccount = if (isBuy) symbol else "RDN BCA",
                categoryName = "Investasi",
                investmentAction = if (isBuy) "buy" else "sell",
                units = lots * 100.0,
                pricePerUnit = price
            )
        }

        // 9. GoPay QRIS / Merchant Payment: "Kamu telah membayar Rp25.000 ke Kopi Kenangan" or "Pembayaran sebesar Rp25.000 ke ... berhasil"
        val gopayPayRegex = Regex("""(?:membayar|Pembayaran sebesar)\s+Rp([\d\.]+)\s+ke\s+(.+?)(?:\s+berhasil|\.|$)""", RegexOption.IGNORE_CASE)
        val gopayPayMatch = gopayPayRegex.find(text)
        if (gopayPayMatch != null) {
            val amount = gopayPayMatch.groupValues[1].replace(".", "").toDoubleOrNull()
            val merchant = gopayPayMatch.groupValues[2].trim()
            return DetectionSummary(
                isFinancial = true,
                eventClass = "expense",
                expectedAmount = amount,
                expectedDirection = "out",
                expectedCounterparty = merchant,
                labelNotes = "Pembayaran Merchant",
                sourceAccount = "GoPay",
                categoryName = "Makanan & Minuman"
            )
        }

        // Default: Strict whitelist rejection. If no confirmed transaction pattern matched, it is non-financial noise.
        return DetectionSummary(isFinancial = false)
    }
}
}

data class DetectionSummary(
    val isFinancial: Boolean = false,
    val eventClass: String? = null,
    val expectedAmount: Double? = null,
    val expectedDirection: String? = null,
    val expectedCounterparty: String? = null,
    val labelNotes: String? = null,
    val sourceAccount: String? = null,
    val targetAccount: String? = null,
    val categoryName: String? = null,
    val investmentAction: String? = null,
    val units: Double? = null,
    val pricePerUnit: Double? = null
)
