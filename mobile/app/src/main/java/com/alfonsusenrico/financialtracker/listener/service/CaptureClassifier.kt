package com.alfonsusenrico.financialtracker.listener.service

import java.math.BigDecimal

enum class CaptureOutcome(val code: String) {
    SEND("send"),
    KEEP_LOCAL("keep_local"),
    DROP("drop")
}

object CaptureReason {
    const val RECOGNISED_TRANSACTION = "recognised_transaction"
    const val MONEY_EVENT = "money_event"
    const val OWNER_SENT = "owner_sent"

    const val HARD_NOISE = "hard_noise"
    const val PROMOTIONAL = "promotional"
    const val MARKET_INFO = "market_info"
    const val HEADLINE_FIGURES = "headline_figures"
    const val NO_AMOUNT = "no_amount"
    const val SETTLEMENT_UNPROVEN = "settlement_unproven"

    const val OWN_NOTIFICATION = "own_notification"
    const val ONGOING = "ongoing"
    const val GROUP_SUMMARY = "group_summary"
    const val EMPTY = "empty"
    const val ONE_TIME_CODE = "one_time_code"
    const val DUPLICATE_SOURCE = "duplicate_source"
}

data class CaptureInput(
    val packageName: String,
    val title: String?,
    val bodyText: String?,
    val bigText: String?,
    val subText: String? = null,
    val summaryText: String? = null,
    val isOwnPackage: Boolean = false,
    val isOngoing: Boolean = false,
    val isGroupSummary: Boolean = false
)

data class CaptureDecision(
    val outcome: CaptureOutcome,
    val reason: String,
    val summary: DetectionSummary = DetectionSummary()
)

/**
 * Decides on the device whether a notification is dropped, sent, or kept on the phone.
 *
 * The text rules mirror `collect_facts` in the backend's `notification_evidence.py`, so a row is kept
 * on the phone only when the backend would ignore it. Deliberate differences: one-time-code words are
 * left to [NotificationProcessor.isOneTimeCode] so safety footers never hide a transaction, and hard
 * noise only applies to unrecognised text, because sending a recognised row costs one request while
 * holding it back could lose a settled payment.
 */
object CaptureClassifier {

    private val MONEY_PATTERN = Regex("""\b(?:Rp\.?|IDR)\s*(-?\d[\d.,]*\d|-?\d)\b""", RegexOption.IGNORE_CASE)
    private val BALANCE_PATTERN = Regex(
        """\b(?:saldo saat ini(?: sebesar)?|current balance(?: is)?|sisa saldo(?: (?:anda|kamu))?(?: sebesar)?|""" +
            """saldo akhir(?: sebesar)?|remaining balance(?: is)?|available balance(?: is)?|""" +
            """saldo (?:anda|kamu)(?: sekarang| saat ini)?(?: sebesar| adalah| jadi)?|saldo tersedia(?: sebesar)?)""" +
            """\s*:?\s*(?:Rp\.?|IDR)?\s*[\d.,]+""",
        RegexOption.IGNORE_CASE
    )
    private val HARD_NOISE_PATTERN = Regex(
        """\b(?:failed|gagal|pending|tertunda|sedang diproses|lagi memproses|akan (?:dikirim|ditransfer)|""" +
            """is requesting|confirm or reject|menunggu pembayaran|segera (?:bayar|transfer)|""" +
            """monitoring transactions|notification listener (?:active|running))\b""" +
            """|\bsebelum \d{1,2} [A-Za-z]{3,9}\b""",
        RegexOption.IGNORE_CASE
    )
    private val PROMOTIONAL_PATTERN = Regex(
        """\b(?:cashback|promo|diskon|bonus|voucher|klaim|up to|hingga)\b|\bs\.d\.""", RegexOption.IGNORE_CASE
    )
    private val MARKET_INFO_PATTERN = Regex(
        """\b(?:IHSG|trending|net (?:buy|sell)|top (?:buy|sell)|YoY|laba bersih|aksi korporasi)\b""",
        RegexOption.IGNORE_CASE
    )
    private val HEADLINE_AMOUNT_PATTERN = Regex(
        """\b(?:Rp\.?|IDR)\s*\d[\d.,]*\s?(?:rb|ribu|k|jt|juta|m|miliar|t|triliun)\b""", RegexOption.IGNORE_CASE
    )
    private val FOREIGN_CURRENCY_PATTERN = Regex("""\b(?:USD|EUR|SGD|MYR)\b|\$\s*\d""", RegexOption.IGNORE_CASE)
    private val SETTLED_PATTERN = Regex(
        """\b(?:berhasil|successful|completed|received|spent|paid|terdebit|""" +
            """pemasukan|pengeluaran|has sent|udah dikirim|telah menerima|""" +
            """mengirimkan dana|you've moved|you have moved|has been moved|""" +
            """telah dipindahkan|telah memindahkan|match di harga|""" +
            """pengisian saldo|telah ditambahkan|top\s*up request|""" +
            """you've sent|you have sent|dikirim|mengirim|terkirim|berhasil kirim|rdn earning|rdn spending|""" +
            """you've transferred|you have transferred|sudah dapat digunakan|deposit berhasil)\b""",
        RegexOption.IGNORE_CASE
    )
    private val OUT_PATTERN = Regex(
        """\b(?:paid|spent|terdebit|pengeluaran|pembayaran|membayar|udah dikirim|""" +
            """transfer (?:ke|to)|dikirim ke|sent to|you've sent|you have sent|mengirim|berhasil kirim|""" +
            """you've transferred|you have transferred|transferred to)\b""",
        RegexOption.IGNORE_CASE
    )
    private val IN_PATTERN = Regex(
        """\b(?:received|pemasukan|menerima|has sent.+to you|""" +
            """mengirimkan dana.+ke ShopeePay-mu|pengisian saldo|""" +
            """ditambahkan ke ShopeePay-mu|top\s*up request.+successful)\b""",
        RegexOption.IGNORE_CASE
    )

    fun classify(input: CaptureInput): CaptureDecision {
        val ownText = listOf(input.title, input.bodyText, input.bigText).any { !it.isNullOrBlank() }
        when {
            input.isOwnPackage -> return drop(CaptureReason.OWN_NOTIFICATION)
            input.isOngoing -> return drop(CaptureReason.ONGOING)
            input.isGroupSummary && !ownText -> return drop(CaptureReason.GROUP_SUMMARY)
            !ownText -> return drop(CaptureReason.EMPTY)
            NotificationProcessor.isOneTimeCode(input.title, input.bodyText, input.bigText) ->
                return drop(CaptureReason.ONE_TIME_CODE)
        }

        val text = notificationText(input)
        val summary = NotificationProcessor.extractDetectionSummary(
            input.packageName, input.title, input.bodyText, input.bigText
        )
        val summaryAmount = summary.expectedAmount?.takeIf { summary.isFinancial && it > 0.0 }

        val balanceSpans = BALANCE_PATTERN.findAll(text).map { it.range }.toList()
        val amounts = mutableSetOf<Long>()
        var idrMentions = 0
        for (match in MONEY_PATTERN.findAll(text)) {
            if (balanceSpans.any { match.range.first >= it.first && match.range.last <= it.last }) continue
            idrMentions++
            parseIdrAmount(match.groupValues[1])?.let(amounts::add)
        }

        val outgoing = OUT_PATTERN.containsMatchIn(text)
        val incoming = IN_PATTERN.containsMatchIn(text)
        val direction = summary.expectedDirection.takeIf { summary.isFinancial }
            ?: if (outgoing != incoming) (if (outgoing) "out" else "in") else null
        val settled = SETTLED_PATTERN.containsMatchIn(text)
        val recognised = summaryAmount != null || (amounts.size == 1 && settled && direction != null)

        if (recognised) return CaptureDecision(CaptureOutcome.SEND, CaptureReason.RECOGNISED_TRANSACTION, summary)

        val keepReason = when {
            HARD_NOISE_PATTERN.containsMatchIn(text) -> CaptureReason.HARD_NOISE
            PROMOTIONAL_PATTERN.containsMatchIn(text) -> CaptureReason.PROMOTIONAL
            MARKET_INFO_PATTERN.containsMatchIn(text) -> CaptureReason.MARKET_INFO
            idrMentions > 0 && HEADLINE_AMOUNT_PATTERN.findAll(text).count() >= idrMentions ->
                CaptureReason.HEADLINE_FIGURES
            idrMentions == 0 && !FOREIGN_CURRENCY_PATTERN.containsMatchIn(text) -> CaptureReason.NO_AMOUNT
            FOREIGN_CURRENCY_PATTERN.containsMatchIn(text) -> null
            amounts.isEmpty() && (!settled || summary.isFinancial) -> CaptureReason.NO_AMOUNT
            amounts.size == 1 && !settled && direction == null -> CaptureReason.SETTLEMENT_UNPROVEN
            else -> null
        }
        return if (keepReason != null) {
            CaptureDecision(CaptureOutcome.KEEP_LOCAL, keepReason, summary)
        } else {
            CaptureDecision(CaptureOutcome.SEND, CaptureReason.MONEY_EVENT, summary)
        }
    }

    /** The backend's `notification_text`: each non-blank field once, joined by newlines. */
    fun notificationText(input: CaptureInput): String =
        listOf(input.title, input.bodyText, input.bigText, input.subText, input.summaryText)
            .mapNotNull { it?.trim()?.takeIf(String::isNotEmpty) }
            .distinct()
            .joinToString("\n")

    /** Port of the backend's `parse_idr_amount`; null where the backend raises an evidence error. */
    fun parseIdrAmount(value: String): Long? {
        val token = value.trim().replace(Regex("""^(?:Rp\.?|IDR)\s*""", RegexOption.IGNORE_CASE), "")
        if (!Regex("""\d+(?:[.,]\d+)*""").matches(token)) return null
        val separators = ".,".filter { it in token }
        val normalized = when (separators.length) {
            2 -> {
                val decimal = if (token.lastIndexOf('.') > token.lastIndexOf(',')) '.' else ','
                val grouping = if (decimal == '.') ',' else '.'
                val integer = token.substringBeforeLast(decimal)
                val fraction = token.substringAfterLast(decimal)
                if (!Regex("""\d{1,3}(?:${Regex.escape(grouping.toString())}\d{3})+""").matches(integer)) return null
                integer.replace(grouping.toString(), "") + "." + fraction
            }
            1 -> {
                val parts = token.split(separators[0])
                when {
                    parts.last().length == 3 -> {
                        if (parts[0].length > 3 || parts.drop(1).any { it.length != 3 }) return null
                        parts.joinToString("")
                    }
                    parts.size == 2 && parts.last().length in 1..2 -> parts[0] + "." + parts[1]
                    else -> return null
                }
            }
            else -> token
        }
        val amount = normalized.toBigDecimalOrNull() ?: return null
        if (amount.stripTrailingZeros().scale() > 0) return null
        if (amount <= BigDecimal.ZERO || amount > BigDecimal.valueOf(Long.MAX_VALUE)) return null
        return amount.toLong()
    }

    private fun drop(reason: String) = CaptureDecision(CaptureOutcome.DROP, reason)
}
