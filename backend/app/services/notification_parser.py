import re
from dataclasses import asdict, dataclass
from typing import Any


@dataclass
class ParsedNotification:
    is_financial: bool
    event_class: str  # "expense", "income", "transfer", "noise"
    amount: int | None
    currency: str
    direction: str | None  # "in", "out", "internal"
    source_pocket: str | None
    target_pocket: str | None
    counterparty: str | None
    category_hint: str | None
    confidence: float
    raw_title: str | None
    raw_text: str
    package_name: str
    symbol: str | None = None
    instrument_symbol: str | None = None
    lots: int | None = None
    units: float | None = None
    price_per_unit: int | None = None
    investment_action: str | None = None  # "buy" or "sell"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _clean_amount(raw: str) -> int | None:
    if not raw:
        return None
    # Remove dots, commas, 'IDR', 'Rp', spaces
    cleaned = raw.replace("IDR", "").replace("Rp", "").replace(" ", "").strip()
    # Handle decimals (e.g. 500,000.00 or 500.000,00)
    if "," in cleaned and "." in cleaned:
        if cleaned.rfind(".") > cleaned.rfind(","):
            # 500,000.00
            cleaned = cleaned.split(".")[0].replace(",", "")
        else:
            # 500.000,00
            cleaned = cleaned.split(",")[0].replace(".", "")
    elif "." in cleaned:
        parts = cleaned.split(".")
        if len(parts[-1]) == 2:  # cents e.g. 500000.00
            cleaned = "".join(parts[:-1])
        else:  # thousand separator e.g. 500.000
            cleaned = cleaned.replace(".", "")
    elif "," in cleaned:
        parts = cleaned.split(",")
        if len(parts[-1]) == 2:  # cents e.g. 500000,00
            cleaned = "".join(parts[:-1])
        else:
            cleaned = cleaned.replace(",", "")

    try:
        return int(cleaned)
    except ValueError:
        return None

# 2. Bank Jago Pocket Transfers
# 2a. Dual-Pocket Move: "Rp500.000 has been moved from your Main Pocket Pocket to your GoPay Tabungan Pocket."
JAGO_DUAL_POCKET_PATTERN = re.compile(
    r"(?:You(?:\x27ve|\s+have)\s+moved|Kamu(?:\s+telah)?\s+memindahkan\s+)?(?:Rp|IDR)?\s*(?P<amount>[\d\.,]+)(?:\s+has been moved|\s+telah dipindahkan)?\s+(?:from your|dari)\s+(?P<source>.+?)\s+(?:Pocket|Kantong)\s+(?:to your|ke)\s+(?P<target>.+?)\s+(?:Pocket|Kantong)",
    re.IGNORECASE,
)
JAGO_POCKET_PATTERN = JAGO_DUAL_POCKET_PATTERN

# 2b. Single-Pocket Outbound: "You've moved Rp500.000 out of your My Emergency Fund Pocket."
JAGO_OUT_POCKET_PATTERN = re.compile(
    r"(?:You(?:\x27ve|\s+have)\s+moved|Kamu(?:\s+telah)?\s+memindahkan\s+)?(?:Rp|IDR)?\s*(?P<amount>[\d\.,]+)(?:\s+has been moved|\s+telah dipindahkan)?\s+(?:out of your|keluar dari)\s+(?P<source>.+?)\s+(?:Pocket|Kantong)",
    re.IGNORECASE,
)

# 2c. Single-Pocket Inbound: "You've moved Rp500.000 into your Tabungan Pocket."
JAGO_IN_POCKET_PATTERN = re.compile(
    r"(?:You(?:\x27ve|\s+have)\s+moved|Kamu(?:\s+telah)?\s+memindahkan\s+)?(?:Rp|IDR)?\s*(?P<amount>[\d\.,]+)(?:\s+has been moved|\s+telah dipindahkan)?\s+(?:into your|ke dalam|ke)\s+(?P<target>.+?)\s+(?:Pocket|Kantong)",
    re.IGNORECASE,
)

# 3. Bank Jago Incoming Transfer
# "Alfonsus Enrico Soebijanto has sent Rp500.050 to you. Need help? Contact Tanya Jago at 1500 746."
JAGO_INBOUND_PATTERN = re.compile(
    r"(?P<sender>.+?)\s+has sent\s+(?:Rp|IDR)?\s*(?P<amount>[\d\.,]+)\s+to you",
    re.IGNORECASE,
)

# 4. GoPay Outbound Transfer
# "Rp500.000 udah dikirim ke BCA ALFONSUS ENRICO SOEBIJAN."
GOPAY_OUTBOUND_PATTERN = re.compile(
    r"(?:Rp|IDR)?\s*(?P<amount>[\d\.,]+)\s+udah dikirim ke\s+(?P<bank>\w+)\s+(?P<recipient>.+?)\.",
    re.IGNORECASE,
)

# 5. GoPay Inbound Deposit
# "Anda telah menerima Tabungan by Jago sebanyak Rp500.000."
GOPAY_INBOUND_PATTERN = re.compile(
    r"menerima\s+(?P<source>.+?)\s+sebanyak\s+(?:Rp|IDR)?\s*(?P<amount>[\d\.,]+)",
    re.IGNORECASE,
)

# 6. GoPay Transfer Confirmed
# "Kamu berhasil transfer ke TABUNGAN BY JAGO"
GOPAY_TRANSFER_CONFIRMED_PATTERN = re.compile(
    r"berhasil transfer ke\s+(?P<target>.+)",
    re.IGNORECASE,
)

# 7. BCA mobile Inbound
# "Financial Diary: Pemasukan sebesar IDR 500,000.00 dari ***PET **AK ***GSA di kategori Transfer Rekening."
BCA_INBOUND_ID_PATTERN = re.compile(
    r"Pemasukan sebesar\s+(?:IDR|Rp)\s*(?P<amount>[\d\.,]+)\s+dari\s+(?P<sender>.+?)\s+di kategori\s+(?P<category>[^\.]+)",
    re.IGNORECASE,
)

# 8. myBCA Inbound (English)
# "You received IDR 500,000.00 from ***PET **AK ***GSA at Account Transfer category."
BCA_INBOUND_EN_PATTERN = re.compile(
    r"You received\s+(?:IDR|Rp)\s*(?P<amount>[\d\.,]+)\s+from\s+(?P<sender>.+?)\s+at\s+(?P<category>.+?)\s+category",
    re.IGNORECASE,
)

# 9. BCA mobile Outbound
# "Financial Diary: Pengeluaran sebesar IDR 500,000.00 di kategori Pembayaran."
BCA_OUTBOUND_ID_PATTERN = re.compile(
    r"Pengeluaran sebesar\s+(?:IDR|Rp)\s*(?P<amount>[\d\.,]+)\s+di kategori\s+(?P<category>[^\.]+)",
    re.IGNORECASE,
)

# 10. myBCA Outbound (English)
# "You spent IDR 500,000.00 at Payment."
BCA_OUTBOUND_EN_PATTERN = re.compile(
    r"You spent\s+(?:IDR|Rp)\s*(?P<amount>[\d\.,]+)\s+at\s+(?P<category>[^\.]+)",
    re.IGNORECASE,
)

# 11. ShopeePay Inbound BI-Fast
# "ALFONSUS ENRICO SOEBIJANTO mengirimkan dana sebesar Rp500.000 ke ShopeePay-mu melalui BI-Fast."
SHOPEEPAY_INBOUND_PATTERN = re.compile(
    r"(?P<sender>.+?)\s+mengirimkan dana sebesar\s+(?:Rp|IDR)?\s*(?P<amount>[\d\.,]+)\s+ke ShopeePay-mu",
    re.IGNORECASE,
)

# 12. Stockbit Stock Order Match (Buying / Selling)
# "Pembelian 4 lot BBRI match di harga Rp3.340" or "Penjualan 10 lot BBRI match di harga Rp3.340"
STOCKBIT_MATCH_PATTERN = re.compile(
    r"(?P<action>Pembelian|Penjualan|Beli|Jual)\s+(?P<lots>[\d\.,]+)\s+lot\s+(?P<symbol>[A-Za-z0-9]+)\s+match\s+di\s+harga\s+(?:Rp|IDR)?\s*(?P<price>[\d\.,]+)",
    re.IGNORECASE,
)

# 13. QRIS Payment / Transfer
# "Transfer QRIS Rp 50.000 berhasil" or "Transfer QRIS Rp 50.000 ke Kopi Kenangan berhasil"
QRIS_PATTERN = re.compile(
    r"(?:Transfer\s+)?QRIS\s+(?:Rp|IDR)?\s*(?P<amount>[\d\.,]+)(?:\s+ke\s+(?P<merchant>.+?))?\s+berhasil",
    re.IGNORECASE,
)

# 14. Debit / Top Up
# "Rp 25.000 terdebit untuk GoPay Top Up" or "Rp50.000 terdebit"
DEBIT_PATTERN = re.compile(
    r"(?:Rp|IDR)?\s*(?P<amount>[\d\.,]+)\s+terdebit(?:\s+untuk\s+(?P<target>.+))?",
    re.IGNORECASE,
)



def parse_notification(
    package_name: str,
    title: str | None,
    body_text: str | None,
    big_text: str | None = None,
) -> ParsedNotification:
    """
    Deterministically parses a financial notification into structured transaction fields.
    Zero-information loss: preserves raw title, full message, and assigns confidence score.
    """
    raw_content = (big_text if big_text and len(big_text.strip()) > 0 else (body_text or "")).strip()
    full_text = f"{title or ''} {raw_content}".strip()
    clean_package = package_name.strip()

    # Step 1: Discard Empty
    if not raw_content and not title:
        return ParsedNotification(
            is_financial=False,
            event_class="noise",
            amount=None,
            currency="IDR",
            direction=None,
            source_pocket=None,
            target_pocket=None,
            counterparty=None,
            category_hint=None,
            confidence=1.0,
            raw_title=title,
            raw_text=raw_content,
            package_name=clean_package,
        )

    # Step 2: Bank Jago Pocket Transfers
    # 2a. Dual-Pocket Movement
    m = JAGO_DUAL_POCKET_PATTERN.search(raw_content)
    if m:
        amt = _clean_amount(m.group("amount"))
        src = m.group("source").strip()
        tgt = m.group("target").strip()
        return ParsedNotification(
            is_financial=True,
            event_class="transfer",
            amount=amt,
            currency="IDR",
            direction="internal",
            source_pocket=src,
            target_pocket=tgt,
            counterparty=f"{src} → {tgt}",
            category_hint="Internal Movement",
            confidence=1.0,
            raw_title=title,
            raw_text=raw_content,
            package_name=clean_package,
        )

    # 2b. Single-Pocket Outbound ("out of your <Pocket> Pocket")
    m = JAGO_OUT_POCKET_PATTERN.search(raw_content)
    if m:
        amt = _clean_amount(m.group("amount"))
        src = m.group("source").strip()
        return ParsedNotification(
            is_financial=True,
            event_class="transfer",
            amount=amt,
            currency="IDR",
            direction="internal",
            source_pocket=src,
            target_pocket="Kantong Utama",
            counterparty=f"{src} → Kantong Utama",
            category_hint="Internal Movement",
            confidence=1.0,
            raw_title=title,
            raw_text=raw_content,
            package_name=clean_package,
        )

    # 2c. Single-Pocket Inbound ("into your <Pocket> Pocket")
    m = JAGO_IN_POCKET_PATTERN.search(raw_content)
    if m:
        amt = _clean_amount(m.group("amount"))
        tgt = m.group("target").strip()
        return ParsedNotification(
            is_financial=True,
            event_class="transfer",
            amount=amt,
            currency="IDR",
            direction="internal",
            source_pocket="Kantong Utama",
            target_pocket=tgt,
            counterparty=f"Kantong Utama → {tgt}",
            category_hint="Internal Movement",
            confidence=1.0,
            raw_title=title,
            raw_text=raw_content,
            package_name=clean_package,
        )

    # Step 3: Bank Jago Inbound
    m = JAGO_INBOUND_PATTERN.search(raw_content)
    if m:
        amt = _clean_amount(m.group("amount"))
        sender = m.group("sender").strip()
        return ParsedNotification(
            is_financial=True,
            event_class="income",
            amount=amt,
            currency="IDR",
            direction="in",
            source_pocket=None,
            target_pocket=None,
            counterparty=sender,
            category_hint="Transfer Masuk",
            confidence=0.98,
            raw_title=title,
            raw_text=raw_content,
            package_name=clean_package,
        )

    # Step 4: GoPay Outbound Transfer
    m = GOPAY_OUTBOUND_PATTERN.search(raw_content)
    if m:
        amt = _clean_amount(m.group("amount"))
        bank = m.group("bank").strip()
        recipient = m.group("recipient").strip()
        return ParsedNotification(
            is_financial=True,
            event_class="expense",
            amount=amt,
            currency="IDR",
            direction="out",
            source_pocket=None,
            target_pocket=None,
            counterparty=f"{bank} {recipient}",
            category_hint="Transfer Keluar",
            confidence=0.98,
            raw_title=title,
            raw_text=raw_content,
            package_name=clean_package,
        )

    # Step 5: GoPay Inbound Deposit
    m = GOPAY_INBOUND_PATTERN.search(raw_content)
    if m:
        amt = _clean_amount(m.group("amount"))
        src = m.group("source").strip()
        return ParsedNotification(
            is_financial=True,
            event_class="income",
            amount=amt,
            currency="IDR",
            direction="in",
            source_pocket=None,
            target_pocket=None,
            counterparty=src,
            category_hint="Internal Movement",
            confidence=0.98,
            raw_title=title,
            raw_text=raw_content,
            package_name=clean_package,
        )

    # Step 6: GoPay Transfer Confirmed
    m = GOPAY_TRANSFER_CONFIRMED_PATTERN.search(raw_content)
    if m:
        tgt = m.group("target").strip()
        return ParsedNotification(
            is_financial=True,
            event_class="transfer",
            amount=None,  # amount was in the earlier notification
            currency="IDR",
            direction="internal",
            source_pocket=None,
            target_pocket=None,
            counterparty=tgt,
            category_hint="Internal Movement",
            confidence=0.90,
            raw_title=title,
            raw_text=raw_content,
            package_name=clean_package,
        )

    # Step 7: BCA Inbound (ID)
    m = BCA_INBOUND_ID_PATTERN.search(raw_content)
    if m:
        amt = _clean_amount(m.group("amount"))
        sender = m.group("sender").strip()
        cat = m.group("category").strip()
        return ParsedNotification(
            is_financial=True,
            event_class="income",
            amount=amt,
            currency="IDR",
            direction="in",
            source_pocket=None,
            target_pocket=None,
            counterparty=sender,
            category_hint=cat,
            confidence=0.98,
            raw_title=title,
            raw_text=raw_content,
            package_name=clean_package,
        )

    # Step 8: BCA Inbound (EN)
    m = BCA_INBOUND_EN_PATTERN.search(raw_content)
    if m:
        amt = _clean_amount(m.group("amount"))
        sender = m.group("sender").strip()
        cat = m.group("category").strip()
        return ParsedNotification(
            is_financial=True,
            event_class="income",
            amount=amt,
            currency="IDR",
            direction="in",
            source_pocket=None,
            target_pocket=None,
            counterparty=sender,
            category_hint=cat,
            confidence=0.98,
            raw_title=title,
            raw_text=raw_content,
            package_name=clean_package,
        )

    # Step 9: BCA Outbound (ID)
    m = BCA_OUTBOUND_ID_PATTERN.search(raw_content)
    if m:
        amt = _clean_amount(m.group("amount"))
        cat = m.group("category").strip()
        return ParsedNotification(
            is_financial=True,
            event_class="expense",
            amount=amt,
            currency="IDR",
            direction="out",
            source_pocket=None,
            target_pocket=None,
            counterparty="BCA Payment",
            category_hint=cat,
            confidence=0.98,
            raw_title=title,
            raw_text=raw_content,
            package_name=clean_package,
        )

    # Step 10: BCA Outbound (EN)
    m = BCA_OUTBOUND_EN_PATTERN.search(raw_content)
    if m:
        amt = _clean_amount(m.group("amount"))
        cat = m.group("category").strip()
        return ParsedNotification(
            is_financial=True,
            event_class="expense",
            amount=amt,
            currency="IDR",
            direction="out",
            source_pocket=None,
            target_pocket=None,
            counterparty="BCA Payment",
            category_hint=cat,
            confidence=0.98,
            raw_title=title,
            raw_text=raw_content,
            package_name=clean_package,
        )

    # Step 11: ShopeePay Inbound BI-Fast
    m = SHOPEEPAY_INBOUND_PATTERN.search(raw_content)
    if m:
        amt = _clean_amount(m.group("amount"))
        sender = m.group("sender").strip()
        return ParsedNotification(
            is_financial=True,
            event_class="income",
            amount=amt,
            currency="IDR",
            direction="in",
            source_pocket=None,
            target_pocket=None,
            counterparty=sender,
            category_hint="Transfer Masuk",
            confidence=0.98,
            raw_title=title,
            raw_text=raw_content,
            package_name=clean_package,
        )

    # Step 12: Stockbit Stock Order Match (Buying / Selling)
    m = STOCKBIT_MATCH_PATTERN.search(raw_content) or STOCKBIT_MATCH_PATTERN.search(full_text)
    if m:
        action = m.group("action").lower()
        lots = _clean_amount(m.group("lots")) or 0
        symbol = m.group("symbol").upper()
        price = _clean_amount(m.group("price")) or 0
        total_amount = lots * 100 * price
        is_buy = action.startswith("pembelian") or action.startswith("beli")
        inst_symbol = f"{symbol}.JK" if not symbol.endswith(".JK") else symbol
        return ParsedNotification(
            is_financial=True,
            event_class="expense" if is_buy else "income",
            amount=total_amount,
            currency="IDR",
            direction="out" if is_buy else "in",
            source_pocket=None,
            target_pocket=None,
            counterparty=f"Stockbit {symbol} ({m.group('lots')} lot @ Rp{m.group('price')})",
            category_hint="Investasi",
            confidence=1.0,
            raw_title=title,
            raw_text=raw_content,
            package_name=clean_package,
            symbol=symbol,
            instrument_symbol=inst_symbol,
            lots=lots,
            units=float(lots * 100),
            price_per_unit=price,
            investment_action="buy" if is_buy else "sell",
        )

    # Step 13: QRIS Payment / Transfer
    m = QRIS_PATTERN.search(raw_content) or QRIS_PATTERN.search(full_text)
    if m:
        amt = _clean_amount(m.group("amount"))
        merchant = (m.group("merchant") or "QRIS Merchant").strip()
        return ParsedNotification(
            is_financial=True,
            event_class="expense",
            amount=amt,
            currency="IDR",
            direction="out",
            source_pocket=None,
            target_pocket=None,
            counterparty=merchant,
            category_hint="Pembayaran QRIS",
            confidence=1.0,
            raw_title=title,
            raw_text=raw_content,
            package_name=clean_package,
        )

    # Step 14: Debit / Top Up
    m = DEBIT_PATTERN.search(raw_content) or DEBIT_PATTERN.search(full_text)
    if m:
        amt = _clean_amount(m.group("amount"))
        target = (m.group("target") or "").strip()
        return ParsedNotification(
            is_financial=True,
            event_class="expense",
            amount=amt,
            currency="IDR",
            direction="out",
            source_pocket=None,
            target_pocket=None,
            counterparty=target or "Debit",
            category_hint="Pengeluaran",
            confidence=0.95,
            raw_title=title,
            raw_text=raw_content,
            package_name=clean_package,
        )

    # Generic Fallback: Could not determine deterministic pattern -> Strict Whitelist Rejection
    return ParsedNotification(
        is_financial=False,
        event_class="noise",
        amount=None,
        currency="IDR",
        direction=None,
        source_pocket=None,
        target_pocket=None,
        counterparty=None,
        category_hint=None,
        confidence=0.1,
        raw_title=title,
        raw_text=raw_content,
        package_name=clean_package,
    )
