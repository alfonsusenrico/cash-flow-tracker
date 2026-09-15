import asyncio
import json
import logging
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

logger = logging.getLogger("market_data")

# Cache USD/IDR rate for 1 hour
_USDIDR_CACHE: dict[str, Any] = {
    "rate": 16500.0,
    "expires_at": 0.0,
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
}


def _http_get_json(url: str, timeout: int = 6) -> dict[str, Any] | None:
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            if resp.status == 200:
                content = resp.read().decode("utf-8")
                return json.loads(content)
    except Exception as e:
        logger.warning(f"Market data request failed for {url}: {e}")
        return None
    return None


def _map_yahoo_quote_type(quote_type: str, symbol: str) -> str:
    qt = (quote_type or "").upper()
    sym = (symbol or "").upper()
    if sym.endswith("=F") or "GOLD" in sym:
        return "gold"
    if qt in ("CRYPTOCURRENCY", "COIN") or sym.endswith("-USD"):
        return "crypto"
    if qt in ("ETF", "MUTUALFUND"):
        return "mutual_fund"
    if qt in ("EQUITY",):
        return "stock"
    return "other"


async def get_usdidr_rate() -> float:
    now = time.time()
    if now < _USDIDR_CACHE["expires_at"]:
        return _USDIDR_CACHE["rate"]

    def _fetch() -> float | None:
        url = "https://query1.finance.yahoo.com/v8/finance/chart/USDIDR=X?interval=1d&range=1d"
        data = _http_get_json(url, timeout=5)
        if data and "chart" in data and data["chart"].get("result"):
            meta = data["chart"]["result"][0].get("meta", {})
            rate = meta.get("regularMarketPrice")
            if rate and rate > 0:
                return float(rate)
        return None

    rate = await asyncio.to_thread(_fetch)
    if rate:
        _USDIDR_CACHE["rate"] = rate
        _USDIDR_CACHE["expires_at"] = now + 3600  # 1 hour
    return _USDIDR_CACHE["rate"]


async def search_instruments(query: str, limit: int = 10) -> list[dict[str, Any]]:
    clean_query = query.strip()
    if not clean_query:
        return []

    def _fetch() -> list[dict[str, Any]]:
        encoded = urllib.parse.quote(clean_query)
        url = f"https://query2.finance.yahoo.com/v1/finance/search?q={encoded}&quotesCount={limit}&newsCount=0"
        data = _http_get_json(url, timeout=6)
        if not data or "quotes" not in data:
            return []

        results: list[dict[str, Any]] = []
        for q in data["quotes"]:
            symbol = q.get("symbol")
            if not symbol:
                continue

            name = q.get("shortname") or q.get("longname") or symbol
            quote_type = q.get("quoteType", "")
            exchange = q.get("exchange", "")
            inst_type = _map_yahoo_quote_type(quote_type, symbol)

            results.append({
                "symbol": symbol,
                "name": name,
                "type": inst_type,
                "exchange": exchange,
                "quote_type": quote_type,
            })

        # Sort so that IDX stocks (.JK) or exact symbol matches appear first
        def _sort_key(item: dict[str, Any]) -> tuple[int, str]:
            sym = item["symbol"].upper()
            if sym == clean_query.upper():
                return (0, sym)
            if sym.endswith(".JK"):
                return (1, sym)
            return (2, sym)

        results.sort(key=_sort_key)

        # Prepend GOLD-IDR virtual ticker if searching for gold/emas
        if any(k in clean_query.upper() for k in ("EMAS", "GOLD", "XAU")):
            results.insert(0, {
                "symbol": "GOLD-IDR",
                "name": "Harga Emas Dunia (IDR per gram)",
                "type": "gold",
                "exchange": "SPOT",
                "quote_type": "COMMODITY",
            })

        return results[:limit]

    return await asyncio.to_thread(_fetch)


async def get_instrument_quote(symbol: str) -> dict[str, Any] | None:
    clean_symbol = symbol.strip().upper()
    if not clean_symbol:
        return None

    # Support virtual tickers for Gold in IDR per gram (converted from COMEX GC=F and USD/IDR)
    if clean_symbol in ("GOLD-IDR", "EMAS-IDR", "GOLD-GRAM", "EMAS-GRAM", "XAU-IDR"):
        gc_quote = await get_instrument_quote("GC=F")
        if gc_quote and gc_quote.get("price"):
            # 1 Troy Ounce = 31.1034768 grams
            price_per_gram = int(round(gc_quote["price"] / 31.1034768))
            return {
                "symbol": clean_symbol,
                "price": price_per_gram,
                "raw_price": round(gc_quote.get("raw_price", 0) / 31.1034768, 2),
                "currency": "IDR",
                "original_currency": "USD",
                "timestamp": gc_quote.get("timestamp"),
            }
        return None

    def _fetch() -> dict[str, Any] | None:
        encoded = urllib.parse.quote(clean_symbol)
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{encoded}?interval=1d&range=1d"
        data = _http_get_json(url, timeout=6)
        if not data or "chart" not in data or not data["chart"].get("result"):
            return None

        meta = data["chart"]["result"][0].get("meta", {})
        price = meta.get("regularMarketPrice")
        if price is None:
            return None

        currency = (meta.get("currency") or "IDR").upper()
        timestamp = meta.get("regularMarketTime")

        return {
            "symbol": clean_symbol,
            "raw_price": float(price),
            "currency": currency,
            "timestamp": timestamp,
        }

    quote = await asyncio.to_thread(_fetch)
    if not quote:
        return None

    price_in_idr = quote["raw_price"]
    if quote["currency"] == "USD":
        usd_rate = await get_usdidr_rate()
        price_in_idr = price_in_idr * usd_rate

    rounded_idr = int(round(price_in_idr))

    return {
        "symbol": clean_symbol,
        "price": rounded_idr,
        "raw_price": quote["raw_price"],
        "currency": "IDR",
        "original_currency": quote["currency"],
        "timestamp": quote["timestamp"],
    }


async def sync_all_tracked_prices(conn) -> dict[str, int]:
    """
    Fetches latest quotes for all active accounts with instrument_symbol,
    updates last_price and last_price_at in database.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT instrument_symbol
            FROM accounts
            WHERE instrument_symbol IS NOT NULL
              AND is_archived = FALSE
        """)
        symbols = [row["instrument_symbol"] for row in cur.fetchall() if row.get("instrument_symbol")]

    if not symbols:
        return {"symbols_checked": 0, "accounts_updated": 0}

    now_utc = datetime.now(timezone.utc)
    updated_count = 0

    for sym in symbols:
        quote = await get_instrument_quote(sym)
        if not quote or quote.get("price") is None:
            continue

        latest_price = quote["price"]
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE accounts
                SET last_price = %s,
                    last_price_at = %s,
                    updated_at = NOW()
                WHERE instrument_symbol = %s
                  AND is_archived = FALSE
            """, (latest_price, now_utc, sym))
            updated_count += cur.rowcount
        conn.commit()

    return {"symbols_checked": len(symbols), "accounts_updated": updated_count}
