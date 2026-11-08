"""Money inside server sentences (localize-server-text D3).

Stored amounts are whole rupiah, so server sentences always state IDR. The three styles reproduce the shapes the
server wrote before this change, so Indonesian text stays byte-identical (owner decision OD1). Region `US`
groups digits with commas.
"""

from __future__ import annotations

STYLES = ("figure", "inline", "bare")
_GROUPING = {"ID": ".", "US": ","}


def format_money(amount: int, region: str, style: str) -> str:
    """`figure` "Rp 1.234.567" (summaries), `inline` "Rp1.234.567" (questions), `bare` "1.234.567"."""
    if style not in STYLES:
        raise ValueError(f"unknown money style {style!r}")
    if region not in _GROUPING:
        raise ValueError(f"unknown region {region!r}")
    digits = f"{int(amount):,}".replace(",", _GROUPING[region])
    if style == "figure":
        return f"Rp {digits}"
    if style == "inline":
        return f"Rp{digits}"
    return digits
