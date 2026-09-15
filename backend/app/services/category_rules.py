import re
from typing import Any
from uuid import UUID

from app.services.notification_parser import ParsedNotification

# Built-in Indonesian Merchant & Keyword Dictionary
BUILTIN_CATEGORY_KEYWORDS: list[tuple[str, str, str]] = [
    # (pattern_regex, category_name, kakeibo_type)
    (r"kopi|resto|cafe|warung|food|gofood|grabfood|shopeefood|mcd|kfc|hokben|mie|bakso|burger|pizza|dapur", "Makanan & Minuman", "need"),
    (r"gojek|goride|gocar|grab|grabride|grabcar|mrt|krl|kereta|kai|pertamina|shell|spbu|parkir|toll", "Transportasi", "need"),
    (r"pln|listrik|pdam|wifi|indihome|myrepublic|biznet|telkomsel|indosat|tri|xl|pulsa|paket data|bpjs|pbb", "Tagihan & Utilitas", "need"),
    (r"apotek|kimia farma|k24|halodoc|alodokter|rs|rumah sakit|klinik|dokter|obat|laboratorium", "Kesehatan", "need"),
    (r"tokopedia|shopee|lazada|blibli|uniqlo|zara|h&m|indomaret|alfamart|superindo|hypermart", "Belanja", "want"),
    (r"bioskop|cinema|xxi|cgv|cinepolis|netflix|spotify|youtube|disney|steam|playstation|game", "Hiburan", "want"),
    (r"stockbit|bibit|ajaib|pluang|bareksa|rdn|reksadana|saham|gold|emas", "Investasi", "saving"),
    (r"gaji|payroll|salary|honor|upah", "Gaji", "need"),
]

# Jago pocket mapping to default categories
JAGO_POCKET_MAPPINGS: dict[str, str] = {
    "makan-jalan": "Makanan & Minuman",
    "transportasi": "Transportasi",
    "paket-pulsa": "Tagihan & Utilitas",
    "dokter": "Kesehatan",
    "gopay tabungan": "Internal Movement",
    "main pocket": "Internal Movement",
    "dana darurat": "Investasi",
    "cicilan": "Tagihan & Utilitas",
    "kendaraan": "Transportasi",
    "subscription": "Tagihan & Utilitas",
    "potong rambut": "Belanja",
}

# User identity keywords indicating internal movement
USER_IDENTITY_KEYWORDS = [
    "alfonsus enrico",
    "alfonsus enrico soebijan",
    "alfonsus enrico soebijanto",
    "tabungan by jago",
    "kantong jago",
    "main pocket",
]


def resolve_category_for_notification(
    parsed: ParsedNotification,
    categories: list[dict[str, Any]],
    user_rules: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """
    Resolves the best category for a parsed notification using:
    1. Internal Movement detection (self-transfers, pocket moves)
    2. Investment detection (stocks, mutual funds, RDN)
    3. Bank Jago pocket direct match
    4. Learned user rules (merchant_category_rules)
    5. Indonesian keyword / merchant dictionary
    6. Generic fallback
    """
    category_by_name = {c["name"].lower(): c for c in categories}
    raw_search = f"{parsed.raw_title or ''} {parsed.raw_text} {parsed.counterparty or ''}".lower()

    # 1. Internal Movement Check
    is_internal = (
        parsed.direction == "internal"
        or parsed.category_hint == "Internal Movement"
        or any(uid in raw_search for uid in USER_IDENTITY_KEYWORDS)
    )
    if is_internal and "internal movement" in category_by_name:
        target = category_by_name["internal movement"]
        return {
            "category_id": target["id"],
            "category_name": target["name"],
            "kakeibo_type": "saving",
            "is_excluded_from_budget": True,
            "match_source": "internal_movement",
        }

    # 2. Investment Check
    is_investment = any(kw in raw_search for kw in ["stockbit", "bibit", "rdn", "reksadana", "saham", "investasi"])
    if is_investment and "investasi" in category_by_name:
        target = category_by_name["investasi"]
        return {
            "category_id": target["id"],
            "category_name": target["name"],
            "kakeibo_type": "saving",
            "is_excluded_from_budget": True,
            "match_source": "investment_detection",
        }

    # 3. Bank Jago Pocket Direct Match
    for pocket in [parsed.source_pocket, parsed.target_pocket]:
        if pocket:
            cleaned_pocket = pocket.lower().replace("pocket", "").strip()
            if cleaned_pocket in JAGO_POCKET_MAPPINGS:
                mapped_name = JAGO_POCKET_MAPPINGS[cleaned_pocket].lower()
                if mapped_name in category_by_name:
                    target = category_by_name[mapped_name]
                    return {
                        "category_id": target["id"],
                        "category_name": target["name"],
                        "kakeibo_type": target.get("kakeibo_type", "need"),
                        "is_excluded_from_budget": target.get("is_excluded_from_budget", False),
                        "match_source": "jago_pocket_mapping",
                    }

    # 4. Learned User Rules (Merchant Memory)
    if user_rules:
        for rule in user_rules:
            pattern = rule.get("merchant_pattern", "").lower()
            if pattern and pattern in raw_search:
                cid = rule.get("category_id")
                # find category by id
                matched_cat = next((c for c in categories if str(c["id"]) == str(cid)), None)
                if matched_cat:
                    return {
                        "category_id": matched_cat["id"],
                        "category_name": matched_cat["name"],
                        "kakeibo_type": matched_cat.get("kakeibo_type", "need"),
                        "is_excluded_from_budget": matched_cat.get("is_excluded_from_budget", False),
                        "match_source": "user_learned_rule",
                    }

    # 5. Built-in Indonesian Keyword Dictionary
    for pattern, cat_name, k_type in BUILTIN_CATEGORY_KEYWORDS:
        if re.search(pattern, raw_search, re.IGNORECASE):
            c_key = cat_name.lower()
            if c_key in category_by_name:
                target = category_by_name[c_key]
                return {
                    "category_id": target["id"],
                    "category_name": target["name"],
                    "kakeibo_type": k_type,
                    "is_excluded_from_budget": target.get("is_excluded_from_budget", False),
                    "match_source": "keyword_dictionary",
                }

    # 6. Generic Fallback
    target_kind = "income" if parsed.event_class == "income" else "expense"
    fallback_cat = next(
        (c for c in categories if c.get("kind") == target_kind and not c.get("is_excluded_from_budget")),
        categories[0] if categories else None
    )

    if fallback_cat:
        return {
            "category_id": fallback_cat["id"],
            "category_name": fallback_cat["name"],
            "kakeibo_type": fallback_cat.get("kakeibo_type", "need"),
            "is_excluded_from_budget": fallback_cat.get("is_excluded_from_budget", False),
            "match_source": "fallback",
        }

    return {
        "category_id": None,
        "category_name": "Uncategorized",
        "kakeibo_type": "need",
        "is_excluded_from_budget": False,
        "match_source": "none",
    }
