import gzip
import io
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import HTTPException
from PIL import Image, UnidentifiedImageError

from app.core.config import settings

_PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
_JPEG_SIGNATURE = b"\xff\xd8"
_PDF_SIGNATURE = b"%PDF-"
_CATEGORY_SANITIZE_RE = re.compile(r"[^a-z0-9_-]+")


@dataclass(frozen=True)
class PreparedReceipt:
    category: str
    original_filename: str
    original_mime: str
    stored_mime: str
    storage_encoding: str
    compression: str
    stored_ext: str
    original_size: int
    stored_size: int
    content: bytes


def sanitize_receipt_category(value: str | None) -> str:
    cleaned = (value or "general").strip().lower()
    cleaned = cleaned.replace(" ", "-")
    cleaned = _CATEGORY_SANITIZE_RE.sub("-", cleaned).strip("-_")
    if not cleaned:
        cleaned = "general"
    if len(cleaned) > 40:
        cleaned = cleaned[:40].rstrip("-_") or "general"
    return cleaned


def _detect_kind(raw: bytes, filename: str | None, content_type: str | None) -> str:
    _ = filename, content_type
    if raw.startswith(_PDF_SIGNATURE):
        return "pdf"
    if raw.startswith(_PNG_SIGNATURE):
        return "png"
    if raw.startswith(_JPEG_SIGNATURE):
        return "jpeg"
    # Fallback: allow valid images whose headers were rewritten but remain decodable.
    try:
        image = Image.open(io.BytesIO(raw))
        fmt = (image.format or "").upper()
        if fmt == "PNG":
            return "png"
        if fmt == "JPEG":
            return "jpeg"
    except Exception:
        pass
    raise HTTPException(status_code=400, detail="Unsupported receipt type. Allowed: pdf, png, jpg, jpeg")


def _compress_image_to_webp(raw: bytes, quality: int) -> bytes:
    try:
        image = Image.open(io.BytesIO(raw))
        image.load()
    except (UnidentifiedImageError, OSError):
        raise HTTPException(status_code=400, detail="Invalid image file")

    if image.mode not in ("RGB", "RGBA"):
        if "A" in image.getbands():
            image = image.convert("RGBA")
        else:
            image = image.convert("RGB")

    output = io.BytesIO()
    image.save(output, format="WEBP", quality=max(1, min(quality, 100)), method=6)
    return output.getvalue()


def prepare_receipt_payload(
    *,
    raw: bytes,
    filename: str | None,
    content_type: str | None,
    category: str | None,
) -> PreparedReceipt:
    max_bytes = max(1, settings.receipt_max_mb) * 1024 * 1024
    if not raw:
        raise HTTPException(status_code=400, detail="receipt file is empty")
    if len(raw) > max_bytes:
        raise HTTPException(
            status_code=413,
            detail=f"Receipt file too large (max {settings.receipt_max_mb}MB)",
        )

    kind = _detect_kind(raw, filename, content_type)
    normalized_category = sanitize_receipt_category(category)
    original_filename = (filename or "").strip() or "receipt"
    original_size = len(raw)

    if kind in ("png", "jpeg"):
        stored = _compress_image_to_webp(raw, settings.receipt_webp_quality)
        return PreparedReceipt(
            category=normalized_category,
            original_filename=original_filename,
            original_mime="image/png" if kind == "png" else "image/jpeg",
            stored_mime="image/webp",
            storage_encoding="identity",
            compression="webp",
            stored_ext="webp",
            original_size=original_size,
            stored_size=len(stored),
            content=stored,
        )

    compressed_pdf = gzip.compress(raw, compresslevel=9)
    return PreparedReceipt(
        category=normalized_category,
        original_filename=original_filename,
        original_mime="application/pdf",
        stored_mime="application/pdf",
        storage_encoding="gzip",
        compression="gzip",
        stored_ext="pdf.gz",
        original_size=original_size,
        stored_size=len(compressed_pdf),
        content=compressed_pdf,
    )


def _storage_root() -> Path:
    return Path(settings.receipts_dir).expanduser().resolve()


def build_receipt_relative_path(username: str, transaction_id: str, category: str, ext: str) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
    filename = f"{transaction_id}-{category}-{timestamp}.{ext}"
    return f"{username}/{filename}"


def store_receipt(relative_path: str, content: bytes) -> Path:
    root = _storage_root()
    path = (root / relative_path).resolve()
    if root != path and root not in path.parents:
        raise HTTPException(status_code=500, detail="Invalid receipt storage path")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path


def remove_receipt_file(relative_path: str | None) -> None:
    if not relative_path:
        return
    root = _storage_root()
    path = (root / relative_path).resolve()
    if root != path and root not in path.parents:
        return
    try:
        path.unlink(missing_ok=True)
    except Exception:
        pass


def load_receipt_content(relative_path: str, storage_encoding: str) -> bytes:
    root = _storage_root()
    path = (root / relative_path).resolve()
    if root != path and root not in path.parents:
        raise HTTPException(status_code=404, detail="Receipt file not found")
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="Receipt file not found")

    payload = path.read_bytes()
    if storage_encoding == "gzip":
        try:
            return gzip.decompress(payload)
        except (OSError, EOFError):
            raise HTTPException(status_code=500, detail="Corrupted receipt file")
    return payload


def require_transaction_owner(cur, username: str, transaction_id: str) -> None:
    cur.execute(
        """
        SELECT t.transaction_id::text AS transaction_id
        FROM transactions t
        JOIN accounts a ON a.account_id=t.account_id
        WHERE t.transaction_id=%s::uuid AND a.username=%s
        """,
        (transaction_id, username),
    )
    if not cur.fetchone():
        raise HTTPException(status_code=404, detail="Transaction not found")


def get_receipt_row(cur, username: str, transaction_id: str) -> dict[str, Any] | None:
    cur.execute(
        """
        SELECT receipt_id::text AS receipt_id,
               transaction_id::text AS transaction_id,
               username,
               category,
               original_filename,
               original_mime,
               stored_mime,
               storage_encoding,
               compression,
               relative_path,
               original_size,
               stored_size,
               created_at,
               updated_at
        FROM transaction_receipts
        WHERE username=%s AND transaction_id=%s::uuid
        """,
        (username, transaction_id),
    )
    return cur.fetchone()


def upsert_receipt_row(
    cur,
    *,
    username: str,
    transaction_id: str,
    prepared: PreparedReceipt,
    relative_path: str,
) -> tuple[dict[str, Any], str | None]:
    cur.execute(
        """
        SELECT relative_path
        FROM transaction_receipts
        WHERE transaction_id=%s::uuid
        FOR UPDATE
        """,
        (transaction_id,),
    )
    current = cur.fetchone() or {}
    old_relative_path = current.get("relative_path")

    cur.execute(
        """
        INSERT INTO transaction_receipts (
            transaction_id,
            username,
            category,
            original_filename,
            original_mime,
            stored_mime,
            storage_encoding,
            compression,
            relative_path,
            original_size,
            stored_size
        )
        VALUES (%s::uuid, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (transaction_id)
        DO UPDATE SET
            username=EXCLUDED.username,
            category=EXCLUDED.category,
            original_filename=EXCLUDED.original_filename,
            original_mime=EXCLUDED.original_mime,
            stored_mime=EXCLUDED.stored_mime,
            storage_encoding=EXCLUDED.storage_encoding,
            compression=EXCLUDED.compression,
            relative_path=EXCLUDED.relative_path,
            original_size=EXCLUDED.original_size,
            stored_size=EXCLUDED.stored_size,
            updated_at=now()
        RETURNING receipt_id::text AS receipt_id,
                  transaction_id::text AS transaction_id,
                  username,
                  category,
                  original_filename,
                  original_mime,
                  stored_mime,
                  storage_encoding,
                  compression,
                  relative_path,
                  original_size,
                  stored_size,
                  created_at,
                  updated_at
        """,
        (
            transaction_id,
            username,
            prepared.category,
            prepared.original_filename,
            prepared.original_mime,
            prepared.stored_mime,
            prepared.storage_encoding,
            prepared.compression,
            relative_path,
            prepared.original_size,
            prepared.stored_size,
        ),
    )
    row = cur.fetchone()
    return row, old_relative_path


def delete_receipt_row(cur, username: str, transaction_id: str) -> dict[str, Any] | None:
    cur.execute(
        """
        DELETE FROM transaction_receipts
        WHERE username=%s AND transaction_id=%s::uuid
        RETURNING receipt_id::text AS receipt_id,
                  transaction_id::text AS transaction_id,
                  username,
                  category,
                  original_filename,
                  original_mime,
                  stored_mime,
                  storage_encoding,
                  compression,
                  relative_path,
                  original_size,
                  stored_size,
                  created_at,
                  updated_at
        """,
        (username, transaction_id),
    )
    return cur.fetchone()


def serialize_receipt_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "receipt_id": row["receipt_id"],
        "transaction_id": row["transaction_id"],
        "username": row["username"],
        "category": row["category"],
        "original_filename": row["original_filename"],
        "original_mime": row["original_mime"],
        "stored_mime": row["stored_mime"],
        "storage_encoding": row["storage_encoding"],
        "compression": row["compression"],
        "original_size": int(row["original_size"] or 0),
        "stored_size": int(row["stored_size"] or 0),
        "created_at": row["created_at"].astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
        "updated_at": row["updated_at"].astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
    }


def infer_inline_filename(transaction_id: str, category: str, stored_mime: str) -> str:
    ext = "pdf" if stored_mime == "application/pdf" else "webp"
    safe_category = sanitize_receipt_category(category)
    return f"{transaction_id}-{safe_category}.{ext}"
