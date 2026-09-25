import gzip
import io
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from fastapi import HTTPException
from PIL import Image, UnidentifiedImageError

from app.core.config import settings

Image.MAX_IMAGE_PIXELS = settings.receipt_max_pixels

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
    except Image.DecompressionBombError:
        raise HTTPException(status_code=413, detail="Receipt image dimensions are too large")
    except Exception:
        pass
    raise HTTPException(status_code=400, detail="Unsupported receipt type. Allowed: pdf, png, jpg, jpeg")


def _compress_image_to_webp(raw: bytes, quality: int) -> bytes:
    try:
        image = Image.open(io.BytesIO(raw))
        image.load()
    except Image.DecompressionBombError:
        raise HTTPException(status_code=413, detail="Receipt image dimensions are too large")
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
