"""Export helpers: CSV and PDF ledger export, currency formatting."""
import csv
import io
from datetime import datetime, timezone
from typing import Any

from fastapi import HTTPException
from fpdf import FPDF


def parse_currency(currency: str | None, fx_rate: str | None) -> tuple[str, float | None]:
    cur = (currency or "IDR").upper()
    if cur not in ("IDR", "USD"):
        cur = "IDR"
    fx = None
    if cur == "USD":
        try:
            fx = float(fx_rate or 0)
        except Exception:
            fx = None
        if not fx or fx <= 0:
            raise HTTPException(status_code=400, detail="fx_rate required for USD export")
    return cur, fx


def format_amount(amount: int, currency: str, fx_rate: float | None) -> str:
    if currency == "USD":
        return f"${float(amount) * float(fx_rate or 0):,.2f}"
    return f"Rp {amount:,.0f}".replace(",", ".")


def format_tx_date(iso_z: str) -> str:
    if not iso_z:
        return ""
    dt = datetime.fromisoformat(iso_z.replace("Z", "+00:00"))
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M")


def safe_pdf_text(value: Any) -> str:
    text = str(value or "").replace("\n", " ").replace("\r", " ")
    try:
        text.encode("latin-1")
        return text
    except UnicodeEncodeError:
        return text.encode("latin-1", "replace").decode("latin-1")


def export_ledger_file(
    rows: list[dict[str, Any]],
    summary_accounts: list[dict[str, Any]],
    scope: str,
    account_id: str | None,
    username: str,
    from_date: str,
    to_date: str,
    export_format: str,
    currency: str,
    fx: float | None,
) -> dict[str, Any]:
    account_name = "All"
    if scope == "account" and account_id:
        match = next((a for a in summary_accounts if a["account_id"] == account_id), None)
        if match:
            account_name = match["account_name"]

    include_account = scope == "all"
    headers = (
        ["No", "Account", "Date", "Transaction", "In", "Out", "Balance"]
        if include_account
        else ["No", "Date", "Transaction", "In", "Out", "Balance"]
    )

    def row_cells(r: dict[str, Any]) -> list[str]:
        debit = int(r.get("debit") or 0)
        credit = int(r.get("credit") or 0)
        base = [
            str(r.get("no") or ""),
            format_tx_date(r.get("date") or ""),
            str(r.get("transaction_name") or ""),
            format_amount(debit, currency, fx) if debit else "",
            format_amount(credit, currency, fx) if credit else "",
            format_amount(int(r.get("balance") or 0), currency, fx),
        ]
        if include_account:
            base.insert(1, str(r.get("account_name") or ""))
        return base

    if export_format == "csv":
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(headers)
        for r in rows:
            writer.writerow(row_cells(r))
        return {"content": output.getvalue(), "media_type": "text/csv", "filename": f"ledger_{from_date}_to_{to_date}.csv"}

    pdf = FPDF(orientation="L" if include_account else "P", unit="mm", format="A4")
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 8, "Ledger Export", ln=True)
    pdf.set_font("Helvetica", size=10)
    pdf.multi_cell(0, 6, f"User: {username} | Account: {account_name} | Range: {from_date} to {to_date}")
    pdf.ln(2)

    widths = [10, 36, 32, 58, 28, 28, 28] if include_account else [10, 32, 64, 28, 28, 28]
    pdf.set_font("Helvetica", "B", 9)
    for idx, label in enumerate(headers):
        pdf.cell(widths[idx], 7, label, border=1)
    pdf.ln()

    pdf.set_font("Helvetica", size=9)
    for r in rows:
        for idx, val in enumerate(row_cells(r)):
            cell = safe_pdf_text(val)
            if len(cell) > 40:
                cell = cell[:37] + "..."
            pdf.cell(widths[idx], 6, cell, border=1)
        pdf.ln()

    pdf_bytes = pdf.output(dest="S")
    if isinstance(pdf_bytes, bytearray):
        pdf_bytes = bytes(pdf_bytes)
    elif isinstance(pdf_bytes, str):
        pdf_bytes = pdf_bytes.encode("latin-1")
    return {"content": pdf_bytes, "media_type": "application/pdf", "filename": f"ledger_{from_date}_to_{to_date}.pdf"}
