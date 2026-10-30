"""Owner-confirmed salary sources and receipts; no transaction is created here."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
import logging
import re
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

from app.core.config import settings
from app.db.pool import db_conn
from app.services.report_figures import reportable_income_sql

logger = logging.getLogger(__name__)


@dataclass
class SalaryError(Exception):
    code: str
    status: int
    message: str


def has_explicit_salary_marker(merchant: str | None, notes: str | None) -> bool:
    evidence = f"{merchant or ''} {notes or ''}".casefold()
    salary = re.search(r'(?<!\w)(?:gaji|salary)(?!\w)', evidence)
    excluded = re.search(r'(?<!\w)(?:bonus|refund|reimbursement|cashback)(?!\w)|ganti\s+uang|(?:bukan|tidak|not)\s+(?:gaji|salary)', evidence)
    return bool(salary and not excluded)


def _eligible_income(cur: Any, user_id: str, transaction_id: str) -> dict[str, Any]:
    cur.execute(
        f"""SELECT t.id, t.type, t.date, t.merchant, t.notes, t.movement_id,
                  {reportable_income_sql()} AS reportable
           FROM transactions t LEFT JOIN categories c ON c.id = t.category_id AND c.user_id = t.user_id
           WHERE t.user_id = %s AND t.id = %s FOR UPDATE OF t""",
        (user_id, transaction_id),
    )
    row = cur.fetchone()
    if not row:
        raise SalaryError('salary_transaction_not_found', 404, 'Transaksi tidak ditemukan.')
    if not row['reportable'] or row['movement_id'] is not None or row['date'] > datetime.now(timezone.utc):
        raise SalaryError('invalid_salary_income', 422, 'Pilih pemasukan tercatat yang bukan ganti uang.')
    return row


def configure_source(conn: Any, user_id: str, transaction_id: str) -> dict[str, str]:
    with conn.cursor() as cur:
        cur.execute('SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))', (f'salary:{user_id}',))
        row = _eligible_income(cur, user_id, transaction_id)
        field = 'merchant' if (row['merchant'] or '').strip() else 'notes'
        value = ' '.join((row[field] or '').split())
        if not value or len(value) > 500:
            raise SalaryError('salary_source_missing', 422, 'Pemasukan ini belum memiliki nama pengirim atau keterangan.')
        cur.execute(
            """INSERT INTO salary_sources (user_id, matcher_field, matcher_text)
               VALUES (%s, %s, %s)
               ON CONFLICT (user_id) DO UPDATE SET matcher_field = EXCLUDED.matcher_field,
                   matcher_text = EXCLUDED.matcher_text, configured_at = NOW()""",
            (user_id, field, value),
        )
        cur.execute('INSERT INTO salary_source_events (user_id, transaction_id) VALUES (%s, %s)', (user_id, transaction_id))
    conn.commit()
    return {'field': field, 'value': value}


def get_source(conn: Any, user_id: str) -> dict[str, str] | None:
    with conn.cursor() as cur:
        cur.execute('SELECT matcher_field, matcher_text FROM salary_sources WHERE user_id = %s', (user_id,))
        row = cur.fetchone()
    return {'field': row['matcher_field'], 'value': row['matcher_text']} if row else None


def candidates(conn: Any, user_id: str) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            f"""SELECT t.id, t.amount, t.date, t.merchant, t.notes,
                      (r.id IS NOT NULL AND r.revoked_at IS NULL) AS confirmed,
                      (r.id IS NOT NULL) AS previously_confirmed, r.confirmation_method
               FROM salary_sources s JOIN transactions t ON t.user_id = s.user_id
               LEFT JOIN categories c ON c.id = t.category_id AND c.user_id = t.user_id
               LEFT JOIN salary_receipts r ON r.user_id = t.user_id AND r.transaction_id = t.id
               WHERE s.user_id = %s AND {reportable_income_sql()} AND t.movement_id IS NULL AND t.date <= NOW()
                 AND lower(regexp_replace(trim(COALESCE(CASE WHEN s.matcher_field = 'merchant'
                       THEN t.merchant ELSE t.notes END, '')), '[[:space:]]+', ' ', 'g')) = lower(s.matcher_text)
               ORDER BY t.date DESC, t.id DESC LIMIT 100""",
            (user_id,),
        )
        result = []
        for row in cur.fetchall():
            item = dict(row)
            invalid_detection = item['confirmed'] and item['confirmation_method'] == 'detected' and not has_explicit_salary_marker(item['merchant'], item['notes'])
            item['confirmed'] = bool(item['confirmed'] and not invalid_detection)
            item['rejected'] = bool(item['previously_confirmed'] and not item['confirmed'] and item['confirmation_method'] == 'owner' and not invalid_detection)
            item['needs_review'] = not item['confirmed'] and not item['rejected']
            result.append(item)
        return result


def receipts(conn: Any, user_id: str) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            f"""SELECT t.id, t.amount, t.date, t.merchant, t.notes, r.confirmed_at, r.confirmation_method
               FROM salary_receipts r JOIN transactions t ON t.id = r.transaction_id AND t.user_id = r.user_id
               LEFT JOIN categories c ON c.id = t.category_id AND c.user_id = t.user_id
               WHERE r.user_id = %s AND r.revoked_at IS NULL AND {reportable_income_sql()}
                 AND t.movement_id IS NULL AND t.date <= NOW()
               ORDER BY t.date DESC, t.id DESC""",
            (user_id,),
        )
        return [dict(row) for row in cur.fetchall()
                if row['confirmation_method'] == 'owner' or has_explicit_salary_marker(row['merchant'], row['notes'])]


def invalidated_detections(conn: Any, user_id: str) -> list[dict[str, Any]]:
    """Expose edited automatic confirmations for review even after source matching changes."""
    with conn.cursor() as cur:
        cur.execute(
            f"""SELECT t.id, t.amount, t.date, t.merchant, t.notes
                FROM salary_receipts r
                JOIN transactions t ON t.id = r.transaction_id AND t.user_id = r.user_id
                LEFT JOIN categories c ON c.id = t.category_id AND c.user_id = t.user_id
                WHERE r.user_id = %s AND r.revoked_at IS NULL
                  AND r.confirmation_method = 'detected' AND {reportable_income_sql()}
                  AND t.movement_id IS NULL AND t.date <= NOW()
                ORDER BY t.date DESC, t.id DESC""",
            (user_id,),
        )
        return [dict(row) for row in cur.fetchall()
                if not has_explicit_salary_marker(row['merchant'], row['notes'])]


def confirm(conn: Any, user_id: str, transaction_id: str, *, method: str = 'owner') -> bool:
    if method not in {'owner', 'detected'}:
        raise ValueError('invalid salary confirmation method')
    with conn.cursor() as cur:
        cur.execute('SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))', (f'salary:{user_id}',))
        row = _eligible_income(cur, user_id, transaction_id)
        if method == 'detected':
            cur.execute('SELECT matcher_field, matcher_text FROM salary_sources WHERE user_id = %s', (user_id,))
            source = cur.fetchone()
            observed = ' '.join((row[source['matcher_field']] or '').split()) if source else ''
            if not source or observed.casefold() != source['matcher_text'].casefold() or not has_explicit_salary_marker(row['merchant'], row['notes']):
                raise SalaryError('salary_detection_stale', 409, 'Pemasukan berubah. Periksa sebelum konfirmasi.')
        cur.execute('SELECT id, revoked_at, confirmation_method FROM salary_receipts WHERE user_id = %s AND transaction_id = %s FOR UPDATE', (user_id, transaction_id))
        existing = cur.fetchone()
        if existing and method == 'detected':
            # An owner revocation must not be undone by an automatic retry.
            return False
        cur.execute(
            f"""SELECT r.id, r.confirmation_method, t.merchant, t.notes FROM salary_receipts r JOIN transactions t ON t.id = r.transaction_id AND t.user_id = r.user_id
               LEFT JOIN categories c ON c.id = t.category_id AND c.user_id = t.user_id
               WHERE r.user_id = %s AND r.revoked_at IS NULL AND {reportable_income_sql()} AND t.movement_id IS NULL
                 AND (t.date AT TIME ZONE %s)::date = (%s::timestamptz AT TIME ZONE %s)::date
                 AND t.id <> %s""",
            (user_id, settings.tz, row['date'], settings.tz, transaction_id),
        )
        conflicts = cur.fetchall()
        if any(item['confirmation_method'] == 'owner' or has_explicit_salary_marker(item['merchant'], item['notes']) for item in conflicts):
            raise SalaryError('salary_same_day_conflict', 409, 'Sudah ada gaji terkonfirmasi pada hari ini. Periksa transaksi tersebut.')
        if existing and existing['revoked_at'] is None:
            if existing['confirmation_method'] == 'owner':
                return False
            cur.execute("UPDATE salary_receipts SET confirmation_method = 'owner', confirmed_at = NOW() WHERE id = %s", (existing['id'],))
            cur.execute("INSERT INTO salary_receipt_events (user_id, salary_receipt_id, action, confirmation_method) VALUES (%s, %s, 'confirm', 'owner')", (user_id, existing['id']))
            conn.commit()
            return True
        cur.execute(
            """INSERT INTO salary_receipts (user_id, transaction_id, confirmation_method) VALUES (%s, %s, %s)
               ON CONFLICT (user_id, transaction_id) DO UPDATE SET revoked_at = NULL, confirmed_at = NOW(),
                   confirmation_method = EXCLUDED.confirmation_method
               RETURNING id""",
            (user_id, transaction_id, method),
        )
        receipt_id = cur.fetchone()['id']
        cur.execute("INSERT INTO salary_receipt_events (user_id, salary_receipt_id, action, confirmation_method) VALUES (%s, %s, 'confirm', %s)", (user_id, receipt_id, method))
    conn.commit()
    return True


def revoke(conn: Any, user_id: str, transaction_id: str, *, method: str = 'owner') -> bool:
    with conn.cursor() as cur:
        cur.execute('SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))', (f'salary:{user_id}',))
        cur.execute('UPDATE salary_receipts SET revoked_at = NOW() WHERE user_id = %s AND transaction_id = %s AND revoked_at IS NULL RETURNING id', (user_id, transaction_id))
        row = cur.fetchone()
        if not row:
            return False
        cur.execute("INSERT INTO salary_receipt_events (user_id, salary_receipt_id, action, confirmation_method) VALUES (%s, %s, 'revoke', %s)", (user_id, row['id'], method))
    conn.commit()
    return True


def reject(conn: Any, user_id: str, transaction_id: str) -> bool:
    """Record an owner's decision that an eligible income is not salary."""
    with conn.cursor() as cur:
        cur.execute('SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))', (f'salary:{user_id}',))
        _eligible_income(cur, user_id, transaction_id)
        cur.execute('SELECT id, revoked_at, confirmation_method FROM salary_receipts WHERE user_id = %s AND transaction_id = %s FOR UPDATE', (user_id, transaction_id))
        previous = cur.fetchone()
        if previous and previous['revoked_at'] is not None and previous['confirmation_method'] == 'owner':
            return False
        cur.execute(
            """INSERT INTO salary_receipts (user_id, transaction_id, revoked_at, confirmation_method)
               VALUES (%s, %s, NOW(), 'owner')
               ON CONFLICT (user_id, transaction_id) DO UPDATE
               SET revoked_at = NOW(), confirmation_method = 'owner'
               RETURNING id""",
            (user_id, transaction_id),
        )
        receipt_id = cur.fetchone()['id']
        cur.execute("INSERT INTO salary_receipt_events (user_id, salary_receipt_id, action, confirmation_method) VALUES (%s, %s, 'revoke', 'owner')", (user_id, receipt_id))
    conn.commit()
    return True


def detect_for_user(conn: Any, user_id: str) -> dict[str, int]:
    """Confirm clear matches; ambiguous and previously revoked receipts remain for review."""
    matches = candidates(conn, user_id)
    zone = ZoneInfo(settings.tz)
    day_counts = Counter(item['date'].astimezone(zone).date() for item in matches)
    # The oldest visible day may be truncated by the candidate limit; never auto-confirm it.
    incomplete_day = matches[-1]['date'].astimezone(zone).date() if len(matches) == 100 else None
    detected = review = 0
    for item in matches:
        day = item['date'].astimezone(zone).date()
        ambiguous = day_counts[day] != 1 or day == incomplete_day
        if item['confirmed'] and item['confirmation_method'] == 'detected' and ambiguous:
            revoke(conn, user_id, str(item['id']), method='detected')
            review += 1
            continue
        if item['previously_confirmed']:
            if item['needs_review']:
                review += 1
            continue
        if ambiguous or not has_explicit_salary_marker(item['merchant'], item['notes']):
            review += 1
            continue
        try:
            detected += int(confirm(conn, user_id, str(item['id']), method='detected'))
        except SalaryError as error:
            if error.code not in {'salary_same_day_conflict', 'salary_detection_stale'}:
                raise
            review += 1
    return {'detected': detected, 'review': review}


def process_sources(after: str | None = None, *, limit: int = 20) -> str | None:
    """Process one bounded page of owners; return the cursor for the next page."""
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT user_id FROM salary_sources WHERE (%s::uuid IS NULL OR user_id > %s::uuid) ORDER BY user_id LIMIT %s', (after, after, limit))
            owners = [str(row['user_id']) for row in cur.fetchall()]
    for user_id in owners:
        try:
            with db_conn() as conn:
                detect_for_user(conn, user_id)
        except Exception as error:
            # A broken owner must not starve other owners; avoid logging their data.
            logger.warning('Salary detection failed for an owner: %s', type(error).__name__)
    return owners[-1] if len(owners) == limit else None
