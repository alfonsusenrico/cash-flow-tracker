## Context

The phase 3 pieces this builds on are:
- owner merchant memory (`merchant_memory`, keyed match);
- append-only `classification_feedback` with the owner-edit hooks (`record_owner_corrections`);
- `purpose_normalization`, which shows the strict-schema pattern for turning owner text into classification;
- the provider allowlist and redaction (`sanitize_text`);
- the LLM boundary: deterministic code owns amounts, timestamps and ledger effects.

Movements use `create_bilateral_movement` (equal legs).

## Decisions

### D1. Deterministic facts first (`capture_parsing.py`)

**Amount.** Exactly one money expression is required:
- digits with Indonesian separators: "10.000", "10000", "Rp10.000";
- shorthand: "10rb", "10 ribu", "25k", "1,5jt", "1.5 juta", "2jt".

The result must be an integer IDR above 0 and at most 1e12. Zero or several expressions means asking for the amount.

**Kind**, decided by a word table:
- `expense` by default;
- `income` for "terima", "dapat", "dibayar", "gaji", "refund", "dikembalikan", "transfer masuk";
- `movement` for "tarik tunai", "setor tunai", "pindah … ke …", "top up … ke …", "isi saldo".

**Date** (Asia/Jakarta): the device time by default. "kemarin" means minus 1 day, "tadi malam" yesterday evening, "tadi pagi" today morning. An explicit date ("tgl 3", "3/10") is accepted only when it is unambiguous and not in the future.

**Account mentions** are matched against the owner's active account and pocket names, plus learned aliases. "tunai", "cash" and "dompet" mean the Cash account. Bank or wallet words (bca, jago, gopay, shopeepay) resolve to the default pocket of that institution. A match on more than one account counts as unresolved.

*Rejected:* letting the model read amounts. That breaks the project's LLM boundary, and shorthand is cheap to parse exactly.

### D2. Classification order

1. **Owner memory.** The match key is the normalized merchant phrase: the text with the money, date and account tokens removed. A hit supplies the merchant, description, category, pillar and account.
2. **Model.** The `capture_normalization` method returns `merchant`, `description` (Indonesian and informative, never a placeholder), `type` (it can only confirm or flag the parsed kind), `category_id` and `kakeibo` (both from supplied ids), and `account_id` (only from supplied ids, with confidence). Each field also has a `confidence` from 0 to 1.

   The context it receives:
   - categories with their default pillars;
   - owner memory for close keys;
   - top-k similar past transactions, preferring owner-reviewed ones;
   - accounts with aliases;
   - the parsed facts.

   The input is sanitized and capped at 30 KB. Version: `capture-normalization-1`.
3. **Fallback** when the provider is unavailable or the output is invalid: no category is guessed, and the category is asked.

### D3. Account resolution (owner decision: learn, ask when unsure)

The account is resolved in this order:
1. an explicit mention;
2. a memory entry with an account for this key;
3. clear history: at least 3 of the last 5 captures or transactions with the same key used the same account;
4. a model suggestion with confidence ≥ 0.85 that also appears in that history.

Otherwise the app asks "Dari rekening mana?". The options are liquid accounts and pockets, most recently used first, at most 6.

For a movement, the source and the target each resolve the same way. "tarik tunai" fixes the target to Cash.

### D4. Gate and questions

A capture is recorded only when:
- the amount is certain;
- the kind is certain;
- the account (or both movement accounts) is resolved;
- the category and pillar come from memory or have model confidence ≥ 0.8 (setting `CAPTURE_CONFIDENCE_THRESHOLD`).

Otherwise exactly one question is asked, for the first missing field, in this order: amount, kind, account, then category with pillar.

| Question | Options |
|---|---|
| Amount | Free numeric text, parsed by D1 |
| Kind | Pengeluaran / Pemasukan / Pindah saldo |
| Account | Account chips |
| Category | Up to 5 ranked category chips, each with its pillar, plus "Lainnya", which opens the full list |

Each answer is validated, and the capture continues, possibly to the next question. The question id and the reply id make answers idempotent; a stale question returns 409 `stale_capture_question`.

### D5. Recording and provenance

| Kind | How it is recorded |
|---|---|
| Expense or income | One transaction with `source='capture'`, `notes` = description, `merchant`, category, pillar (expenses only) and date. Idempotency key `capture:{capture_id}`. |
| Movement | `create_bilateral_movement` with the movement provenance, equal legs, and keys `capture:{id}:out` / `:in`. |

How the row is reviewed and classified:

| Case | `review_status` | `classified_by` |
|---|---|---|
| Recorded confidently without questions | `reviewed` (owner decision) | `rule` for a memory hit, otherwise `ai` |
| Recorded after the owner answered a classification question | `reviewed` | `owner` |

Learning happens through `record_owner_corrections`, with the feedback kind `capture` and the context features (local weekday and hour, amount, account, key). Memory is upserted as `owner` when the owner answered or when the text itself named the merchant; otherwise as `ai`.

### D6. Undo (owner decision: 10 seconds)

`undo_until` = recorded_at + 10 s. The server accepts the undo until undo_until + 5 s, to cover network latency, and only if the transaction is unchanged since recording.

Undo deletes the transaction (or both movement legs) through the existing delete path, which already reverses balances, and marks the capture `undone`. After the window the request returns 409 `undo_expired`.

### D7. Storage and privacy

`text_captures` keeps the owner's text, because it is the owner's own note, together with the status, draft, question, transaction and model metadata. Logs carry ids, status and reason codes only, never the text or amounts.

### D8. Limits

- Text: 1–200 characters, validated at the boundary.
- One model call per capture plus at most one more after an answer that changes the merchant phrase.
- Rate limit: the existing per-user limiter, 30 captures per minute.

## Risks / Trade-offs

- [The model proposes a plausible but wrong category for a new merchant with confidence ≥ 0.8] → It is recorded as reviewed, as the owner decided. The owner can edit it, which teaches memory. The threshold is a setting.
- [Ambiguous shorthand ("1.5" with no unit)] → Treated as no money expression, so the amount is asked.
- [Several transactions in one sentence] → Detected by more than one money expression, so the app asks to send them separately.

## Implementation notes (2026-10-29)

These record how the decisions above were implemented where the text left room. None changes a money invariant.

- **N1. Memory account (D2, D3).** `merchant_memory` gains a nullable `account_id`. Only captures write it; a notification write without an account keeps the remembered one (`COALESCE`). Memory gives the account for any entry with the exact key; it gives the classification only for an owner entry (the existing `owner_memory_hit` rule).
- **N2. Model schema (D2).** `normalize_capture` returns `merchant`, `description`, `type`, `type_confidence`, `category_id`, `kakeibo`, `category_confidence`, `account_id` and `account_confidence` under the strict schema `capture_normalization`. The gate uses the three confidences; a description such as "Pembayaran" is replaced by the parsed description. Unknown category or account ids make the output invalid, which falls back to asking the category.
- **N3. Kind flag (D2, D4).** When the model's `type` differs from a certain parsed kind, the kind is asked. The model never decides the kind.
- **N4. One model call (D8).** The merchant phrase comes from the first message only; an amount answer contributes only its money expression. So a capture makes at most one model call, within the D8 bound.
- **N5. Mentions win (D3).** An account mention that resolves to no usable account (for example "tunai" without a cash account, or an investment account) is asked, never replaced by memory or history. Several matches are offered first among the options.
- **N6. Answers.** Answers are stored on the capture row (`answers`) with their reply id; a replay with the same body returns the current capture, a different body under the same reply id is 409 `capture_answer_conflict`. "Lainnya" is answered with `account_id` or `category_id` (+ optional `kakeibo`) instead of an option id.
- **N7. Learning (D5).** A new row has no "before" state, so the capture calls the two functions `record_owner_corrections` is built on: `insert_feedback` (kind `capture`, `answer_text` = the typed text) and `upsert_memory`. The owner "answered" when a kind, account or category question was answered; an amount answer is not a classification.
- **N8. Undo (D6).** The transaction delete path and the movement delete path moved into `ledger_mutations` (`delete_transaction_rows`, `delete_movement_rows`) so the ledger endpoints and undo share them. "Unchanged" compares the recorded rows' columns, including `updated_at`, with a snapshot taken at recording. Undo also puts back the merchant memory entry the capture wrote, unless something changed that entry since; the append-only feedback row stays, and the capture row records `undone`.

## Open decisions for owner

- **O1. Movement provenance.** D5 says movements use "the movement provenance"; the provenance spec says every row recorded from typed text has `source = capture`. The legs are recorded with `source = capture`, `review_status = reviewed`, and `classified_by = rule` (the word table) or `owner` (the owner chose "Pindah saldo"). Alternative: `source = movement`, `classified_by = system`.
- **O2. Account answer and `classified_by`.** The row's `classified_by` follows who classified it (category and pillar), as in the D5 table. An answered account is recorded in the capture's answers and teaches memory as `owner`, but leaves `classified_by` as `ai` when the model gave the category. Alternative: any answer makes the row `owner`.
- **O3. Balance.** A typed capture records money already spent, so it is recorded even when it takes the ledger balance below zero (as notification-settled money is), instead of the manual form's 409 `insufficient_funds`.
- **O4. Explicit dates.** "tgl N" is the latest day N that is not after today. A day/month in the future, an impossible date, or two date expressions return 422 `invalid_capture_date` and store nothing, because D4 has no date question. Alternative: a date question.

## API contract

All endpoints accept the session cookie or the companion's Bearer API key, and answer with `{"ok": true, "capture": <capture>}`. Errors are `{"ok": false, "detail": {"code": "..."}}`.

| Endpoint | Body | Errors |
|---|---|---|
| `POST /api/capture/text` | `client_capture_id` (UUID), `text` (1–200 characters), `device_time` (ISO-8601 with offset, at most 10 minutes ahead) | 422 `invalid_capture_text`, `invalid_device_time`, `invalid_capture_date`; 409 `capture_id_conflict` (same id, other text); 429 `capture_rate_limited` |
| `POST /api/capture/{id}/answer` | `question_id`, `reply_id` (UUIDs) and one of: `text` (amount), `option_id` (kind, account or category option), `account_id`, `category_id` (+ optional `kakeibo`) | 404 `capture_not_found`; 409 `stale_capture_question`, `capture_answer_conflict`; 422 `invalid_capture_answer` |
| `POST /api/capture/{id}/undo` | none | 404; 409 `undo_expired`, `capture_transaction_changed`, `capture_not_recorded` |
| `GET /api/capture/{id}` | none | 404 `capture_not_found` |

`capture` has `id`, `client_capture_id`, `status` (`needs_detail`, `recorded` or `undone`), `text`, `question`, `summary` (`type`, `amount`, `date`, `description`), `transaction` (the expense, income or outbound leg), `movement` (`id`, `source`, `target` legs, or null), `created_at`, `recorded_at`, `undo_until` and `undone_at`. A `question` has `question_id`, `type` (`amount`, `kind`, `account` or `category`), `role` (`source` or `target` for accounts), `reason` (`missing`, `several` or `invalid` for amounts), `prompt`, `options` (`id`, `label`, plus `type` for accounts and `category_id`/`kakeibo` for categories), `allows_text` and `allows_other`.

## Migration Plan

V33 is additive:
- the table `text_captures`;
- `capture` in `chk_transactions_source` and in `classification_feedback_kind_check`;
- the nullable `merchant_memory.account_id` (N1).

Flyway is the only schema owner; the API runs no DDL. Roll back by reverting the code. The table, the extra constraint values and the extra column are harmless to the old code, which never writes them.
