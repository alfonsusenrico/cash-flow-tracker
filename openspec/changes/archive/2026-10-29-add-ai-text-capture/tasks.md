## 1. Schema and parsing

- [x] 1.1 V33: `text_captures` table and `capture` in `chk_transactions_source` / provenance SOURCES. Evidence: fresh V1→V33 migration; a constraint test accepts `capture`.
- [x] 1.2 `capture_parsing.py`: amounts, kind, dates, account mentions (D1). Evidence: table-driven unit tests for every spec example and the negatives (no amount, two amounts, ambiguous "1.5", future date).

Expected Result: money facts are deterministic and tested.

## 2. Classification, gate and questions

- [x] 2.1 Memory-first classification and the `capture_normalization` provider method with prompt `capture-normalization-1` (D2). Evidence: a memory hit makes no provider call (mock asserts); strict-schema validation tests; fallback on provider failure.
- [x] 2.2 Account resolution and the gate with the question order (D3, D4). Evidence: tests for each resolution source and each question type; idempotent answers; stale 409.
- [x] 2.3 Recording, movements, provenance and learning (D5). Evidence: DB tests for expense, income and "tarik tunai" (equal legs, not spending); feedback row and memory upsert; second capture needs no question.
- [x] 2.4 Undo window (D6). Evidence: undo at 4 s succeeds and restores balances; at 20 s or after an edit returns 409.
- [x] 2.5 Routes, boundaries, idempotency, rate limit and privacy-safe logs (D7, D8). Evidence: 422/404 tests; a resend returns the same capture; a log-capture test shows no text in logs.

Expected Result: the endpoints behave as specified on a fresh V1→V33 database.

## 3. Verification and delivery

- [x] 3.1 Full backend suite (baseline 910), `openspec validate --all --strict`, and `verification.md`.
- [x] 3.2 After owner acceptance: archive, push `feat/ai-text-capture`, and report the compare link. The companion UI follows in the mobile change for the chosen design.
