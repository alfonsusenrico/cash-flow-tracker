# Verification: add-history-suggestions

Date: 2026-10-28. Branch `feat/history-suggestions` from 0950cc6 (phase 6 archived; merged into main as
7de01ac). All checks ran locally on the working tree before the commit. Synthetic data only. No
production access, no real model call (the provider is mocked in tests and disabled in the browser check).

## Backend

- Disposable PostgreSQL 16 (`cft-p7-db`, loopback port 55507), `ledger_test` dropped and recreated after
  the last code change, Flyway 10 applied V1→V31: "Successfully applied 31 migrations … now at version
  v31".
- `PYTHONPATH=backend TEST_DATABASE_URL=… SKIP_TEST_MIGRATIONS=1 pytest backend/tests -q -rs`:
  **904 passed, 0 skipped, 0 failed** (exit 0). Baseline on the base was 874 passed.
- New: `tests/test_history_suggestions.py` (28 tests incl. 5 invalid windows and 4 invalid decision
  bodies) and two provider tests in `tests/test_openai_notification_provider.py`
  (`test_history_suggestions_use_their_own_strict_schema_and_prompt`,
  `test_history_suggestions_reject_invalid_output`).
- No existing test changed. The benchmark import-isolation test (`evaluation.notification_benchmark`
  must not load the database modules) caught a module-level import of the suggestion service in the
  provider during development; the provider now imports it inside `suggest_history`.
- `git diff --check`: clean.

## Frontend

- `npm test`: **43 files, 205 tests passed** (exit 0); baseline on the base was 195.
  New: `components/review/HistorySuggestionsSection.test.tsx` (6, axe on the list, the open estimate and
  the empty state), `lib/historySuggestions.test.ts` (4). `app/review/page.test.tsx` (7) passes with the
  section mounted; its API mock answers the two new GET routes.
- `npm run lint`: exit 0. `npm run type-check`: exit 0. `npm run build`: exit 0 (`/review` prerendered).

## Browser (headless Brave over CDP)

Setup: uvicorn on 127.0.0.1:8017 against a separate `ledger_browser` database (V1→V31), AI disabled;
`next dev` on 127.0.0.1:3017. Seed (`seed_history.py`, scratch folder): a user registered through the API
(default categories), nine automatic rows inserted with generic names over the last seven weeks (three
"Pengeluaran makanan dan minuman" at Kedai Awan, two at Kopi Senja, two incoming "Transfer masuk tf
makan"/"… patungan" in "Pendapatan Lain", two "Belanja"), and one owner edit through the API that
teaches memory for Kedai Awan.

Final run (`browser-run3`, after the last code change):

- Keyboard from the top of `/review` (1280, light): 43 Tab presses reach "Hitung perkiraan" (focus
  visible: `:focus-visible`, 2 px outline); Enter shows the estimate "Perkiraan: 8 transaksi akan
  diperiksa · 2 cocok dengan ingatan Anda, 4 dengan aturan · AI tidak aktif: hanya saran dari ingatan dan
  aturan"; one Tab reaches "Mulai buat saran"; Enter starts the run; the live region reports "Pembuatan
  saran selesai." and the last run reads "6 transaksi diperiksa · 6 saran baru (2 dari ingatan, 4 dari
  aturan, 0 dari AI)" with the AI-disabled notice.
- Groups: Kedai Awan (memory), Kopi Senja (café rule → Jajan & Nongkrong, Keinginan), and two "Ganti uang
  patungan" groups (repayment rule; one with the meal offset, one without, because groups never mix
  offsets).
- Keyboard decisions: Tab to the first history "Terima semua (2)" (Kedai Awan), Enter → "Kedai Awan: 2
  saran diterima."; Tab to "Lihat 2 saran", Enter opens it, two Tabs reach "Tolak saran untuk
  Pengeluaran makanan dan minuman 20 Okt 2026", Enter → "Kopi Senja: 1 saran ditolak."
- Database after the run (`browser-run3-db.txt`): suggestions 2 accepted, 1 rejected, 3 pending; feedback
  2 `accepted_suggestion` + 1 `correction` (the seed edit); all three Kedai Awan rows in "Jajan &
  Nongkrong", owner-classified, reviewed, notes "Ngopi di Kedai Awan"; memory `merchant:kedai awan` owner.

| `/review` with suggestions | 390 light | 1280 light | 390 dark | 1280 dark |
|---|---|---|---|---|
| horizontal overflow | none | none | none | none |
| axe on the history section | 0 | 0 | 0 | 0 |

- Console errors: **0** across all page loads.
- axe-core 4 (WCAG 2.0/2.1/2.2 A and AA rules) on the whole page reports only the pre-existing findings
  listed in phase 6 (white on the coral active navigation pill 3.6:1 light / 3.33:1 dark; the phone
  bottom dock labels 2.19:1). None is in an element added or changed by this change.

Not done: a screen-reader pass (no assistive technology run), 200 % text and text-spacing overrides,
320 px width, and a run against a real model. This is not a conformance audit.

## OpenSpec

- `openspec validate add-history-suggestions`: valid (exit 0).
- `openspec validate --all`: 53 passed, 0 failed (exit 0).
- `--strict` on the change: exit 1 with 6 warnings, all "Requirement text is very long (>500
  characters)" on the new requirements. `--all --strict`: 44 passed, 9 failed — this change and eight
  main specs that already failed strict on the base for the same warning kind. Recorded as a follow-up;
  specs were not reshaped only to silence warnings.
- No other open change modifies `merchant-memory` or adds `history-suggestions`.

## Follow-ups

1. Split the long requirements flagged by `--strict`.
2. A first run against real history with the model enabled (owner, after deploy) to measure acceptance
   per source; the counts are on the run row.
3. Rejected suggestions as negative examples for phase 9 (open decision 3).
4. Re-pairing old one-sided transfers is out of scope; they stay in the phase 6 queue.
