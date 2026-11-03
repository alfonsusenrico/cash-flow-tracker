# Verification: add-tanya-conversation

To be filled during implementation. Every check runs locally against disposable resources, with synthetic data only:
- PostgreSQL 16 container;
- the worktree API with AI off and outbound HTTP blocked;
- `next start` behind a loopback proxy;
- headless Brave over CDP.

No production host, phone or real model is used.

- **Resources:** Postgres container `cft-tanya-pg` on 127.0.0.1:55522 (databases `ledger_test` for pytest, `ledger_browser` for the browser); API on 8222 (`run_api.sh`: AI off, jobs off, outbound HTTP through a dead proxy); `next start` on 3223 behind `proxy.mjs` on 3222; headless Brave CDP on 9522.
- **Scripts and seed:** `scratchpad/web-p6/` (copied from `web-p3/` with ports and paths retargeted). `seed.py` creates the synthetic owners `beranda_contoh` and `pemilik_lain`; the seeded database is dumped once and restored with `reset_db.sh` before each flow run.
- **Base:** phase 3 is not merged yet. The branch is rebased onto its pushed tip `17e15bd` (`origin/feat/transactions-workspace`), and the baseline is measured at `137eb6b` (plan approval on top of it). It is rebased onto `main` once phase 3 merges.

Revisions: baseline `137eb6b`; revision under test and document-only commits are named in each section.

## 1. Prerequisites (task 1.1)

| Phase 3 contract | Expected (design) | Found on `main` | Drift and action |
| --- | --- | --- | --- |
| `POST /api/capture/parse` | `{text, device_time}` → `preview` with `amount`, `problems`, `kind`, `description`, `occurred_at`, `time_phrase`, account and category hints; own bucket 120 per 60 s | `routers/capture.py` `parse_capture_text`: exactly those fields plus `time_options`; `PARSE_RATE_LIMIT = 120`, `PARSE_RATE_WINDOW = 60`, 429 `capture_rate_limited`. Problem codes `missing_amount` and `several_amounts` | None |
| `CatatDialog` inline client | Types and helpers inside `CatatDialog.tsx` | `Preview`, `CaptureQuestion`, `Capture`, `deviceTime`, `newId`, `timeText`, `undoStillOpen` and the inline `api.post` calls are there. The `/capture/text` response also carries `transaction` (account, category, pillar, `review_status`) and `movement` | Moved types are named `CapturePreview`, `CaptureQuestion`, `Capture` (D6). The capture card reads `capture.transaction` for account and category; no contract change |
| `TransactionDrawer` open API | Opened with a row id | `TransactionDrawer({ transactionId, onClose, record, onPickPair?, onDeleted? })`; `record` comes from `useTransactionUndo()` | Tanya passes `record` from its own `useTransactionUndo()` and no `onPickPair` (no list to pick from) |
| Copy-register guard | A test with an allowlist mechanism | `frontend/src/copyRegister.test.ts`: `SLANG` token regex, `CONVERSATION` exact-string allowlist, `ROOTS` scope | Tanya adds `components/tanya`, `app/tanya` and `lib/tanya` (minus `replies.ts`) to `ROOTS`; `lib/tanya/replies.ts` stays out of scope as the assistant-voice module (D10) |
| `LEDGER_NAV_HREF` | Exported from `navigation.ts` | `"/ledger?period=cycle"` in `components/workspace/navigation.ts` | None |

## 2. Baseline (task 1.2)

| Check | Result |
| --- | --- |
| `npm ci` / type-check / lint / `npm test` / build | All pass at `137eb6b`. Lint 0 errors, 0 warnings. Vitest 68 files, 612 tests |
| First-load JS per route and shared chunk (raw, gzip) | Shared chunk 13 scripts, 226.7 KB gzip. Route totals (raw / gzip KB): `/` 844.3 / 253.9, `/review` 811.8 / 244.6, `/ledger` 835.7 / 252.7, `/accounts` 871.4 / 255.3, `/goals` 790.2 / 234.9, `/insights` 1195.1 / 345.4, `/laporan` 790.3 / 237.1, `/settings` 765.5 / 232.1. CSS 18.6 KB gzip |
| Backend suite (Flyway, `REQUIRE_TEST_DATABASE=1`) | Flyway V1 to V44 on a fresh database; 1664 passed, 0 failed, 0 skipped (111 s) |
| Sidebar 1440 and Lainnya 390 screenshots | `web-p6/baseline/nav-{1440,1024,390}-{light,dark}.png`: sidebar groups Harian, Uang, Laporan; Lainnya lists Rekening & Kantong, Target & Tagihan, Analisis, Laporan, Pengaturan |

## 3. Unit and component tests (tasks 2.1–4.8)

| Area | Tests | Result |
| --- | --- | --- |
| `lib/capture.ts` move (CatatDialog tests before and after) | `CatatDialog.test.tsx` unchanged; new `lib/capture.test.ts` | CatatDialog 6 passed before and 6 after; helpers 6 passed |
| Navigation | `workspace.test.tsx`: group order Harian, AI, Uang, Laporan with `/tanya`; Tanya active on `/tanya`; rail has 11 named items; Lainnya lists Tanya first; tab bar unchanged | 108 passed (6 files) |
| Intent table (phrases per intent, near misses, benchmark) | `lib/tanya/intents.test.ts`: 6 to 11 Indonesian and English phrases per intent (53 in total), 7 near misses, specific intents before `cycle`, bills `range`, normalisation, the 200-character cap, question markers, and a 2000-run benchmark under 1 ms per 200-character text | 14 passed |
| Resolver | `lib/tanya/resolve.test.ts` with the API mocked: each intent card and no capture request; help; failed card fetch is an error card; one amount records once with the message's id; Tinjau wording; `needs_detail` kept; four question phrasings never recorded; `missing_amount` and `several_amounts` ask and never submit; unknown text after exactly one parse call; mixed text offers the chip and the forced capture records; parse 429 and network failure keep the text; network failure then resend reuses the id; record 429; empty text | 23 passed |
| Thread store (no persistence, user reset, cap) | `lib/tanya/thread.test.ts`: spies on `Storage.prototype.setItem`, `document.cookie` and `indexedDB.open` record no write; reset on a new user id; 100-message cap drops the oldest; clear; update in place | 5 passed |
| Cards (figures equal fixtures, no "belum dihitung", hidden balances) | | |
| Capture in the composer | | |
| Accessibility (roles, focus, keys, axe) | | |
| Failure states | | |
| Copy-register guard (passes; planted token fails) | | |

## 4. Full checks (task 5.1)

| Check | Result |
| --- | --- |
| Frontend `npm ci` / type-check / lint / `npm test` / build | |
| Backend suite | |

## 5. No model call (task 5.2)

| Check | Result |
| --- | --- |
| Vitest whole-session allowlist | |
| Browser request log: requests per path, foreign hosts | |

## 6. Browser matrix on `/tanya` (task 5.3)

| Width | Theme | axe serious or critical | Overflow | Console errors | Lowest contrast | Small targets | Screenshot |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1440 | light | | | | | | |
| 1440 | dark | | | | | | |
| 1280 | light | | | | | | |
| 1280 | dark | | | | | | |
| 1024 | light | | | | | | |
| 1024 | dark | | | | | | |
| 768 | light | | | | | | |
| 768 | dark | | | | | | |
| 390 | light | | | | | | |
| 390 | dark | | | | | | |

Zoom 200% and 400%, reduced motion, and focus not obscured at 390:

## 7. Real-UI flows (task 5.4)

| Flow | Expected | Observed |
| --- | --- | --- |
| Card figures equal the API and Beranda | | |
| "parkir 5rb jam 2 tadi" records one row at 14.00 | | |
| Urungkan restores Beranda's Keluar | | |
| `needs_detail` answered in the thread | | |
| Ubah opens the drawer | | |
| "parkir tadi" records nothing | | |
| "berapa keluar 2 hari ini?" records nothing | | |
| "halo" gives chips, records nothing | | |
| Waiting links land on the exact Tinjau item | | |
| Reload empties; navigation keeps | | |
| Second user sees nothing of the first | | |
| API stopped: error cards, no 0 | | |

## 8. Regression and bundle (tasks 5.5, 5.6)

| Check | Result |
| --- | --- |
| Other routes against `main` (axe, overflow, console) | |
| Nav order at 1440, 1024 and 390 | |
| `/tanya` route JS over shared (gzip) | |
| Shared chunk change (gzip) | |
| `package.json` dependency diff | |

## 9. OpenSpec and diff (task 5.7)

- `openspec validate add-tanya-conversation --strict`:
- `openspec validate --all --strict`:
- `git diff --check`:

## Not verified

- The on-screen keyboard on a real phone (headless browsers have none). This goes on the end-of-redesign check list.
- Screen readers (semantics checked with role queries and axe).
