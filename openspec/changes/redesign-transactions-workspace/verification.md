# Verification

The verification record of `redesign-transactions-workspace`. It uses synthetic data only (owners `beranda_contoh`, `pemilik_lain` and `volume_contoh`, seeded by the session scratchpad's `seed.py`, `seed_triage.py` and `volume_seed.py`). Each result names the commit it was measured at. Everything ran locally: a disposable PostgreSQL 16 (`cft-tx-pg`, Flyway V1–V44), the API on 127.0.0.1:8194 (8192 belongs to another project's container), a production `next start` behind the loopback proxy on 3192, and headless Brave over CDP on 9492.

## 1. Baseline (task 1.1, at `28826f1`, the approved plan)

| Check | Result |
| --- | --- |
| `npm test` | 64 files, 604 passed |
| `npm run type-check` / `lint` / `build` | pass / 0 errors, 0 warnings / pass |
| `/ledger` first-load JS (gzip) | 253.4 kB (route's own 28.5 kB) |
| Shared first-load JS (gzip, 13 scripts every route loads) | 224.9 kB |
| Backend suite (pass / fail / skip) | 1630 / 0 / 0 |
| `GET /api/transactions` median of 20, 5 000 rows over 18 months | no filter 12.5 ms, `period=cycle` 8.4 ms, `logical_movements=true` 12.7 ms |
| axe on `/ledger`, the edit modal and `QuickCaptureModal` (1280, 390; light, dark) | 0 violations in all 12 cells, overflow 0, console errors 0 |

## 2. URL defect (tasks 1.2, 3.1)

The before column is the build of `28826f1` with a seeded backend and a real session (`url_defect.mjs`). The after column is the build of `d5ae894`. "Requests" counts list requests after the click.

| Step | Before (URL after, requests) | After |
| --- | --- | --- |
| "Semua" from `/ledger?type=income` | `/ledger?type=income`, 0 | `/ledger`, 1 |
| Sidebar "Transaksi" from `/ledger?type=income` | `/ledger?type=income`, 0 | `/ledger?period=cycle`, 1 |
| Type chip from plain `/ledger` | `/ledger?type=expense`, 1 | `/ledger?type=expense`, 1 |
| Period from plain `/ledger` | `/ledger?period=cycle`, 1 | `/ledger?period=cycle`, 1 |
| Stepper back from `/ledger?period=cycle` | `/ledger?period=cycle`, 0 | `/ledger?period=cycle&offset=-1`, 1 |
| Back to all types from `/ledger?period=cycle&type=expense` | unchanged, 0 | `/ledger?period=cycle`, 1 |
| Laporan basis from `/laporan?basis=month` | unchanged, 0 | `/laporan?basis=cycle`, 1 report request |
| Next.js 16.2.12 (newest 16.2 patch) on a scratch copy | the same seven failures as 16.2.6 | no version bump proposed (D13) |

A second finding during the browser run: passing Next's own history keys (`__NA`) to `replaceState` makes Next.js treat the call as its own and skip the `useSearchParams` sync, so the URL changed and the list did not. `replaceSearch` now passes `null` (or its own `ledgerSheet` state); the after column is measured with that fix (`d5ae894`).

## 3. Backend (tasks 2.1–2.6, at `2ca5c01`)

New tests, all passing:
- **Day totals, tab counts, `local_date` and order** (`test_ledger_day_totals.py`, 17 tests): day totals add up to the period's inflow and gross outflow across pages of 2; the KPI outflow equals gross minus the reimbursement offset (Rp 167.000 − 10.000); a split day carries the same totals on both pages; a movement counts once; the 23.30 / 00.30 local-day boundary; a waiting row counted in its day; tab counts not changed by the tab itself; owner isolation; an unknown `include` → 422; 120 rows with the same `date` and `created_at` paged without repeats or gaps; the response shape without `include`; one row per movement within a period and on the Pindah saldo tab; a fixed statement count.
- **New filters:** `waiting=true` equals `review/summary.attention` (3 rows: needs review, an awaiting leg, an open question); the Investasi tab after a conversion through `/investment-topups/from-transaction`; Pindah saldo excludes top-up legs; `type=transfer` returns the same rows as the old SQL condition.
- **Detail:** `waiting`, `local_date` and `origin` (notification text, typed capture text); another owner's row is 404.
- **Capture parse** (`test_capture_preview.py`, 13 tests): "parkir 5rb jam 2" at 15.00 reads Rp 5.000, 14.00 and the owner's remembered category; a named account is a hint only when it is the one candidate; missing and several amounts; an ambiguous time gives options and no `occurred_at`; no INSERT, UPDATE or DELETE statement runs, the provider mock is never awaited, and the row counts of five tables are unchanged; another owner's memory and accounts never appear; text bounds and a naive device time are 422; the own bucket allows 120 and then answers 429 while `/capture/text` still records; logs hold the user id, outcome and problem codes only.
- **Delete paths** (`test_drawer_delete_paths.py`, 3 tests): deleting a converted top-up restores the expense (same id, category, amount and idempotency key) and the product and funding balances; `DELETE /movements/{id}` on a top-up answers 409 `investment_topup_lifecycle_required`; a goal link moves no goal progress, so the delete confirmation names balance and debt effects only. Debt reversal on delete is covered by the existing `test_allocated_payment_edit_delete_and_idempotent_retry`.

| Measurement (median of 20, `volume_contoh`, final run at `7372576`; first run at `487d068` in brackets) | Result | Gate |
| --- | --- | --- |
| `GET /api/transactions` without `include` | 13.5 ms (12.8); `period=cycle` 8.3 ms (7.6); `logical_movements=true` 12.7 ms (12.8); baseline 12.5 / 8.4 / 12.7 | ≤ baseline + 10% |
| With `include=daily_totals,type_counts` | 14.8 ms (14.7); with `period=cycle` 9.4 ms (6.4); with `logical_movements=true` 15.7 ms (19.9) | < baseline + 25 ms |
| Statements per list request with both includes | 7: summary, two existing offset lookups, page, the existing allocation lookup, days, types; equal for 1 and 50 rows | fixed |
| `POST /capture/parse` | 3.2 ms (3.1) | < 50 ms |

- **Spec tidy (task 5.4):** "Purpose Answer Undo" is backed by phase 2's `test_review_undo.py::test_purpose_answer_undo_reopens_the_question_and_refuses_a_replay` (undo token on an applied answer, replay of an undone answer → 409 `purpose_answer_undone`), passing in the full suite.
- **Full suite** at `2ca5c01` on a freshly migrated V44 database with `REQUIRE_TEST_DATABASE=1`: 1664 passed, 0 failed, 0 skipped.

## 4. Frontend (tasks 3.1–5.6)

- **Vitest** at `7372576`: 68 files, 612 tests passed. New or rewritten: `app/ledger/page.test.tsx` (17: day groups and axe, split day and load more, load-more failure, no client sums without `daily_totals`, KPIs with the offset line, empty state, phone feed, URL writes back to plain `/ledger`, the period shared with Analisis, the drawer with check and undo, a waiting row, confirm-first delete of a top-up and of a row, inline edit PATCH, return to the Tinjau item and a tampered folder, a deleted row, J and K and typing in search), `lib/ledgerDays.test.ts`, `lib/urlState.test.ts`, new cases in `lib/ledgerFilters.test.ts`, `components/capture/CatatDialog.test.tsx` (6: preview without recording and axe, refusal, record and Urungkan, inline question, the Isi manual amount, failure keeps the text), Beranda's drawer and deleted-row cases, and `copyRegister.test.ts`.
- **Copy register guard:** `copyRegister.test.ts` scans the label-type sources (shell, Beranda, Tinjau, Transaksi, Catat, device pairing, their libraries); it fails on a planted "Udah dicek" in its fixture and passes on the sources. Survey of informal tokens in `frontend/src` (tests excluded): about 91 hits before; after, only the allowlisted conversation (Beranda's summary sentence and the connect-phone guide prose).
- **Bundle** (final build of `7372576`): `/ledger` 252.7 kB gzip (−0.7), shared 226.7 kB (+1.8), Beranda `/` 253.9 kB (+5.7, it now opens the shared drawer; the drawer's editors load on first use since `dbb2524`). Gates: `/ledger` ≤ +15 kB, shared ≤ +3 kB.
- **Removed code:** `rg "MobileLedgerFeed|TransactionDetailDialog|DetailDrawer|router\.replace\(" frontend/src` finds nothing.
- **Final CI-equivalent run** at `7372576`: `npm ci` 0, `type-check` 0, `lint` 0 errors and 0 warnings, `npm test` 68 files and 612 tests passed, `build` 0.

## 5. Browser

### Matrix (task 6.2, production build of `7372576`, `matrix_tx.mjs`)

60 cells (plus 2 zoom cells): `/ledger` list, the drawer (`?edit=<automatic Kedai Awan row>`), the Investasi tab, and Catat empty, with a preview ("parkir 5rb jam 2") and refused ("bayar parkir"), at 1440, 1280, 1024, 768 and 390 px in light and dark.

| Check | Result |
| --- | --- |
| axe serious or critical | 0 in all 60 cells (and 0 minor) |
| Horizontal overflow, clipped labels, console errors | 0, 0, 0 everywhere |
| Drawer mode | side column at 1440 and 1280, overlay at 1024 and 768, bottom sheet at 390 |
| List composition | the table where the list is wider than 760 px, the phone feed below (also while the side drawer narrows it) |
| Lowest contrast, light | tabs 9.25, chips 9.76, day headings 5.32, pills 5.0, amounts 5.84, captions 5.04, drawer facts 5.8, Catat 6.56 |
| Lowest contrast, dark | tabs 9.06, chips 11.15, day headings 6.35, pills 7.95, amounts 9.46, captions 5.65, drawer facts 6.35, Catat 9.5 |
| Targets under 24 px | none (the first run at `dbb2524` found the inline "Muat lagi" in "{k} lagi di hari ini"; it now has a 24 px minimum) |
| Zoom 200% (640 px) and 400% (320 px) at 1280 | phone feed, overflow 0, axe 0 |
| Reduced motion | the sheet has no animation and a 0 s transition |

Screenshots read: list, drawer and Catat states at 1280 and 390 in both themes, among them `matrix/drawer-1280-light.png`, `matrix/drawer-390-light.png` and `matrix/catat-preview-390-dark.png`.

- **Keyboard walk** (build of `2b50bfc`, `keys.mjs`): J, J, K move between row buttons; Enter opens the drawer and puts `edit` in the URL; Tab reaches "Tutup detail"; Esc closes it, removes `edit` and returns focus to the row; Z with nothing to undo says "Tidak ada yang dapat dibatalkan."; ⌘K focuses `#ledger-search`, where J types a letter; N opens Catat with focus in its field; ? opens the help, which lists "Di Transaksi". 0 console errors.

### Real-interface flows (task 6.3, build of `d5ae894`, `flows.mjs` on a reset seed)

| Flow | Before | After | After undo |
| --- | --- | --- | --- |
| 1. Day totals of all loaded pages (62 rows) vs the KPIs, this cycle | Σ day Belanja Rp 2.913.500 = gross outflow = KPI Belanja; Σ day Masuk Rp 9.800.000 = KPI Masuk; no "lagi di hari ini" left after loading all | | |
| 2. "Tandai sudah dicek" on Kopi Pagi, then Z | unreviewed 5 | 4 | 5 ("Dibatalkan: Kopi Pagi.") |
| 3. Top up Investasi from the drawer (Laundry Kilat Rp 72.000 → Reksa Dana A) | Dompet Digital 648.000, Reksa Dana A 6.532.590, Beranda saving 912.590 | 648.000, 6.604.590, 984.590 | 648.000, 6.532.590, 912.590 |
| 4. Minimarket Kita Rp 88.000 Keluar → Masuk → Keluar | Masuk 9.800.000, Belanja 2.913.500 | 9.888.000, 2.825.500 | back to 9.800.000, 2.913.500 |
| 5. Delete the Oktober Cicilan Laptop payment after the confirmation | debt remaining Rp 2.400.000; the confirmation names "Saldo Utama bertambah kembali Rp 600.000" and the debt | Rp 3.000.000 | no undo (OD6) |
| 6. Catat | "bayar parkir" refused with "Jumlah belum ada. Tulis jumlahnya, misalnya 5rb." and "Catat" disabled; "parkir 5rb jam 2" previews Rp 5.000 and the notes; 137 rows before and after the preview | the questions "Jam berapa?", "Dari rekening mana?", "Kategorinya apa?" answered inline; 138 rows; toast "Dicatat: Parkir Rp 5.000, Kemarin 14.00." | 137 rows |
| 7. Tinjau "Ubah" on a Perlu jawaban row, edit the notes, close | link `/ledger?edit=…&return=review&folder=jawab` | back on `/review?folder=jawab` with Kedai Awan selected; the saved edit reviewed the row, so Tinjau said it was already resolved and opened the folder's first item (D14) | |
| 8. Beranda: open Parkir, delete it | 8 recent rows, Keluar Rp 3.826.090 | 7 rows, Rp 3.821.090, focus on `#main` | |

0 console errors in the final flow run. An earlier run showed 404 refetches of a deleted row's detail and focus falling to the page body after a Beranda delete; both were fixed in `10ce99d` before this run.

### Copy register (task 6.3 item 9)

Screenshots of Beranda, Tinjau, Transaksi and Catat at 1280 and 390 px after the sweep (build of `7372576`) are in `copy/`; a scan of their headings, buttons, links, labels, pills, chips, captions and notices found no listed slang token outside the allowlist. Reading them led to `7372576`, which neutralised "Saranku", "Apa ini?" as the Tinjau folder pill and list label, and "buat" in Tinjau and Beranda. The remaining casual text is the allowlisted conversation (Beranda's summary sentence, the connect-phone guide prose) and server text (the purpose question "Apa ini? Rp45.000 dari …", the learned top-up suggestion).

### Regression (task 6.5)

`/`, `/review`, `/insights`, `/laporan`, `/accounts`, `/goals` and `/settings` at 1280 and 390 px, on this build (`d5ae894`, repeated at `7372576`) and on a build of `origin/main` (`861212b`): no console errors and no overflow on either; the only serious axe finding is the known `/accounts` `color-contrast` (2 nodes at 1280), present on both builds.

## 6. OpenSpec and diff

- `openspec validate redesign-transactions-workspace --strict`: valid.
- `openspec validate --all --strict` before the archive: 73 passed, 1 failed, the existing main-spec length warning of `notification-purpose-questions` that this change's split fixes at archive.
- Archive rehearsal on a scratch copy: `openspec archive redesign-transactions-workspace -y`, then the new main spec `ui-copy-register` gets its real `## Purpose` (tasks, workflow follow-up), then `openspec validate --all --strict`: 74 passed, exit 0.
- `git diff --check origin/main...HEAD`: clean.

## Not verified

- No phone, companion or production check. The companion does not call `GET /api/transactions`; its `PATCH`, `DELETE` and capture endpoints are unchanged (code reading).
- No screen reader run; semantics were checked with axe and role queries.
- The Android companion's own labels still use the casual register in places; its sweep is a later follow-up in the mobile code.
- Flow 7 did not land on the edited item itself, because the edit reviewed that row; landing on an item that is still waiting is covered by the Vitest return test only.

## For the owner's final check (after all phases)

- Transaksi: open it from the sidebar, step back a cycle, switch tabs and chips, and load more on a long month; the day headings should match what you expect for those days.
- Open a row on the phone and on the computer; try "Tandai sudah dicek" and Urungkan, "Top up Investasi" and Urungkan, and a delete after reading its confirmation.
- Catat from the phone tab and with N: one sentence with an amount, one without, and one with an ambiguous time.
- The wording: labels, buttons and messages should read as neutral Indonesian everywhere; the casual voice should remain only in the Beranda sentence, the connect-phone guide and the app's own questions.
