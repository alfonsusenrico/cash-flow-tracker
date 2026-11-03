# Verification: redesign-money-and-settings

Each entry names the commit or working-tree state it was measured on. Data is synthetic only: the phase 1–4 seed (owners `beranda_contoh` and `pemilik_lain`) and, for the unchanged-writes flows and OD57, the same seed plus `scratchpad/web-p5/d17_extra.sql` (a unit-tracked "Reksa Dana C", a "Kantong Minus" pocket at −Rp 50.000 waiting for reconciliation, a standalone goal "Laptop Contoh", one notification account mapping and two sender names).

## 1. Rebase and baseline (tasks 1.1–1.3)

- **Rebase:** onto `main` at `01d762f` (phases 3, 4 and 6 merged as PRs #65–#68). MODIFIED blocks checked against the main specs: every one of the 11 modified requirements is unchanged on `main` since `861212b`, so no block needed refreshing. `openspec validate redesign-money-and-settings --strict` passes.
- **Alias measurement after the rebase** (`p5-plan/alias_usage.py`):

  | Kind | Count | Largest consumers |
  | --- | --- | --- |
  | `var()` alias uses | 676 | accounts page 115, goals page 76, QuickCaptureModal 45, CategoryBudgetSection 45, RecurringRulesModal 36, ObservedInsights 29 |
  | Legacy Tailwind keys and palette remaps | 383 | accounts page 102, goals page 70, CategoryBudgetSection 56, RecurringRulesModal 24, ObservedInsights 22 |
  | Legacy utility classes | 6 classes: btn-primary 1 file, btn-secondary 3, cockpit-card 3, card-squircle 4, pressable 2, donut-ring 1 | |

- **Baseline** (tree `5b19456` = main + plan):

  | Check | Result |
  | --- | --- |
  | `npm test` / type-check / lint / build | 77 files, 716 tests passed / 0 / 0 warnings / ok |
  | First-load JS gzip | `/accounts` 254.7 kB, `/goals` 234.3 kB, `/settings` 231.5 kB, `/settings/profil` 231.5 kB, `/settings/perangkat` 246.2 kB |
  | Built CSS | 4 files, 114.208 bytes raw, 24.891 gzip (shared sheet 16.490 gzip) |
  | Backend suite | 1687 passed, 0 failed, 0 skipped |
  | `GET /api/dashboard/net-worth` median (20 calls) | 6.1 ms |
  | `GET /api/accounts` median (20 calls) | 3.5 ms |
  | Every route at 1280 / 390, light / dark (80 cells) | axe serious: only `/accounts` color-contrast (2 nodes, the known finding); overflow 0; console: the logged-out `/api/auth/me` 401 on the sign-in page, and 404 for the two new settings addresses that did not exist yet |

  Screenshots: `scratchpad/web-p5/baseline/`.
- **Unchanged-writes harness on `main`** (`harness.mjs main`, a copy of the main build kept in `web-p5/main-src`): 40 flows, each from a freshly restored database, each recording its non-GET requests and a normalised snapshot of every table (`snapshot.py`; new ids, timestamps and random secrets normalised). Every flow made exactly 1 write request. Both runs used the same phase 5 API, so the comparison isolates the web build. Recordings: `web-p5/d17-main/`.

## 2. Backend (tasks 2.1–2.4, at `572ec52` and `e62f570`)

| Area | Tests | Result |
| --- | --- | --- |
| Net worth helper (2.1) | `test_cycle_summary.py`: Rekening equals Beranda with a parent, two pockets, an amount product, a stock position, a −Rp 50.000 pocket waiting for reconciliation and one active debt; archived and paid-off debts left out; earlier keys kept; owner scoping; constant statement count | 4 new tests pass |
| `GET /api/habits` (2.2) | `test_habit_list.py`: ready / learning / conflicting, correction weight, undone label, two bands, "Tanpa nama toko · BCA" without the signature in the body, AI and movement labels excluded, 100-entry cut with `truncated`, other owner and 401, API key, read only (checksums of every table), no model provider, no merchant or amount logged, switch off, constant statement count for 10 and 400 labels, learned / ready / conflicting top-ups | 14 tests pass |
| Capture undo reverts its label (2.3) | `test_text_capture.py`: one `undo` row naming the capture row, label no longer live, label unchanged, replay writes no second row, a failure after the insert rolls it back with the rest, append-only trigger still refuses an update | 3 new tests pass; the other capture, suggestion and review-undo suites (100 tests) pass |
| Bills total | `scheduled.plans_total` equals the plan lines and excludes goal set-asides | 1 new test passes |
| Full suite (2.4) | Flyway V1–V44 on a fresh PostgreSQL 16, `REQUIRE_TEST_DATABASE=1` | **1709 passed, 0 failed, 0 skipped** (at `9f3ae78`) |

`GET /api/habits` with 1000 owner labels over 60 keys and 200 top-up answers (`habits_volume.sql`): median **13.1 ms** (p95 16.4). Statements are constant between 10 and 400 labels (test above).

## 3. Frontend (tasks 3.1–6.10)

| Area | Tests | Result |
| --- | --- | --- |
| Rekening & Kantong (3.1–3.4) | `app/accounts/page.test.tsx` (27): net worth failing shows "Kekayaan bersih belum dapat dihitung" with "Coba lagi" and no figure; server totals and lines; drawer actions per kind (liquid, pocket, product, position, platform); every form's request body (pocket, deposit, platform, colours, conversion, default pocket, move under a bank, rejected name, reconciliation, archive with confirmation and its error); twelve pockets as indented rows and folding; reorder buttons and drag for accounts and pockets with their bodies, failure restores the order, focus returns to "Ubah urutan"; reconciliation labels and the flag opening "Sesuaikan Saldo"; `?tampil=` and `?akun=`; counts of unfinished movements without amounts; hidden balances | 27 pass |
| Target & Tagihan (4.1–4.3) | `app/goals/page.test.tsx` (6): standalone-only progress without a transaction; confirmation before account backing; debt validation; payment link first; statuses in words; bills from the plan lines with the server's total; unknown runway; archived debts; `?target=` and archive confirmation | 6 pass |
| Pengaturan (5.1–5.4) | `app/settings/layout.test.tsx` (eight sections, Akun group, every address, focus), section tests unchanged in request bodies, `HabitsSettings.test.tsx` (Kebiasaan rows and status words, switch off, hidden amounts, empty and error; the Ringkasan card; Tampilan driving the shared theme, shortcut and hidden-balance state), `lib/utils.test.ts` (one greeting rule) | pass |
| Tokens and aliases (6.1–6.7) | `app/tokens.test.ts` (80): no removed alias declared or used, no legacy key or palette remap, no default palette class, no legacy utility, every semantic key built from a token declared in both themes, contrast pairs in both themes, drawers above the scrim. A planted `text-emerald-600 btn-primary var(--text-secondary)` made 4 tests fail; removed, all pass | pass |
| Copy register (6.8) | `copyRegister.test.ts` covers the money pages, Pengaturan, their modals and `/auth/login`, and forbids "Anda" there | pass |
| Whole suite | `npm test` at `95a4ac5` | **78 files, 710 tests passed** (716 before: the old card-layout and alias tests were replaced by the tests above), 0 unhandled errors; type-check 0; lint 0 warnings; build ok |

- **Alias removal:** `remove_aliases.py` made 912 replacements in 53 files (largest: components/accounts/AccountDialogs.tsx 127, components/analysis/CategoryBudgetSection.tsx 101, components/goals/GoalDialogs.tsx 95, components/recurring/RecurringRulesModal.tsx 63, components/ui/QuickCaptureModal.tsx 58, components/insights/ObservedInsights.tsx 51). Afterwards the measurement reads **0 `var()` alias uses and 0 legacy keys**; the alias block, its dark overrides, the legacy Tailwind keys and remaps and the six legacy utilities are deleted. The mapping table is in design.md, "Implementation notes".
- **Removed components** (no importer; `grep` finds 0 references afterwards): `DonutChart`, `FilterBar`, `MoneyInput`, `Sparkline`. `AppShell` and `PageHeader` still have importers and stay.
- **Label renames (old → new):** Sync Harga → Perbarui harga; Sinkronisasi… → Memperbarui harga…; Sync selesai: n akun diperbarui → Harga diperbarui: n produk; Gagal sinkronisasi harga pasar → Harga belum dapat diperbarui; Update Nilai → Perbarui nilai (also in two API error texts); Capital Gain → Untung/rugi; Rekening Baru / Tambah Rekening Baru → Tambah rekening; Target Baru → Tambah target; Tagihan Baru → Tambah utang; + Catat Progres → Tambah progres; Simpan Progres → Simpan progres; Perangkat & akses → HP & perangkat; section purposes without "Anda"; Dikonfirmasi Anda → Dikonfirmasi pemilik; Masuk ke akun Anda → Masuk ke akun; three form hints without "Anda"; "tidak dobel" → "tidak dihitung dua kali"; Kalau ketemu → Saat ditemukan.

## 4. Browser (tasks 7.2, 7.4, 7.6, production build of `95a4ac5` (matrix) and `3fa9789` (the 400% zoom fix))

### Matrix on `/accounts`, `/goals` and `/settings/*`

`web-p5/matrix.mjs`: 21 states (each page; a pocket, a position, a goal and a debt drawer; reorder mode; the new-account, reconcile, buy, new-goal and pay modals; every settings section and the index) × 1440, 1280, 1024, 768 and 390 px × light and dark = **210 cells**, D17 seed.

| Check | Result |
| --- | --- |
| axe serious and critical | 0 in all 210 cells |
| Horizontal overflow | 0 |
| Console errors | 0 |
| Every scripted open of a drawer or modal | reached (210 of 210) |
| Lowest measured contrast of text | 5.0 light, 5.65 dark |
| Targets under 24 px | only checkboxes and radios inside their 44 px labels (WCAG 2.5.8 label exception); the two "Isi cepat" buttons of the buy dialog were 21 px tall and now have a 24 px minimum |

The first run, at `0abe48a`-era CSS, showed the phone sheet and the overlay drawer under the page scrim (`z-index` 75 under 79, present since phase 1): taps on a sheet reached the scrim. Fixed at `95a4ac5` with a token test; a hit test of the sheet's first action now lands inside the drawer, and the whole matrix was rerun on that build. Screenshots: `web-p5/matrix/<state>-<width>-<theme>.png`, contact sheet `web-p5/sheet.png`.

### Keyboard, zoom and reduced motion

- **Keyboard walk at 1280** (`keyboard.mjs`): Tab reaches the row button, Enter opens the drawer with focus on its heading, Tab walks "Tutup detail", the primary action and every other action in order, Escape closes the drawer and focus returns to the row (Rekening: "Dompet Digital"; Target: "Liburan Lombok"). Reorder mode: "Turunkan Tunai" is reachable, and "Selesai" returns focus to "Ubah urutan".
- **Phone sheet at 390**: focus cycles inside the sheet (focus trap), and Escape returns it to the row.
- **Zoom**: 1280 at 200% and 400% as 640 and 320 CSS px, every state, both themes (84 cells): overflow 0, console 0. At 320 px axe found one target-size issue (the "Lihat Ketahanan Dana" link, 17 px tall next to "Buka Tinjau"); fixed at `3fa9789` and rechecked on all 28 Rekening cells at 640 and 320: 0 findings.
- **Reduced motion**: the new pages add no animation; the shared drawer and toast honour `prefers-reduced-motion` as in phases 1–3.

### Pixel comparison of every route against the baseline

`pixdiff.py` (share of pixels changed by more than 24/255, over the common area), 80 cells, `web-p5/pixdiff.txt`:

| Route | 1280 light | 1280 dark | 390 light | 390 dark | Explanation |
| --- | --- | --- | --- | --- | --- |
| `/review`, `/ledger`, `/insights?tab=kalender`, `/insights?tab=aliran` | 0.00% | 0.00% | 0.00% | 0.00% | unchanged |
| `/` | 0.24% | 0.24% | small | small | the lede greets by the display name (5.2a) |
| `/tanya` | 0.32% | 0.32% | 0.48% | 0.48% | the greeting's time of day ("Siang" → "Sore") between the runs |
| `/auth/login` | 0.05% | 0.05% | 0.18% | 0.17% | "Masuk ke akun" (copy register) |
| `/insights` (Ringkasan) | 6.29% | 4.36% | 10.55% | 8.27% | 6.10: the insight cards on workspace cards, and the new "Kebiasaan dihafal" card (5.3a) |
| `/laporan` | 7.56% | 4.62% | 12.49% | 9.56% | 6.10: the report cards on workspace cards |
| `/accounts`, `/goals`, `/settings/*` | | | | | redesigned in this phase |

Every route after the change, both themes and widths: **axe serious or critical 0** (the `/accounts` contrast finding is gone), overflow 0, console only the logged-out `/api/auth/me` 401 on the sign-in page (also on the baseline). Screenshots: `web-p5/after/`.

### Cross-page consistency (D17 seed, with the −Rp 50.000 pocket)

`consistency.py` reads the endpoints the pages show:

| Figure | Beranda | This page | Equal |
| --- | --- | --- | --- |
| Kekayaan bersih | Rp 48.155.000 (`cycle-summary.net_worth.total`) | Rp 48.155.000 (`net-worth.net_worth`, Rekening) | yes |
| Total aset (OD57) | liquid + investment Rp 50.605.000 | `total_assets` Rp 50.605.000; "Saldo minus perlu dicocokkan" −Rp 50.000 on its own line | yes |
| Bills until the next salary | plan lines of Terjadwal, Rp 995.000 | `scheduled.plans_total` Rp 995.000 (Target & Tagihan) | yes |
| Ketahanan Dana | 0,8 bulan | 0,8 bulan (same runway) | yes |
| After a Rp 150.000 top-up from "Utama" into "Reksa Dana A" | Rp 48.155.000 | Rp 48.155.000; the product rose by Rp 150.000 | yes |

A typed capture through the API ("parkir 5rb tunai") asked a follow-up question instead of recording, so the capture-and-undo leg of 7.6 was proven by the backend tests in §2 (the label is reverted and no reader counts it), not in this browser run; the habit list was unchanged.

## 5. Unchanged writes (task 7.3)

`compare.py d17-main d17-new` (`web-p5/d17-compare.txt`): **40 of 40 flows write the same requests and the same rows**, with one explained difference: in the top-up flow the client generates a fresh idempotency key per submit, so `transactions.idempotency_key` and `investment_topups.request_fingerprint` differ between the two runs (as between any two submits); every other column is equal. Request bodies are compared with the submit time kept to its day and idempotency keys masked.

| Flows | Requests equal | Rows equal |
| --- | --- | --- |
| Accounts: create bank, wallet, cash, investment parent, stock position; edit; pocket create; archive account and pocket; reorder accounts and pockets; valuation; reconcile; price sync (market blocked, no external request); buy; sell; top-up; amount tracking switch | 17 / 17 | 17 / 17 (top-up: the key above) |
| Plans: create, edit, toggle, delete | 4 / 4 | 4 / 4 |
| Goals and debts: goal create, edit (standalone), edit (account-backed, refused by the API on both builds, OD70), adjust, delete; debt create, edit, delete, payment record | 10 / 10 | 10 / 10 |
| Settings: profile, budget, salary source, pairing code, device revoke, API key reset, mapping removal, sender name edit and removal | 9 / 9 | 9 / 9 |

## 6. Gates (task 7.5)

| Gate | Baseline | After | Limit | Pass |
| --- | --- | --- | --- | --- |
| First-load JS `/accounts` | 254.7 kB | 252.4 kB | ≤ +10 kB | yes |
| First-load JS `/goals` | 234.3 kB | 239.1 kB | ≤ +10 kB | yes (+4.8, the plans editor loads on demand) |
| `/settings/kebiasaan`, `/settings/tampilan` vs `/settings/profil` | — | 234.3 / 232.8 vs 232.6 kB | within 10 kB | yes |
| `/settings` (now opens HP & perangkat, OD53) | 231.5 kB | 247.3 kB | — | equals `/settings/perangkat` (+1.1 kB against its baseline) |
| Built CSS, all files | 24.891 B gzip | 25.477 B gzip | not larger | **no, +586 B**: the shared sheet every page loads fell from 16.490 to 15.001 B gzip, and the money pages' module adds 2.549 B on those routes only (per-route CSS: 16.6 kB on Beranda and Transaksi, was 18.5; 19.1 kB on the money pages) |
| `/dashboard/net-worth` median | 6.1 ms | 5.6 ms | ≤ +10% | yes |
| `GET /api/habits` | — | 13.1 ms on the D7 dataset; constant statements | < 300 ms | yes |

## 7. OpenSpec and diff (task 7.1)

- `openspec validate redesign-money-and-settings --strict`: valid. `openspec validate --all --strict`: 79 passed, exit 0 (at `3fa9789`).
- `git diff --check origin/main...HEAD`: clean (at `3fa9789`).

## Not verified

- Production. Checked after the deploy and recorded then.
- Screen readers. Semantics were checked with axe, role queries and the keyboard walk.
- Real phone touch and drag: drag reordering was exercised with synthetic drag events in Vitest; the move buttons cover touch.
- Storage that throws for the Tampilan settings: the existing providers' try/catch covers it, but no browser run blocked storage.

## OD70 fix (2026-11-03)

- Cause: `GoalDialogs.tsx` sent `current_amount: 0` for account-backed goals; `PATCH /goals/{id}` refuses any `current_amount` when the goal ends up with linked accounts (409). Affected: editing an account-backed goal, and switching a standalone goal to linked accounts. Present on `main` too.
- Fix: the form omits `current_amount` for account-backed payloads. The API rule is unchanged.
- Regression: `app/goals/page.test.tsx` "changes the target of an account-backed goal without sending manual progress" and the corrected standalone-to-linked case both failed before the fix (`expected {…} to not have property "current_amount"`) and pass after. Backend `test_account_backed_goal_edit_changes_target_and_refuses_manual_progress`: the form's payload returns 200 with the new target, `current_amount` alone returns 409.
- Runs: backend 1710 passed on a fresh Flyway V44 PostgreSQL 16 (`SKIP_TEST_MIGRATIONS=1`, `REQUIRE_TEST_DATABASE=1`); Vitest 78 files, 711 tests, 0 unhandled errors; type-check and lint clean.
- D17: the goal edit and the standalone-to-linked switch are the one intended request-body change (no `current_amount`); every other flow stays identical.
