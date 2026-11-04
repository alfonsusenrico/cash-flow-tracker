# Tasks

This plan waits for the owner's approval (proposal.md, "Approval"). Evidence for every task goes into `verification.md`: commands, counts, skips with reasons, screenshots and measured values.

**Data.** Use synthetic data only:
- the phase seeds for regression;
- the preview seed shaped like the owner's account (`cash-flow-tracker-prototypes/2026-11-04-workspace-refinement/src/seed_owner.py`) for every browser check of this change (design D12).

**Backend tests** run as `.github/workflows/checks.yml` does, on a disposable PostgreSQL 16 migrated by Flyway, with `REQUIRE_TEST_DATABASE=1`.

**The browser matrix** means 1440, 1280, 1024, 768 and 390 px, each in light and dark, on a local production build.

**Local processes.** Every one is started on this change's own ports and stopped by PID or port. Each verification step ends with `ps`, `lsof` and `docker ps` output showing nothing left.

## 1. Baseline

- [x] 1.1 On `feat/workspace-refinement` before any change, record:
  - `npm test`, type-check, lint and build;
  - the backend suite;
  - first-load JS per route;
  - medians of 20 calls of `GET /api/accounts`, `GET /api/transactions` (summary) and `GET /api/dashboard/cycle-summary`;
  - screenshots of every route at 1440 dark, 1440 light and 390 dark on the preview seed.

  Evidence: the baseline table in verification.md.
- [x] 1.2 Run the phase 5 unchanged-writes harness on a `main` build with the preview seed: every Rekening and Target & Tagihan form, and the account and pocket reorder. Evidence: stored request bodies and table snapshots.

Expected Result: a measured starting point and stored reference writes.

## 2. Backend (additive)

- [x] 2.1 Add `spending` and `fresh_saving` to `GET /api/dashboard/cycle-summary`, each with lines that add up (D4). Tests:
  - `spending + fresh_saving = keluar`;
  - `remaining = masuk − keluar − scheduled` unchanged;
  - a saving-classified expense lands in `fresh_saving`, not `spending`;
  - another owner's rows are excluded.

  Evidence: pytest node ids and counts.
- [x] 2.2 Add `spending` and `saving` to the ledger summary and `spending` to `daily_totals` (D4, "Ledger Spending Figure"). Tests:
  - `outflow = spending + saving`;
  - the day sums equal the summary across three pages;
  - the summary equals the cycle summary's `spending` for the open cycle;
  - existing fields unchanged (snapshot of a pre-change response).

  Evidence: pytest counts.
- [x] 2.3 Add `group_capital_gain` and `group_cost_basis` on parent investment accounts (D8). Tests:
  - the platform total equals the sum of the products' server gains;
  - a product without a cost basis gives `null`;
  - liquid parents carry no group fields;
  - owner scoping.

  Evidence: pytest counts.
- [x] 2.4 Full backend suite on Flyway-migrated PostgreSQL 16. Evidence: pass, fail and skip counts tied to the commit.

Expected Result: the API carries every figure the new pages name, and no existing field changes.

## 3. Palette, type and navigation

- [x] 3.1 Set the pre-redesign values on the existing tokens in both themes (D1). Restore Plus Jakarta Sans with its licence and remove the Geist sans file (D2). Extend `tokens.test.ts` with the value table and keep every contrast pair. Evidence: Vitest counts, the list of derived tokens, and the font files in the build output.
- [x] 3.2 Sidebar and Lainnya (D3):
  - seven links without groups;
  - the coral active fill;
  - the foot holds Pengaturan and the account card;
  - no companion status or Bantuan;
  - Lainnya without Tanya and Bantuan.

  Tests: the navigation tests rewritten to the new order and the rail. Evidence: Vitest counts.
- [x] 3.3 Retire Tanya (D10):
  - `/tanya` answers 308 to `/`;
  - delete `app/tanya`, `components/tanya` and `lib/tanya` with their tests;
  - keep `lib/capture.ts` and Catat;
  - remove the copy guard's Tanya allowlist.

  Evidence: the Vitest redirect test, plus a grep showing no import of `lib/tanya`.
- [x] 3.4 Pengaturan: the companion status at the top of HP & perangkat, and "Bantuan & pintasan" in Tampilan. `?` still opens help. Evidence: Vitest counts.

Expected Result: the shell looks and reads like the preview, and nothing links to Tanya.

## 4. Pages

- [x] 4.1 Beranda (D5):
  - KPI cards with "Aman dipakai sampai gajian" from `remaining`;
  - no written summary, insight banner or same-date chip;
  - "Uang siklus ini" with four segments;
  - "Perlu tindakan" with the waiting amount;
  - the Kakeibo card with Pengeluaran total and Tabungan apart;
  - a full Ketahanan Dana card;
  - the phone order.

  Tests:
  - every KPI figure equals its API field;
  - the unknown-salary and over-budget states;
  - no "vs … hari pertama" text;
  - the one-column order;
  - axe.

  Evidence: Vitest counts.
- [x] 4.2 Transaksi (D6):
  - one toolbar row with the Filter popover and the "⋯" menu;
  - labels Pengeluaran;
  - KPI and day headings from `spending`.

  Tests:
  - every existing filter reachable in the popover;
  - the active-filter count;
  - removable chips;
  - URL writes unchanged;
  - keyboard access to the popover and the menu.

  Evidence: Vitest counts.
- [x] 4.3 Rekening & Kantong (D7–D9):
  - KPI cards;
  - "Komposisi aset" without debt;
  - account cards with pockets inside;
  - whole-row activation;
  - centered detail dialog, a sheet on phones, no scroll on open;
  - distinct default hues;
  - product and platform gains;
  - drag handles with move controls, and "Ubah urutan" on phones.

  Tests:
  - reorder by button and by drag sends the same `POST /api/accounts/reorder` sequence as phase 5 for the same moves;
  - a move is announced and keeps focus;
  - "Modal belum dicatat";
  - the dialog returns focus.

  Evidence: Vitest counts.
- [x] 4.4 Target & Tagihan (D11): three equal cards, then goals at full width, with details in centered dialogs. Tests: the empty bills state, the unknown essential cost, axe. Evidence: Vitest counts.
- [x] 4.5 Copy and docs:
  - the copy guard rejects "Keluar" and "Belanja" as figure labels ("Spending Terms");
  - AGENTS.md (palette, terms, Tanya retired);
  - `docs/NEW_USER_GUIDE.md`.

  Evidence: Vitest counts and the diff.

Expected Result: every page in scope matches the preview in content and order, with the money identities intact.

## 5. Verification

- [x] 5.1 Frontend `npm ci`, type-check, lint (0 warnings), `npm test` (0 unhandled errors) and build. Gate: JS per route at most +5 kB gzip against 1.1. Evidence: counts tied to the commit.
- [x] 5.2 Preview match (D12, gate 1): screenshots on the preview seed at 1440 dark, 1440 light and 390 dark, beside the approved preview images. Each difference is fixed or recorded with its reason. Evidence: a local comparison page in the scratchpad, plus the deviation list.
- [x] 5.3 Browser matrix on the changed routes, the dialogs, the Filter popover and Lainnya: axe serious 0, overflow 0, console errors 0, targets, focus not obscured, 200% and 400% zoom, reduced motion. Evidence: counts per cell.
- [x] 5.4 Unchanged writes (gate 3): the 1.2 harness on this build. Every form and reorder flow equals `main`. Evidence: the comparison output.
- [x] 5.5 Money identities in the UI (gate 4) on the seed, with and without a saving-classified expense:
  - Beranda's four segments add up to Masuk;
  - Transaksi Pengeluaran equals Beranda Pengeluaran;
  - the platform gain equals its products;
  - Komposisi aset adds up to Total aset.

  Evidence: the values read from the UI.
- [x] 5.6 Speed (gate 5): the three API medians within +10% of 1.1. Evidence: the table.
- [x] 5.7 Regression of every route against a `main` build (gate 6): differences come only from the palette and the planned pages. Evidence: screenshot pairs and the list.
- [x] 5.8 `openspec validate --all --strict` exits 0 before and after an archive rehearsal on a scratch copy, and `git diff --check` is clean. Evidence: the command output.
- [x] 5.9 Clean-up (gate 7): `ps`, `lsof` and `docker ps` show nothing left from this change. Evidence: the output.

Expected Result: the change is proven locally on data shaped like the owner's account.

## 6. Acceptance

- [ ] 6.1 Present to the owner:
  - verification.md;
  - the comparison page against the approved preview;
  - the deviations;
  - OD80–OD87, and OD96–OD98 with their applied defaults.

  The owner looks at the result and accepts it. Record the acceptance with the owner's words and the date in proposal.md. Evidence: the quote.

## Workflow follow-up

- After acceptance:
  - archive this change on the branch with `openspec archive refine-workspace-layout -y`;
  - write a real `## Purpose` for any new main spec;
  - confirm `openspec validate --all --strict` exits 0;
  - push the branch and report the compare link.
- The owner opens and merges the pull request. The release runs only through `deploy.yml`.
- After the deploy, check production read-only (health, the new fields answer 401 without a session, the frontend build has no `/tanya` page) and delete the branch locally and on the remote.
