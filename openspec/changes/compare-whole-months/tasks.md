# Tasks

This plan waits for the owner's approval (proposal.md, "Approval") and for `refine-workspace-layout` to merge first. This branch is rebased onto that merge before any code (task 1.1).

Evidence for every task goes into `verification.md`.

**Data:** synthetic only. Use the preview seed shaped like the owner's account, `src/seed_owner.py` of the 2026-11-04 preview, extended to Juli–November. Use the phase seeds for regression.

**Backend tests:** run as `.github/workflows/checks.yml` does, on a disposable PostgreSQL 16 migrated by Flyway with `REQUIRE_TEST_DATABASE=1`.

**Local processes:** every one uses this change's own ports and is stopped by PID or port. Record `ps`, `lsof` and `docker ps` at the end.

## 1. Rebase and baseline

- [ ] 1.1 Rebase `feat/whole-month-comparisons` onto `main` after `refine-workspace-layout` merges:
  - refresh every MODIFIED block from the main specs, keeping every scenario name;
  - confirm that the ledger's `spending` field and the "Spending Terms" requirement exist.

  Evidence: `openspec validate compare-whole-months --strict` passes.
- [ ] 1.2 Record the baseline:
  - the backend suite;
  - Vitest, type-check, lint and build;
  - the medians of 20 calls of `GET /api/reports/monthly` for a completed and a running month on the preview seed;
  - first-load JS of `/laporan` and `/insights`;
  - screenshots of both pages at 1440 dark, 1440 light and 390 dark.

  Evidence: the baseline table.

Expected Result: a measured starting point on the merged layout.

## 2. Backend

- [ ] 2.1 Add `services/monthly_comparison.py` (D1–D7): grouped queries over one window, totals, previous, baseline, progress, pillars with categories, facts and history. Tests (gate 2):
  - Oktober's baseline is the mean of Jul–Sep, and Oktober is never in its own baseline;
  - 2 usable periods give "rata-rata 2 bulan"; 1 or 0 give no baseline;
  - a partial previous period gives `partial: true` and no change;
  - the running period has `progress` and no `change`;
  - the salary-cycle basis with a closed and an open cycle;
  - a category new this month;
  - the 23.30 local boundary;
  - rounding half up.

  Evidence: pytest node ids and counts.
- [ ] 2.2 Ledger equality (gate 1): for every history, previous and baseline period, Pengeluaran, Masuk and every pillar and category amount equal the `GET /api/transactions` summaries of the same window and filters. Run on the preview seed and the phase seeds. Evidence: pytest counts.
- [ ] 2.3 Facts (gate 3, D6):
  - each fact's amount equals its reference's ledger summary;
  - the threshold edge at Rp 49.999 and Rp 50.000;
  - a new category from Rp 100.000;
  - the running period gets only facts 3–5;
  - at most seven facts.

  Evidence: pytest counts.
- [ ] 2.4 Add the route `GET /api/reports/monthly/comparison`, and use the same object as `comparison` in `GET /api/reports/monthly`. Remove `_median` and the report's call to `period_comparison`. Tests:
  - 401;
  - 404 `period_not_found`;
  - 409 `cycle_unavailable`;
  - owner scope;
  - `/reports/monthly/latest` unchanged (snapshot);
  - `/dashboard/period-comparison` unchanged (snapshot).

  Evidence: pytest counts.
- [ ] 2.5 Speed (gate 5): the comparison median is under 150 ms on the preview seed with 6 months, the statement count is equal for 1 and 200 rows per period, and `/reports/monthly` is within +10% of 1.2. Evidence: the table.
- [ ] 2.6 Full backend suite. Evidence: pass, fail and skip counts tied to the commit.

Expected Result: whole-period figures that equal the ledger, with no same-day rule left in the report.

## 3. Laporan

- [ ] 3.1 Build the page from D8:
  - "Kualitas data" line;
  - KPI cards with previous and baseline chips, or progress on a running month;
  - the pillar and category table;
  - "Yang bisa diperbaiki" or "Sejauh ini";
  - "Pengeluaran per bulan";
  - "Tempat belanja terbesar";
  - compact "Laporan sebelumnya", budget (only when set), savings and income detail;
  - the reflection only when a note exists;
  - no written lead.

  Tests:
  - the running month shows no arrow or verdict word;
  - "belum lengkap" on a partial previous month;
  - "Rata-rata muncul setelah 2 bulan lengkap";
  - every amount links to its ledger filter;
  - the reflection is hidden without a note and shown read-only with "Ubah" when one exists;
  - axe.

  Evidence: Vitest counts.

Expected Result: Laporan answers "how did this month go against my usual months" at a glance.

## 4. Analisis Ringkasan

- [ ] 4.1 Build Ringkasan from D9:
  - KPI row;
  - running progress in total and per pillar;
  - "{last completed} vs rata-rata 3 bulan";
  - the daily chart;
  - compact bill, question and habit cards with "Rincian";
  - "Kategori & anggaran" unchanged;
  - the same-day marks and the increases and decreases card removed;
  - no request to `/dashboard/period-comparison`.

  Tests: figure equality with Beranda and Transaksi, no same-day text, the "Rincian" disclosure, axe. Evidence: Vitest counts.
- [ ] 4.2 Docs: `docs/NEW_USER_GUIDE.md` (Laporan and Analisis) and AGENTS.md (the whole-period rule). Evidence: the diff.

Expected Result: Analisis shows progress for the running period and a fair baseline for the last completed one.

## 5. Verification

- [ ] 5.1 Frontend `npm ci`, type-check, lint (0 warnings), `npm test` (0 unhandled errors) and build. Gate: `/laporan` and `/insights` JS at most +8 kB gzip. Evidence: counts tied to the commit.
- [ ] 5.2 Preview match (gate 6): Laporan (Oktober, and November running) and Ringkasan at 1440 dark, 1440 light and 390 dark on the preview seed, beside the approved preview images. Each difference is fixed or recorded with its reason. Evidence: a local comparison page and the deviation list.
- [ ] 5.3 Full matrix on `/laporan` and `/insights?tab=ringkasan`, plus 200% and 400% zoom (gate 7). Evidence: counts per cell.
- [ ] 5.4 Real-UI checks:
  - Oktober's KPI and baseline figures equal the API;
  - a fact's link opens a ledger whose summary equals the fact;
  - stepping to November shows progress only;
  - switching the basis to Siklus gaji recomputes.

  Evidence: the values read from the UI.
- [ ] 5.5 Regression of the other routes against a `main` build. Evidence: screenshot pairs.
- [ ] 5.6 `openspec validate --all --strict` exits 0 before and after an archive rehearsal, and `git diff --check` is clean. Evidence: the output.
- [ ] 5.7 Clean-up evidence (gate 8). Evidence: the output.

Expected Result: the change is proven locally on data shaped like the owner's account.

## 6. Acceptance

- [ ] 6.1 Present to the owner: verification.md, the comparison page, the deviations and OD88–OD95. The owner looks at the result and accepts it. Record the acceptance with the owner's words and the date in proposal.md. Evidence: the quote.

## Workflow follow-up

- **After acceptance:**
  - archive this change on the branch with `openspec archive compare-whole-months -y`;
  - write a real `## Purpose` for any new main spec;
  - confirm `openspec validate --all --strict` exits 0;
  - push the branch and report the compare link.
- **The owner** opens and merges the pull request. The release runs only through `deploy.yml`.
- **After the deploy:**
  - check production read-only: health, and the comparison route answers 401 without a session;
  - delete the branch locally and on the remote.
