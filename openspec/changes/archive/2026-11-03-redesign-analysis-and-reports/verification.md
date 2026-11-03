# Verification: redesign-analysis-and-reports

Recorded 2026-11-03 on a developer machine with synthetic data only.

**Environment**
- Disposable PostgreSQL 16 container `cft-an-pg` on 127.0.0.1:55502, migrated by Flyway 10 from V1 to V44.
- API on 127.0.0.1:8202 with AI, background jobs and plan auto-link off.
- `next start` on 3203 behind the loopback proxy on 3202.
- Headless Brave with CDP on 9502.
- No production host, phone or real model was used.

**Data:** synthetic owners from the phase 1–3 seed scripts:
- `beranda_contoh`: the preview ledger, with salary cycles, pockets, goals, investments and waiting rows;
- `pemilik_lain`: for isolation;
- `volume_contoh`: 5 000 rows over 18 months, for timings;
- `laporan_kosong`: no rows, for the new-user state.

The backend tests create their own owners.

**Revisions**
- Baseline: `e85f30b` (the approved plan on phase 3's pushed tip `17e15bd`), measured before any change.
- Under test: the branch rebased onto `main` `0ba4c41`, which holds phase 3 and its archive. The backend suite ran at `1e79564`, and the frontend checks and browser runs at `b453ce8`.
- Regression build: `main` `0ba4c41`, built from a clean `git archive`.

## 1. Baseline (task 1.1)

| Check | Result |
| --- | --- |
| Backend suite, Flyway V1–V44, `REQUIRE_TEST_DATABASE=1` | 1664 passed, 0 failed, 0 skipped |
| `npm ci` / `npm test` | ok / 68 files, 612 tests passed |
| `npm run type-check` / `npm run lint` / `npm run build` | 0 / 0 errors, 0 warnings / ok |
| First-load JS, gzip | `/insights` 345.5 kB (raw 1195.1), `/laporan` 237.1 kB, shared by all routes 226.7 kB |
| `GET /dashboard/analytics`, median of 20 on `volume_contoh` | cycle 12.4 ms, month 7.8 ms |
| `GET /dashboard/period-comparison`, median of 20 | cycle 8.4 ms, month 6.1 ms |
| Legacy `/insights` (both tabs) and `/laporan` at 1280 and 390, light and dark | axe 0 violations, overflow 0, console errors 0 (`baseline/`) |

## 2. Shared helpers and additive fields (tasks 2.1–2.3)

| Check | Result |
| --- | --- |
| Beranda response before and after the move | `cycle-summary` on the seeded owner is equal key by key, from the base API and the branch API on the same database, `as_of` excepted. The existing 21 cycle-summary tests pass unchanged |
| `period-comparison.kakeibo` | `kakeibo.current` equals analytics' buckets and its total equals `total_allocated`, with a saving transfer of Rp 400.000; `previous` is null without previous data; `pillars` keeps its keys. The other fields are equal to the base API; only the `now`-based `end` instants differ |
| `analytics.explanations` | Keluar lines add up to `total_allocated` and Belanja lines (gross, reimbursement −Rp 30.000) to `total_outflow`; hrefs carry period and offset; existing keys unchanged |

## 3. Calendar (tasks 3.1–3.2)

| Check | Result |
| --- | --- |
| Calendar tests | 11 in `test_analysis_calendar_flow.py`: whole window, waiting rows, movement once per pair, top-up apart, heat thresholds and levels, future days and upcoming plans, `met`/`skipped`, open cycle to the expected salary (day index 9), open cycle without an estimate (one nominal month), 422, 401, owner isolation, statement count |
| Σ `days.spend` equals `total_outflow` | Tested with a later reimbursement (offset Rp 30.000, no negative day). On the seeded owner the days equal analytics for cycle 0, cycle −1, month 0 and month −1 (2.721.500, 6.740.000, 626.000, 7.988.500) |
| Statement count | 8 queries plus the read-only isolation statement (≤ 9, see design "Implementation notes"), unchanged with 20 more rows and plans |
| Median of 20 calls (gate: under 150 ms) | cycle 4.4 ms, month −1 5.6 ms |

## 4. Flow (tasks 4.1–4.4)

| Check | Result |
| --- | --- |
| Flow tests | 9, over a fixture with income, all six buckets, an excluded category, a reimbursement, a transfer between accounts, a move into the account's own pocket, a goal pocket, a top-up, a sale back to cash, an awaiting leg, a question in a bucket, and a negative opening and closing |
| Conservation | Every holder node balances for `through` on every day of the window. On the last day each node's closing equals the ledger balance of its accounts (bank with its pocket, wallet, investment parent with its product, goal pocket) |
| Rows total equals the link | For every link of the fixture, plus 60 extra rows paged 50 at a time. Balance links return no rows; a node request lists each row once; unknown and foreign keys answer 404, malformed keys 422, `through` outside the window 422 |
| Summary at the window end | Belanja, Keluar and Masuk equal analytics; waiting out Rp 112.590; saving net Rp 1.000.000; investment net Rp 350.000 |
| Statement counts | flow ≤ 10 and rows ≤ 4, unchanged with 30 more accounts |
| Medians of 20 | flow: cycle 5.4 ms, month −1 8.0 ms (gate 200 ms). Rows: 5.2 ms (gate 100 ms). Analytics: 8.7 / 6.2 ms against 12.4 / 7.8 ms. Comparison: 8.5 / 6.4 ms against 8.4 / 6.1 ms (+1% and +5%, within 10%). The machine also ran another agent's stack |
| Full backend suite at `1e79564` | 1687 passed, 0 failed, 0 skipped (23 new); one passlib deprecation warning, as at baseline |

## 5. Frontend (tasks 5.1–9.3)

| Check | Result |
| --- | --- |
| `npm ci`, type-check, lint, `npm test`, build | ok, 0, 0 errors and 0 warnings, 70 files with 631 tests, ok |
| `/insights` first-load JS (gate: not above baseline) | 248.2 kB gzip (raw 834.3), down from 345.5 |
| Kalender and Peta Aliran chunks (gate: each under 40 kB gzip) | 4.1 kB and 9.9 kB |
| `/laporan` (gate: at most +5 kB gzip) | 239.1 kB (+2.0) |
| Shared first-load JS (gate: at most +1 kB gzip) | 226.1 kB (−0.6) |
| CSS | `/insights` and `/laporan` 22.2 kB gzip, against 18.6 (the Analisis module) |
| Copy guard | The phase 3 guard covers `components/analysis`, `components/report`, `components/insights`, both pages and `lib/reportLead.ts`. A second test refuses the formal "Anda" there. Both pass |
| Dependency removed | `recharts`, last used by `BurnCadenceChart` (`npm ls recharts`: empty) |

### Label renames

| Before | After |
| --- | --- |
| "Uang keluar" tile | "Belanja" (OD24); "Keluar" is now the Kakeibo total, as on Beranda |
| "Ringkasan & Anggaran" / "Bandingkan & Kakeibo" tabs | "Ringkasan" / "Kalender" / "Peta Aliran" |
| "Analisis Pengeluaran & Anggaran" heading | the page title "Analisis" and the "Kategori & anggaran" card |
| "Laju Pengeluaran Harian" | "Belanja harian" |
| "Peta Pola Belanja Mingguan" | removed (OD43) |
| title "Klik untuk mengatur batas anggaran" | "Atur batas anggaran" |
| "…sampai Anda memilih pilar." | "…sampai pilarnya dipilih." |
| "Catatan Anda untuk {periode}" | "Catatan untuk {periode}" |
| "Pilihan Anda juga dipakai untuk belajar kebiasaan Anda." | "Pilihan ini juga dipakai untuk mempelajari kebiasaan." |
| "berapa yang ditanyakan ke Anda" / "Dijawab Anda" | "berapa yang ditanyakan" / "Dijawab" |
| "Laporan lain" select | the report stepper and "Laporan sebelumnya" |

## 6. Browser (tasks 10.1–10.3)

### Matrix

Production build at `b453ce8`; results in `web-p4/matrix/results.json` and 89 screenshots, read by the agent through contact sheets and single views. The 1440 and 1280 cells also cover the side drawer, 1024 the rail and overlay drawer, and 768 and 390 the phone composition.

| Page and state | Widths × themes | axe serious or critical | Overflow | Console errors | Lowest contrast |
| --- | --- | --- | --- | --- | --- |
| Ringkasan | 5 × 2 | 0 | 0 | 0 | 5.04 |
| Kalender with a selected day | 5 × 2 | 0 | 0 | 0 | 5.00 |
| Peta Aliran map folded, table unfolded | 5 × 2 each | 0 | 0 | 0 | 5.04 |
| Peta Aliran table | 5 × 2 | 0 | 0 | 0 | 5.04 |
| Node and link drawers | 5 × 2 each | 0 | 0 | 0 | 5.04 |
| Laporan, complete and new user | 5 × 2, plus 1280 light for the new user | 0 | 0 | 0 | 4.91 |
| 200% and 400% zoom (640 and 320 CSS px) | 4 states × 2 | 0 | 0 | 0 | 5.11 |

- **Targets under 24 px:** none in any cell. The fix gave the insight-card links and "Baca Laporan Bulanan" 24 px targets.
- **Reduced motion:** "Putar ulang" is disabled, the note shows, and the slider still steps.
- **New user:** "Belum ada laporan", with no Rp 0 anywhere.
- **Fixed after reading the screenshots:**
  - the map's spending ribbons were hairlines next to the salary (fold now hides income, OD49);
  - labels of the saving and bucket columns overlapped (one shared column);
  - an empty first column wasted width;
  - day amounts were cut off ("Rp 412.00"), so cells show "412rb";
  - marks overflowed their cells;
  - the selected day had no outline;
  - transfers showed a minus sign in the day sheet;
  - the y-axis wrapped;
  - the comparison card's figures were cut off in the side column;
  - the phone fold button and the tabs at 400% did not wrap;
  - the phone week strip lost its heat colours;
  - opening a node drawer at 1280 swapped the map for the table, so focus could not return (the layout is now chosen by viewport).

### Real UI (`web-p4/interact/interact.json`)

| Check | Result |
| --- | --- |
| Keluar (Ringkasan) = Keluar (Beranda); Belanja (Ringkasan) = Belanja (Transaksi, cycle) | Rp 3.521.500 = Rp 3.521.500; Rp 2.721.500 = Rp 2.721.500 |
| Calendar days add up to Belanja | 31 cells sum to Rp 2.721.500 = the band = Ringkasan's Belanja |
| Every account node balances in the unfolded table | 7 holder nodes, 0 unbalanced (Bank Utama in = out = Rp 15.759.500) |
| A link drawer's rows add up to the link | "Dompet Digital → Kebutuhan": 14 rows = Rp 650.000. A row opens the transaction drawer; closing it brings the flow drawer back |
| Replay from day 1 to today | Day 1 "Minggu, 25 Oktober" up to day 10 "Selasa, 3 November", 10 days announced, node set stable |
| Waiting leg answered in Tinjau leaves "Menunggu pasangan" | The "Pindah saldo belum lengkap" item answered with an account; on the next load RDN Contoh → Menunggu pasangan fell from Rp 224.000 to Rp 112.000 |
| Keyboard walks | Tab list: Right to Kalender, End to Peta Aliran, Home to Ringkasan, with the URL updating. Grid: Right moves 3 → 4 Nov, Enter opens "Rabu, 4 November" with `day=2026-11-04`. Map: Right and Down move between nodes, Enter opens the node drawer, Esc returns focus to the node |
| Laporan stepper from a URL with a query | From `?basis=month&start=2026-10-01`: back gives September with the URL updated, forward gives October again |

### Regression (`regression-new/`, `regression-main/`)

`/`, `/review`, `/ledger`, `/accounts`, `/goals` and `/settings` at 1280 and 390, light and dark.

| Route | This branch | `main` `0ba4c41` |
| --- | --- | --- |
| `/`, `/review`, `/ledger`, `/goals`, `/settings` | axe serious 0, overflow 0, console errors 0 | same |
| `/accounts` | `color-contrast` 2 at 1280 (light and dark); 0 at 390 | same (the known phase 5 issue) |

## 7. OpenSpec and diff (task 11.1)

- `openspec validate --all --strict`: 75 passed, 0 failed, exit 0 (the main spec warning cleared with phase 3's archive).
- `git diff --check`: clean.
- CI "Checks" run 41591719715 on the pushed branch: success (backend on PostgreSQL 16, frontend type check, lint, tests and build, compose config and backup round trip).

## Not verified

- **A screen reader:** not run. Semantics were checked with axe, role queries and accessible names.
- **The phone's on-screen keyboard and real touch:** the browser emulated 390 px with touch only.
- **Calendar test gaps:** a month whose 31st is clamped by a short cycle and a movement fee row are not separate calendar or flow fixtures. Fee rows follow the generic expense rule, covered by the excluded and bucket cases.
- **Timings:** measured on a shared developer machine, not on the server.

## Acceptance

- 2026-11-03: the branch merged `main` (`5d97cd5`, phase 6) in `294cfbe`, resolving `copyRegister.test.ts` and `docs/NEW_USER_GUIDE.md` by keeping both phases' additions. The merged tree: type-check 0, lint 0 warnings, Vitest 716 passed, `openspec validate --all --strict` 76 passed. The owner said "accepted, defaults ok".
