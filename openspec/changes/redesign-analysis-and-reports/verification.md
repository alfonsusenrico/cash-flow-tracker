# Verification: redesign-analysis-and-reports

Not started. This plan is pending approval. Fill each section while implementing. Tie every result to the revision tested and record skips with their reasons.

Environment (to fill in):
- the disposable PostgreSQL 16 container and its port;
- the API port, with AI and background jobs off;
- the `next start` port and the proxy;
- the headless browser's CDP port.

No production host, phone or real model may be used. The data is synthetic only (owners and seed scripts to name here).

Revisions (to fill in):
- the baseline commit;
- the backend under test;
- the frontend under test;
- the `main` commit used for the regression build.

## 1. Baseline (task 1.1)

| Check | Result |
| --- | --- |
| Backend suite, Flyway V1–V?, `REQUIRE_TEST_DATABASE=1` | |
| `npm ci` / `npm test` | |
| `npm run type-check` / `npm run lint` / `npm run build` | |
| `/insights`, `/laporan` and shared first-load JS (raw and gzip) | |
| `GET /dashboard/analytics`, median of 20 on the 5 000-row owner | |
| `GET /dashboard/period-comparison`, median of 20 | |
| `/insights` and `/laporan` in the browser at 1280 and 390, light and dark | |

## 2. Shared helpers and additive fields (tasks 2.1–2.3)

| Check | Result |
| --- | --- |
| Beranda response fixture equal before and after the move | |
| `period-comparison.kakeibo` tests | |
| `analytics.explanations` tests | |

## 3. Calendar (tasks 3.1–3.2)

| Check | Result |
| --- | --- |
| Calendar tests (counts) | |
| Σ days.spend equals `total_outflow` (cycle, month, with reimbursements) | |
| Statement count (31-day and 28-day windows) | |
| Median of 20 calls (gate: under 150 ms) | |

## 4. Flow (tasks 4.1–4.4)

| Check | Result |
| --- | --- |
| Flow tests (counts) | |
| Conservation for every account node on every fixture day | |
| Rows total equal to the link for every link kind | |
| Statement counts (flow ≤ 10, rows ≤ 4) | |
| Medians: flow (under 200 ms), rows (under 100 ms), analytics and comparison (within 10%) | |
| Full backend suite | |

## 5. Frontend (tasks 5.1–9.3)

| Check | Result |
| --- | --- |
| `npm ci`, type-check, lint (0 warnings), `npm test`, build | |
| `/insights` first-load JS (gate: not above baseline) | |
| Kalender and Peta Aliran chunks (gate: each under 40 kB gzip) | |
| `/laporan` (gate: +5 kB gzip at most) and shared (gate: +1 kB gzip at most) | |
| Copy guard over the new files | |

### Label renames

| Before | After |
| --- | --- |
| | |

## 6. Browser (tasks 10.1–10.3)

### Matrix

| Page and state | Widths × themes | axe serious or critical | Overflow | Console errors | Lowest contrast |
| --- | --- | --- | --- | --- | --- |
| Ringkasan | | | | | |
| Kalender with a selected day | | | | | |
| Peta Aliran map, folded and unfolded | | | | | |
| Peta Aliran table | | | | | |
| Node and link drawers | | | | | |
| Laporan, complete and new user | | | | | |
| 200% and 400% zoom, reduced motion | | | | | |

### Real UI

| Check | Result |
| --- | --- |
| Keluar (Ringkasan) = Keluar (Beranda); Belanja (Ringkasan) = Belanja (Transaksi) | |
| Calendar days add up to Belanja | |
| Every account node balances in the table | |
| A link drawer's rows add up to the link | |
| Replay from day 1 to today | |
| Waiting leg answered in Tinjau leaves "Menunggu pasangan" | |
| Keyboard walks: tab list, calendar grid, map nodes | |
| Laporan stepper and URL from a URL with a query | |

### Regression

| Route | This branch | `main` |
| --- | --- | --- |
| | | |

## 7. OpenSpec and diff (task 11.1)

## Not verified
