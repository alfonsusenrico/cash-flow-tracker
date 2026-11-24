# Verification: improve-daily-spending-detail

Tested on 2026-11-24, branch `feat/analysis-daily-detail` at `05cf3dd1` (base `origin/main`) plus the uncommitted implementation. The backend (task 1.1) and the first web build (tasks 2.1–2.3) were done by delegated agents and reviewed here; the browser fixes below were made here.

## Checks

| Check | Result |
| --- | --- |
| Backend, full suite (Postgres 16, Flyway V1→V59) | 2467 passed (backend unchanged since that run) |
| Frontend type check, lint, tests (vitest), build | 0 errors, lint passed, 1184 passed (102 files), build passed |
| `git diff --check` | clean |
| `openspec validate --all --strict` | 87 passed, 0 failed |
| `frontend/package-lock.json` | unchanged |

## Scenario evidence

| Scenario | Evidence |
| --- | --- |
| A bar opens its day | `analysis.test.tsx` (click, Enter, Space, a Rp 0 day, touch tip with "Lihat di Kalender"); `insights/page.test.tsx`; browser: the 11 Nov bar opened `?tab=kalender&day=2026-11-11` at all six widths, showing that day's Rp 78.500 and its rows |
| Hovering, tapping, labels at every width | existing chart tests unchanged and passing |
| Pillars add up | `tests/test_spending_basis.py::test_a_reimbursed_want_lowers_its_day_and_pillar` (need 412.000, want 627.649), `test_daily_pillars_and_categories_add_up_to_pengeluaran` (every day of three periods) |
| Uncategorised and saving rows | `test_uncategorised_and_saving_rows_in_the_daily_detail` |
| Per category columns | `analysis.test.tsx` (top five, "Lainnya", tie order, five or fewer); browser at 1280 |
| Narrow screen | browser at 390: the table scrolls inside its box with the date fixed; no page-level horizontal overflow |
| Comparison label fits | browser at 1280 and 1024: "seluruh periode" ends before its bar; the whole-period rows share one grid |

## Browser check

A production build with the prototype's synthetic dataset at V59. Ringkasan, the chart, both table modes and a bar click, at 1440, 1280, 1024 and 390 px, light and dark (30 captures): 0 serious or critical axe violations, 0 console errors, no horizontal overflow.

**Found and fixed during the check:**
- the table's scroll box was not keyboard-reachable: it is now a focusable region named by the table;
- at 390 px the page widened to 693 px: screen-reader-only cell text (absolutely positioned) escaped the scroll box. `.tbl-scroll` is now its own containing block (this also covers Laporan's two tables);
- long category headers were cut by a two-line clamp ("MAKANAN &"): headers now wrap in full;
- dates took two lines: with the breakdown the date is short ("Rab, 11 Nov") and never breaks, only its suffix wraps;
- on desktop Total scrolled out of view at 1280 px: Total is pinned on the right from 768 px with a 1 px rule, so a partly hidden column under it shows there is more; on the phone it scrolls with the rest;
- the date column took the leftover width: it now hugs its content.

axe's `target-size` appeared only on captures scrolled so that an element sat under the sticky top bar, and moved between elements with the scroll offset; at the other offsets it did not appear. It is a capture artefact, not a target smaller than 24 px.

Captures: `research/evidence/improve-daily-spending-detail/`.

## Not verified

- The owner's own data (after the deploy).
- Real touch input; the touch flow is covered by tests with pointer type `touch`.

## Known limits

- A reimbursement is spread over all days of its category and pillar, as `spending` already nets it, not only the day of the linked expense.
- Keyboard focus on a bar shows its name and focus ring, not the tip.
