# Verification: attribute-linked-reimbursements-to-their-day

Tested on 2026-11-24, branch `fix/reimbursement-day` at `fd0fa1cf` (base `aea6ae8d`) plus the uncommitted implementation. Server-side only; no frontend change.

## Checks

| Check | Result |
| --- | --- |
| Backend, full suite (Postgres 16, Flyway V1→V59) | 2468 passed, 1 failed: `test_apportion_is_exact_and_never_exceeds_a_weight` imported `apportion` through `report_figures`, which no longer needs it; the test now imports it from `services/apportionment`, and its file passes |
| Affected files after that fix (`test_report_figures.py`, `test_spending_basis.py`, `test_reimbursements.py`) | 37 passed |
| Regression proof | `test_a_repaid_meal_stays_on_its_day` fails on the base code (the meal's day read 109.612, its share of a spread) and passes with the change; `test_an_unlinked_reimbursement_still_spreads` passes on both |
| Frontend suites | not run: no frontend file changed; CI runs them on the PR |
| `git diff --check` | clean |
| `openspec validate --all --strict` | 87 passed, 0 failed |

## Scenario evidence

| Scenario | Evidence |
| --- | --- |
| A repaid meal stays on its day | `tests/test_spending_basis.py::test_a_repaid_meal_stays_on_its_day`: the meal's day is 205.350 − 2 × 68.450, the later day keeps its full 88.290 (pillar and category too), the month's days add up to the comparison's unchanged `totals.spending`, and Kalender reads the same days |
| An unlinked reimbursement still spreads | `test_an_unlinked_reimbursement_still_spreads`: Rp 30.000 and Rp 120.000 |
| Single spending day | existing reimbursement tests, unchanged |
| Totals unchanged | the existing reimbursement, analytics, comparison and calendar tests pass with their figures |

## Smoke check

The backend on the prototype's synthetic dataset at V59, for the current cycle, the previous cycle and the previous month: the analytics days add up to Kalender's total (Rp 3.502.990, Rp 5.452.830 and Rp 3.884.330, the same figures as before the change), Kalender's days equal the analytics days, and every day's pillars and categories add up to its Pengeluaran with no negative day. The web pages read these fields unchanged, so no browser capture was taken.

## Not verified

- The owner's own data; after the deploy, 23 November should read Rp 109.290 (Rp 109.920 once the 53.290 typo is corrected) and 31 October the Mie Tore meal net of Rp 136.900.
