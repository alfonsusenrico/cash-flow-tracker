# Verification: improve-ledger-readability

Tested on 2026-11-16, branch `fix/ledger-readability` at `ca39d4d2` (base `2fdc25a8`) plus the uncommitted implementation. The backend (tasks 1.1–1.3) and the web (tasks 2.1–2.4) were built by delegated agents and reviewed here. After the owner's correction, the switch to payday-to-payday labels, the removal of the "Siklus" prefix, the shared category/pillar cell, and the test and fixture updates for them were done here.

## Decisions made during the build

- **Payday to payday (owner, 2026-11-16).** The plan first said a finished cycle ends the day before its next salary. The backend review showed it ends on the salary day, because the boundary is the salary's moment, so payday-morning spending belongs to the old cycle. The owner chose payday to payday for both finished and running cycles.
- **No "Siklus" prefix.** Cycle ranges read "30 Okt – 29 Nov", as the owner asked. English keeps the region's day-month order ("30 Oct – 29 Nov").
- **Category and pillar in one cell (owner, 2026-11-16).** The first browser check showed a 200 px name column at 1280 px. Stacking the pillar under the category widened it to 336 px.

## Checks

| Check | Result |
| --- | --- |
| Backend, full suite (Postgres 16, Flyway V1→V58) | 2425 passed |
| Frontend type check, lint, tests (vitest), build | 0 errors, lint passed, 1170 passed (102 files), build passed |
| `git diff --check` | clean |
| `openspec validate --all --strict` | 87 passed, 0 failed |
| `frontend/package-lock.json` | unchanged |

## Scenario evidence

| Scenario | Evidence |
| --- | --- |
| A long name keeps its line; extras line up; a row without extras | `LedgerList.test.tsx` (title line, order meta · note · tags, one-line row, other lists unchanged); browser: name column 432 px at 1440, 336 px at 1280, 312 px at 1024 |
| Category and pillar share a cell | `LedgerList.test.tsx` "puts the category above the pillar…"; captures `rl2-transaksi-*` |
| Narrow desktop | captures at 1024 px: no horizontal scroll |
| Every row has a dot | `rows.test.tsx` (seven kinds, dot hidden from assistive technology); captures |
| Top-ups under Tabungan & investasi; `saving_net` unchanged | `test_ledger_moved_and_saving_type.py`, `test_ledger_saving_flow.py`; browser: the tab lists the top-ups with "Investasi" |
| An old Investasi link | `ledger/page.test.tsx`, `ledgerFilters.test.ts`; browser: `/ledger?type=investment` became `?type=saving` |
| Hero for Masuk and Pindah saldo; older API | `ledger/page.test.tsx`; `test_ledger_moved_and_saving_type.py` (`moved` equals the day totals' sum over pages); browser: Pindah saldo hero Rp 9.400.000 |
| Salary cycle labels (setting, late, early, regular receipts) | `test_running_cycle_labels.py`; `PeriodStepper.test.tsx`, `insights`/`laporan`/ledger page tests; browser: Transaksi and Analisis read "3 Nov – 3 Des", the finished cycle "6 Okt – 3 Nov" |
| Cycle label is not a window | `test_running_cycle_labels.py`, `test_expected_salary_date.py`, `test_salary_cycle_consumers.py`: the running window still ends at now |

## Browser check

**Setup:** a production build (`next start`), the backend and a loopback proxy, with the prototype's synthetic dataset migrated to V58. Three rows were given long names, notes and up to three tags through the API.

**Coverage:** Transaksi (cycle, Masuk, Pindah saldo, Tabungan & investasi, the old Investasi link), Analisis and Beranda, at 1440, 1280, 1024 and 390 px, light and dark (42 captures), then Transaksi again after the shared cell (15 captures).

**Automated findings:** 0 serious or critical axe violations, 0 console errors and no horizontal overflow in all 57 captures. The style checker's "coloured borders" (Catat's type switch, Beranda) and one non-tabular "105%" on Analisis are existing elements outside this change.

Selected captures are in `research/evidence/improve-ledger-readability/`.

## Not verified

- **The owner's own data.** Only the synthetic dataset was used. Production shows the new labels after the deploy.
- **Laporan in the browser.** Its label is covered by `laporan/page.test.tsx` only.

## Known limits

- Analisis's hero reads "3 Nov – 3 Des sejauh ini": the figures run to now while the label names the expected end.
- The calendar tab's grid still ends one nominal month after the start when no regular-receipts estimate exists; its range can differ from the label by a day or two.
