# Verification

All data is synthetic. No production host, production database, real financial record or phone was used.

## Revision

Branch `feat/essential-cost-and-insights`, stacked on `feat/financial-reporting-consistency` at `4220bba`. The checks below ran on the working tree that the change's commits contain (2026-10-30).

## Checks

| Check | Result |
| --- | --- |
| Flyway (`flyway/flyway:10`) on a newly created `postgres:16` | V1→V38 applied, 38 migrations |
| Backend `pytest backend/tests` on that database | 1144 passed, 0 failed, 0 skipped (base change recorded 1111) |
| Backend without PostgreSQL (database cases skip, as in CI) | 627 passed, 517 skipped, 0 failed |
| New targeted backend tests (`test_essential_cost.py`, `test_plan_auto_link.py`, `test_observed_insights.py`) | 31 passed |
| Frontend `npm ci` | completed |
| Frontend `npm test` | 48 files, 247 tests passed |
| Frontend `npm run lint` | 0 errors, 1 existing warning (`src/app/review/page.tsx` `_done`, unchanged) |
| Frontend `npm run type-check` | passed |
| Frontend `npm run build` | compiled successfully |
| `git diff --cached --check` (all changes staged) | clean |
| `openspec validate --all --strict` | 63 passed, 0 failed |
| MODIFIED deltas keep every source scenario name (script against base-change or main version) | no scenario dropped |

## Evidence per decision

- **D1 essential cost** (`test_essential_cost.py`): a Rp 30.000.000 `unexpected` month leaves the cost at Rp 2.000.000; a need one-off moves only its own period and the median stays Rp 2.000.000; one period gives `limited_history: true`; a month that began before tracking is not used; `need` budgets are the fallback (`want` and excluded budgets ignored); nothing known gives `null`; a mixed-pillar reimbursement reduces need by its Rp 50.000 share; debt-linked expenses (legacy `obligation_id` and allocation rows) are excluded and debt minimums are added once; movement legs, investment accounts and excluded categories are left out; closed confirmed salary cycles come first, bounded at the receipt instants, then calendar months before the first receipt; median rounding half up.
- **D2 coverage**: unknown cost gives `coverage_months`, `runway_months`, `runway_days` and `monthly_primary_expense` all `null`, status `unknown` (no 999); a known zero cost is also `null` with `essential_cost_zero`; no goal gives `balance_basis: "all_liquid"` from liquid cash only (investment excluded) on net-worth and overview; a flagged goal gives `emergency_goals`, 2.0 months, 61 days, `critical`, with the owner's multiplier. Web: `EmergencyCoverage.test.tsx` (basis sentence, limited history, all-liquid notice, exact drill-down link, axe) and an accounts page test.
- **D3 auto-linking** (`test_plan_auto_link.py`): one certain payment is linked once with `link_method: auto`, debt reduced once, audit event `link/auto`; four concurrent passes link once; two candidates, another account, another amount, 4 days before or 8 days after due never link (3 days before does); income plans link, transfer plans never; a payment two due plans could claim is not linked; undo restores and reactivates the debt once, a repeat changes nothing, the occurrence is due again, the pair is never re-linked automatically and the owner may still link it; undo does not reverse debt the link did not apply; resolving an undone occurrence skips a later resolved one; recording a transaction requests a pass and the request links; the kill switch ignores requests; V38 marks existing links `owner` and re-runs cleanly. Loop tests in `test_notification_lifecycle.py`. Web: `PendingScheduledBanner.test.tsx` (label, undo request, refusal, axe).
- **D4 insights** (`test_observed_insights.py`): the comparison measures March over the same 9 days 18 hours as April (Rp 100.000, not Rp 1.000.000); pillar and category changes; no previous data gives `previous_has_data: false` with null changes; a shorter previous cycle shortens both windows; the endpoint returns its windows; history lists 6 completed months plus the current one in progress; detection finds monthly and weekly charges and rejects irregular amounts or intervals, two occurrences and ended charges; a price change is shown; "Jadikan rencana" posts a non-posting plan, adds no transaction, and the charge is then marked with that plan. Web: `insights/page.test.tsx` (exact-window links for category, merchant and pillar, no percentages, no-data note, history, plan prefill dialog, axe), `RecurringRulesModal.test.tsx`, `ledgerFilters.test.ts`.

## Browser checks (headless Brave over CDP; API and `next dev` on loopback)

One synthetic owner seeded through the API (scripts kept outside the repository). Viewports 390 and 1280 px, light and dark (`dark` class confirmed).

- Dompet: "Dasar Ketahanan Dana" with "14,8x biaya pokok · 448 hari", "Belum ada dana darurat khusus — dihitung dari seluruh saldo kas", "Median 4 periode, kebutuhan (need) saja + cicilan minimum", and four periods whose links open the ledger with `from`, `to`, `kakeibo=need`, `type=expense`.
- Ledger opened from a period link: chips "Uang keluar | Pilar: Kebutuhan | Rentang: 1 Sep – 30 Sep", period summary scope, the two need rows of that month; `q=Musik Contoh` seeds the search box.
- Beranda: "Ditautkan otomatis" lists the debt plan linked by the request loop right after its payment was recorded; the electricity plan with two identical payments stays due as ambiguous.
- Analisis: fair comparison (30 days each), cash flow per period with "berjalan", three detected charges with "Jadikan rencana".
- Keyboard: Tab moves charge link → "Jadikan rencana" → next charge with `:focus-visible` outlines; Enter opens the plan form prefilled (name, Rp 420.000), Escape closes it; Enter on "Batalkan tautan" undoes the link and the plan returns to the due list.
- 24 screenshots, no horizontal page overflow at any width, console errors: 0.
- The first seeding attempt recorded the two identical electricity payments after the plan was due, so the first was linked before the second existed. This is recorded as a risk in design.md; the seed was reordered.

## Not verified

- Production deployment, production data and devices: out of scope.
- Android: not changed and not run; the companion reads only `/dashboard/cash-flow-summary`, which this change does not alter (checked read-only in the companion worktree).
- The notification and text-capture trigger points are covered by code review and the shared request path (`test_recording_requests_a_link_pass_and_the_request_links` uses the transaction route); no notification-specific test drives a payload through to an automatic link.
- Push, merge request and archive: not authorised in this run. Archive only after `audit-financial-calculations-and-automation`.

## Rollout and rollback

See design.md, Migration Plan. Deploy the backend through the approved pipeline (Flyway applies V38), confirm no linked occurrence lacks `link_method`, and watch the first automatic links on Beranda. `PLAN_AUTO_LINK_ENABLED=false` stops automatic linking without a rollback; redeploying the previous release ignores the additive V38 schema.
