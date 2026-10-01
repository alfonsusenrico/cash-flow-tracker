# Verification — 2026-10-01

Owner approved the amount-only contribution plan and selected confirmation of each actual monthly debit. Implementation is on `feat/amount-based-investment-topups`, based on main `2361145`.

## Automated evidence

- Disposable PostgreSQL 16: container `cft-topups-test-db`, loopback port 5546, fictional database `ledger_topups_test`. Applied V1–V23 in numeric order. A regression test separately upgrades seeded V1–V22 legacy accounts/rules, applies V23 twice, and verifies unchanged legacy data.
- `TEST_DATABASE_URL=postgresql://topups_test@127.0.0.1:5546/ledger_topups_test SKIP_TEST_MIGRATIONS=1 PYTHONPATH=backend backend/.venv/bin/pytest backend/tests -q --tb=short`: **595 passed**, no skipped database tests. Local Python 3.14; the existing hosted workflow uses Python 3.12.
- Offline backend preflight (unreachable disposable database URL): **431 passed, 164 database-dependent tests skipped**; it finishes without hanging. This supplements the fully migrated PostgreSQL run above.
- `docker compose -f docker-compose.yml config --quiet`: passed.
- Frontend `npm test -- --reporter=dot`: **126 passed in 31 files**, no unhandled errors.
- Frontend `npm run type-check`, `npm run lint`, and `npm run build`: passed. Build reports the existing multiple-lockfile workspace-root warning; no dependency or lockfile changes were made.
- `openspec validate add-amount-based-investment-topups --strict`: passed.
- `git diff --check`: passed.

Financial regressions cover gains, unknown capital, explicit zero value, ownership, eligibility, insufficient funds, atomic paired writes, concurrent requests, immutable retry fingerprints, correction after valuation, deletion tombstones, generic mutation guards, parent aggregation, net worth, saving allocation counted once, and manual recurring occurrence retries/partial failures. Existing unit trades and ordinary recurring/movement/dashboard suites remain green.

Three existing test assumptions were corrected without changing unrelated application behavior: a valuation connection context-manager mock, a scheduler assertion that assumed no other shared-fixture rule could be due at a month boundary, and Quick Capture tests ending before their success-close timers completed.

## Local browser evidence

Chrome used fictional accounts through isolated backend port 8056 and frontend port 3056, connected only to the disposable database. Temporary Playwright scripts, fixture and screenshots live under `/private/tmp/cft-topups-browser/`; no real credentials or financial records were inspected.

- Rp112.590 from a fictional BCA RDN to a Bibit mutual-fund leaf defaults funding correctly and requires no units/NAB. Capital becomes Rp1.112.590 and estimated value Rp1.162.590, preserving Rp50.000 gain.
- Insufficient-funds feedback retains entered values. Tab focus stays in the dialog; Escape closes and restores the initiating button's focus.
- Update Nilai sets an absolute market value and clears the estimate. Desktop ledger correction and mobile deletion use the contribution endpoints and adjust capital/value by the cash delta.
- Jadwalkan bulanan creates a manual rule without a debit. Confirming its selected due occurrence records one contribution and advances the schedule.
- Checked 1440, 390 and 320 CSS-pixel widths; narrow modal controls remain inside their scrollable container. Checked 200% root text size plus WCAG text-spacing overrides; every form control remains reachable. A 320 px viewport provides the narrow layout equivalent for 400% reflow. Existing page-wide layout outside the affected modal was not certified.
- Rendered axe checks scoped to the top-up dialog pass in light and dark themes, including labels, contrast and target size. Scans wait for the existing theme transition to settle. This is scoped automated/browser evidence, not whole-product accessibility certification or assistive-technology acceptance.

## Handoff and release limits

Only active mutual-fund leaves with `units IS NULL` qualify; explicit zero units remains unit-tracked. Contributions adjust estimated value rather than retrieving allocated units or current NAV. The owner avoids duplicate recording when a debit is already in the ledger.

The repository's hosted workflow runs only on main pushes or explicit dispatch and includes production deployment. A topic-branch push does not run that workflow; it must not be dispatched just to obtain CI evidence. Owner review/merge, hosted checks, deployment, final acceptance, spec synchronization and archive remain separate next steps. No Android or production changes were performed.
