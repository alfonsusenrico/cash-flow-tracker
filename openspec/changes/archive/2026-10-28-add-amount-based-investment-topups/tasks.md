# Tasks

## 1. Persistence and amount valuation

Expected Result: existing data stays intact; amount-tracked products have explicit capital and valuation state independent of their opening ledger balance.

- [x] 1.1 After owner approval, create a topic branch from current main and add the next versioned migration for amount-state fields, contribution journal, owner-scoped retry uniqueness, and recurring top-up constraints; verify the complete migration chain and upgrade of seeded legacy accounts/rules on disposable PostgreSQL.
- [x] 1.2 Implement locked lazy initialization, canonical amount-mode account responses, and Update Nilai semantics; verify known/unknown capital, explicit zero value, unchanged opening balance, Rp50.000 gain preservation, parent aggregation, and liquid/investment/net-worth summaries with database tests.

## 2. Contribution lifecycle

Expected Result: a contribution creates one debit and one credit, supports safe correction/reversal, and cannot be duplicated or bypassed through older mutation routes.

- [x] 2.1 Implement the shared contribution service and authenticated create/read API with effective liquid-source resolution, leaf/ownership/units validation, locked balance checks, and atomic paired writes; verify Rp112.590 success and invalid, foreign, archived, aggregate, unit-tracked, and insufficient-funds cases on PostgreSQL.
- [x] 2.2 Implement owner-scoped create replay, immutable request fingerprints, and complete or hashed retry keys; verify concurrent creates, lost-response replay, changed-input conflicts, and keys sharing a long prefix without a second financial mutation.
- [x] 2.3 Implement amount/time/notes correction and deletion tombstones with signed capital/value deltas; verify increased-debit checks, lower-amount refunds, post-valuation reversal, negative-result rejection, concurrent correction/valuation, and replay after deletion.
- [x] 2.4 Expose contribution metadata in transaction responses and guard generic movement/transaction, account edit/delete, split/merge, and trade paths; verify bypass attempts fail, archived-target reversals work, existing unit trades still pass, and investment allocations count once without ordinary spending/income pollution.

## 3. Monthly confirmation

Expected Result: each product's monthly rule waits for the actual debit, records one selected occurrence on confirmation, and remains pending on failure.

- [x] 3.1 Extend recurring create/update validation and responses with `investment_topup`, using the shared account eligibility checks and manual-only semantics; verify monthly create/edit/deactivate, forbidden auto-post/category/obligation/payroll inputs, and unchanged ordinary recurring behavior.
- [x] 3.2 Add scheduled-date selection to investment confirmations while retaining actual execution date; integrate the contribution service with existing occurrence claims/savepoints and explicitly exclude top-ups from automatic processing; verify delayed/concurrent/retried confirmation, stale/future selection rejection, monthly advancement, batch partial failure, unchanged pending dates on failure, and no re-execution after deleting a confirmed contribution.

## 4. Web entry, ledger, and reminders

Expected Result: amount-tracked products expose a usable Top up action; the ledger and monthly reminder display the contribution and its outcome correctly on desktop and mobile.

- [x] 4.1 Add the product Top up modal and funding-default precedence, with rupiah amount, actual date/time, notes, stable retry identifier, estimate guidance, and a separate monthly-rule shortcut; verify eligible standalone/child products, no units/NAB inputs, failed-entry retention, and safe unit-tracked behavior in focused frontend tests.
- [x] 4.2 Update ledger types, consolidation, labels, and edit/delete dispatch for contributions; verify one Top up Investasi row, dedicated lifecycle requests, error feedback, and refreshed financial caches in desktop/mobile ledger tests.
- [x] 4.3 Extend recurring types/form validation and the existing rules modal with eligible investment targets and fixed manual confirmation; verify the monthly shortcut creates a rule without recording another debit and existing expense/income/transfer forms still behave correctly.
- [x] 4.4 Update pending confirmation to send the selected scheduled dates and inspect per-item outcomes; verify source → product display, exactly-once retry selection, retained failed items, and relevant cache refreshes with component tests.

## 5. Contract verification and handoff

Expected Result: the approved workflow is verified locally, and the owner receives a reviewable topic branch with honest acceptance and release evidence.

- [x] 5.1 Run the full backend suite against disposable migrated PostgreSQL and frontend tests, type-check, lint, and production build; record commands/results and verify prior investment, movement, recurring, and dashboard contracts remain green.
- [x] 5.2 Smoke-test a fictional RDN → Bibit product top-up, valuation update, correction, deletion, monthly rule, and confirmed occurrence in the local browser at desktop/mobile sizes; verify keyboard focus, labels/errors, contrast and zoom/reflow against the UI guide and document observable evidence without touching real financial records.
- [x] 5.3 Run `openspec validate add-amount-based-investment-topups --strict` and `git diff --check`, refresh PROJECT_STATE, and commit/push the approved implementation topic branch; report the branch and checks for owner acceptance, leaving merge, deployment, and archive to their separately authorized workflow.

## 6. Owner-requested copy refinement

- [x] 6.1 Remove persistent top-up eligibility text from standalone and child product rows, preserving existing action eligibility; verify account component tests, type-check, and a fictional unit-tracked product browser check.
