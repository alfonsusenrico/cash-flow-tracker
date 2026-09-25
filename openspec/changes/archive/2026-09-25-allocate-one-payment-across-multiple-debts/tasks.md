# Tasks

## 1. Persistence and Contract

- [x] 1.1 Add the versioned allocation table and idempotent startup schema support, including positive amount, unique transaction/debt pair, and referential constraints; verify fresh and upgrade migrations run and repeat safely without modifying legacy payment rows.
- [x] 1.2 Extend transaction create/update request validation for exclusive legacy ID or allocation list, positive distinct amounts, and exact expense total; verify invalid type, duplicate, mixed-field, and sum-mismatch requests create no records.
- [x] 1.3 Load normalized allocation breakdowns in bounded transaction list/detail responses and include allocation matches in debt filtering; verify page counts, cash totals, and legacy single-debt responses remain unchanged.

## 2. Atomic Debt Settlement

- [x] 2.1 Settle a new multi-debt expense with one account debit and ordered locked debt rows, enforcing ownership, active status, and remaining capacity; verify two-debt, full-payoff, concurrent-overpay, and late-failure rollback integration tests.
- [x] 2.2 Reverse and replace allocations on transaction edit, including edits to amount, account, or debt targets and transitions to/from legacy single-debt links; verify archived debt reactivation, stale/invalid allocation rejection, and original-state preservation after failed edits.
- [x] 2.3 Reverse every allocation on deletion and reject hard deletion of a debt with allocation history; verify delete/reversal, ordinary archive, and existing recurring single-debt payment tests.
- [x] 2.4 Preserve idempotent creation and receipt behavior for allocated payments; verify a repeated key causes one debit and one set of debt reductions, and receipt failure never repeats the payment.

## 3. Form and Ledger Experience

- [x] 3.1 Add a shared debt allocation editor to Quick Capture with amount-per-debt controls, live allocated/unallocated totals, eligible debt choices, field errors, and keyboard/mobile support; verify valid payload, incomplete split, duplicate/over-capacity feedback, and one-payment ledger outcome.
- [x] 3.2 Show saved allocation names and amounts in ledger rows/details and load them into the ordinary edit form, including paid/archived debts; verify edited splits, amount changes, one-row rendering, deletion confirmation, and query refresh in desktop and mobile tests.
- [x] 3.3 Review affected forms at mobile and desktop widths with keyboard, zoom/reflow, and assistive-technology spot checks; record observable results without claiming WCAG conformance.
- [x] 3.4 Default newly selected debt payments to outstanding balances, derive the transaction total, and place partial-payment inputs behind an explicit action in capture and ledger edit; verify full-payoff, partial-payment, saved-edit, and invalid-amount behavior with focused tests and rendered mobile/desktop checks.

## 4. Verification and Handoff

- [x] 4.1 Run focused and full applicable backend/frontend tests, type-check, lint, production build, migration checks, strict OpenSpec validation, and `git diff --check`; record exact current-revision results and limitations in `PROJECT_STATE.md`.
- [x] 4.2 Review the final diff against both modified capability specs for data integrity, legacy compatibility, duplicate cash effects, error recovery, security, and unrelated changes; verify every scenario has implementation or runtime evidence before requesting user acceptance.
- [x] 4.3 Re-run affected frontend checks, production build, strict OpenSpec validation, and final diff review after the default-payment revision; record current local-runtime evidence and remaining manual limits before requesting acceptance.
