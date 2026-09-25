# Tasks

## 1. Form and Test Foundations

- [x] 1.1 Create a topic branch from the intended target after the integrity contracts required by this change are available, update `PROJECT_STATE.md`, and verify the branch, clean starting diff, generated API types, and baseline frontend checks.
- [x] 1.2 Add pinned Vitest, Testing Library, user-event, DOM matcher, and axe test dependencies plus focused scripts/configuration; verify a representative accessible form test runs in CI-compatible mode and the lockfile install is reproducible.
- [x] 1.3 Define canonical query-key factories and migrate affected mutations away from divergent ledger/transaction/recurring keys; verify query-key tests and `rg` show no active stale aliases.
- [x] 1.4 Build reusable semantic field, error-summary, pending-submit, and accessible choice primitives on top of existing styles; verify component tests cover label association, descriptions, errors, live regions, selected states, and disabled states.
- [x] 1.5 Upgrade the shared Modal for unique naming, initial focus, focus containment, Escape, restoration, scroll containment, and reduced motion; verify keyboard and axe tests cover desktop dialog and mobile bottom-sheet modes.

## 2. Canonical Transaction and Movement Forms

- [x] 2.1 Convert Quick Capture to a semantic schema-driven form with type-filtered categories, metadata-derived Kakeibo defaults, explicit overrides, dates, notes, goal/obligation compatibility, and stable accessible fields; verify payload, keyboard-submit, error-focus, and mobile tests pass.
- [x] 2.2 Add recoverable receipt attachment to transaction create and edit flows, preserving the saved transaction when upload fails; verify valid upload, invalid-file feedback, replacement, and retry tests pass.
- [x] 2.3 Make `InternalMovementModal` the only movement form, add canonical edit support, exclude investment positions from generic account choices with a Beli/Jual explanation, and route Home, Accounts, global navigation, and ledger entry points through it; verify all entry-point tests submit the same payload and no duplicate active movement form remains.
- [x] 2.4 Complete ordinary ledger editing for compatible type, account, category, Kakeibo, date, notes, goal/obligation, and receipt fields while delegating linked movements to the movement API; verify mutation and refresh tests cover both paths.
- [x] 2.5 Preserve and expose seconds in Quick Capture, movement create/edit, and ordinary ledger edit date-time controls without changing compact ledger display; verify nonzero seconds survive opening and saving in unit/component tests and a local browser check.

## 3. Recurring, Payroll, and Category Forms

- [x] 3.1 Replace recurring form render-time state mutation with a schema-driven create/edit flow exposing weekday/monthly/payday controls, compatible categories, liquid-only generic transfer choices, explicit clearing, auto-post, active state, and payroll inclusion defaulted off; verify create/edit/type-switch and investment-position exclusion tests pass.
- [x] 3.2 Align payroll configuration and execution forms with explicit inclusion, liquid-only generic transfer choices, available-balance summary, per-item selection/editing, atomic error feedback, and accessible controls; verify affordable, rejected, and investment-position exclusion UI tests pass.
- [x] 3.3 Complete category create/edit with kind, Kakeibo pillar, primary status, budget, icon, color, and archive behavior; verify income/expense compatibility plus named/selected icon and color tests pass.

## 4. Goal, Debt, Trade, Account, and Profile Forms

- [x] 4.1 Replace synthetic goal deposit UI with standalone progress adjustment and account-backed explanatory state, including backing-mode confirmation; verify linked goals show no deposit action and standalone adjustment submits no account or transaction fields.
- [x] 4.2 Add coherent obligation final-state validation and accessible auto-archive/reactivation explanation; verify excessive remaining amount, valid combined update, and keyboard error correction tests pass.
- [x] 4.3 Complete investment trade settlement presentation with funding direction, owned-unit limits, projected cash/position effects, and server financial-error mapping; verify buy, sell, exact liquidation, oversell, and insufficient-funds form tests pass.
- [x] 4.4 Reconcile account and pocket forms around shared monetary/account selectors, correct default-pocket behavior, principal-only deposit entry, and type-compatible instrument fields; verify create/edit/reconcile flows preserve valid values and expose field errors.
- [x] 4.5 Align registration and settings with persisted display name, password requirements, IDR/USD restriction, numeric ranges, and canonical settings refresh; verify registration persistence and unsupported-currency tests pass.

## 5. Cleanup and Responsive Accessibility Review

- [x] 5.1 Remove confirmed-unused `TransactionModal`, `ExportModal`, obsolete domain types, and replaced duplicate form state after all callers migrate; verify `rg`, type-check, and build find no remaining references or missing exports.
- [x] 5.2 Replace active icon-only/title-only actions, browser confirmations, unassociated labels, broad transition classes, and ASCII ellipses in affected forms with accessible project-standard behavior; verify focused source searches and axe tests report no known affected-form violations.
- [x] 5.4 Audit visible labels, section captions, and helper paragraphs in every active financial input form; remove redundant copy, keep essential warnings/errors visible, and move necessary optional explanations into one keyboard/touch/hover-operable information control. Verify affected form tests and focused source review preserve labels, units, validation, and submission behavior.
- [ ] 5.3 Review every affected form at desktop and mobile breakpoints for content parity, touch targets, 200% zoom/reflow, virtual-keyboard behavior, scroll containment, and reduced motion; record observable results and screenshots or notes in the change verification evidence.

## 6. Full Verification and Handoff

- [x] 6.1 Run frontend component tests, type-check, lint, production build, affected backend contract tests, `git diff --check`, and strict OpenSpec validation; record exact results in `PROJECT_STATE.md`.
- [ ] 6.2 Perform keyboard-only and screen-reader spot checks for Quick Capture, movement, ledger edit, recurring, category, goal, obligation, trade, registration, and settings forms; document remaining limitations without claiming WCAG conformance.
- [ ] 6.3 Review the final diff for duplicate business logic, accidental design churn, invalid defaults, inaccessible states, stale query keys, secrets, and unrelated changes; verify every approved scenario has implementation evidence before user acceptance.
