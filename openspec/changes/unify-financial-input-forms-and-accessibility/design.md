# Design

## Context

See `proposal.md` for motivation. The frontend already includes React Hook Form and Zod, but active forms mostly manage independent local state and repeat account, amount, date, error, and modal patterns. `AppLayout` owns shared quick-capture and movement dialogs, while Home and Accounts also contain separate movement implementations. The shared Modal exposes dialog semantics but not complete focus management. The change depends on the backend integrity contracts for goal adjustments, movements, trades, receipts, and error payloads.

## Goals / Non-Goals

**Goals:**

- Give each financial action one canonical schema, submission adapter, and responsive presentation path.
- Make form defaults reflect persisted domain metadata rather than hard-coded classifications.
- Make validation and focus behavior predictable for touch, mouse, keyboard, and assistive technology users.
- Remove dead or duplicated form implementations after all entry points migrate.

**Non-Goals:**

- Restyle unrelated dashboard content or introduce a new visual design system.
- Replace React Query, React Hook Form, Zod, Tailwind, or existing domain APIs wholesale.
- Implement the backend settlement and migration work owned by the integrity change.
- Claim WCAG conformance solely from automated checks.

## Decisions

### 1. Feature schemas are the single form contract

Use React Hook Form with feature-level Zod schemas for transaction capture, movement, recurring rule, category, goal adjustment, obligation, account, trade, payroll, registration, and settings flows. Schemas express client-visible required and cross-field constraints while the server remains authoritative for balances, ownership, and concurrency.

Shared primitives accept standard identifiers, descriptions, error IDs, and refs instead of owning business state. This avoids a universal mega-form abstraction while eliminating repeated semantics.

### 2. One canonical component per financial action

Keep `QuickCaptureModal` as the canonical transaction capture surface and `InternalMovementModal` as the canonical movement surface, extracting form bodies only where desktop/mobile containers genuinely differ. Home, Accounts, and global triggers pass defaults into these components rather than render separate forms. Remove unused legacy `TransactionModal` and `ExportModal` only after confirming no runtime import remains; export itself is not added by this change.

Ledger editing uses a transaction edit form for ordinary entries and calls the canonical movement edit contract for linked movements. Investment trades remain a focused form because unit and price semantics differ from ordinary transactions.

### 3. Category metadata drives defaults

Quick Capture filters categories by selected transaction type. Selecting an expense category applies its Kakeibo pillar as the default. A user override is tracked explicitly so later category changes can establish a new default without overwriting an intentional choice. Income does not submit a Kakeibo pillar.

Category management exposes kind, pillar, primary status, budget, icon, and color with compatibility rules. Recurring forms use the same kind-filtered category selector.

### 4. Goal UI follows backing mode

Account-backed goals show linked accounts and explanatory derived progress, with no Setor action. Standalone goals expose a progress-adjustment dialog that has no account selector and states that it creates no ledger transaction. Switching backing mode requires a confirmation explaining the source-of-truth change.

### 5. Receipt capture is a recoverable two-step flow

Transaction creation occurs first to obtain an ID, followed by receipt upload. If upload fails, the valid transaction remains and the UI shows a retryable attachment error rather than claiming the entire transaction failed. Ledger edit can add or replace the attachment through the same control. Client validation improves feedback but never replaces server content validation.

### 6. Shared dialog focus manager without a new runtime dependency

Enhance Modal with generated title/description IDs, saved trigger focus, initial-focus selection, Tab/Shift+Tab containment, Escape handling, scroll and overscroll containment, and reduced-motion classes. Forms mark the preferred initial control. Bottom-sheet presentation remains CSS-responsive and uses the same dialog behavior.

A new dialog library was considered but rejected because the required behavior is bounded and adding a second component model would increase migration scope. Keyboard and assistive-technology verification is mandatory because focus code is easy to regress.

### 7. Standard error and submission model

Map validation failures to field errors and server domain codes to either the relevant field or a form summary with `aria-live`. The first invalid field receives focus. Submit controls expose pending state, prevent duplicate calls, and retain values after errors. Destructive actions use an accessible confirmation dialog rather than browser `confirm` where the action requires contextual explanation.

### 8. Canonical query keys

Centralize query-key factories for accounts, transactions, dashboard, goals, obligations, categories, recurring rules, pending occurrences, and settings. Mutation success handlers invalidate domain keys, not ad hoc strings such as `ledger`, `transactions`, and `transactions-ledger` for the same resource.

### 9. Verification tooling is proportional and explicit

Add a focused frontend test setup using Vitest, Testing Library, user-event, and an axe integration. Component tests cover schema behavior, keyboard submit, focus containment/restoration, accessible names/states, and canonical mutation payloads. Existing type-check, lint, and production build remain required. A real-browser keyboard, mobile viewport, zoom/reflow, and screen-reader spot check remains a manual acceptance task rather than an automated conformance claim.

### 10. Deposit opening value is principal only

The current backend uses `avg_buy_price` as a per-unit cost in account valuation. The prior deposit form labeled that field as annual interest, so submitting a rate would misstate the position. Hide unit and average-price controls for deposit account/pocket creation, submit the entered principal as `initial_balance`, and leave existing records untouched. Interest accrual requires a separate persisted contract and is outside this change.

### 11. Concise form copy with accessible contextual help

Audit every active input form and remove helper text that only repeats a label, section heading, or obvious interaction. Keep short, persistent labels and units. For unusual concepts such as derived goal progress, investment-only trade paths, and debt allocation, place concise explanation behind an information button next to the relevant label or section title. The help opens on click/tap, keyboard activation, and optional hover; Escape and outside activation dismiss it, and focus remains predictable. Associate the help with its field or group for assistive technology. Do not make essential validation, irreversible consequences, transaction status, or errors hover-only: show those inline when relevant. Reuse one help primitive rather than one-off tooltip implementations.

## Risks / Trade-offs

- **[Backend integrity APIs land after form work]** → Sequence shared primitives and non-contract UI first, then wire goal, movement, trade, and receipt forms only against the approved backend contract.
- **[Large form migration can change defaults unintentionally]** → Add payload regression tests before removing each old implementation and migrate one action at a time.
- **[Custom focus trapping can miss edge cases]** → Test nested interactive content, disabled controls, no-focusable-content fallback, focus restoration, and mobile sheets.
- **[Two-step receipt upload can partially succeed]** → Present the transaction as saved and the attachment as retryable; never resubmit the transaction automatically.
- **[More explicit controls can increase visual density]** → Reveal type-specific fields conditionally while preserving all values and accessible relationships.
- **[Removing captions can hide important meaning]** → Classify each caption as redundant, optional guidance, or essential state; retain essential state visibly and verify help via keyboard, touch, and screen reader.
- **[Added test dependencies increase maintenance]** → Pin them in the existing lockfile and limit setup to affected component behavior.

## Migration Plan

1. Add query-key factories, semantic form primitives, error summary, and enhanced Modal with focused regression tests.
2. Consolidate movement entry points and Quick Capture while preserving current routes.
3. Complete transaction edit and receipt controls after integrity endpoints are available.
4. Migrate recurring, payroll, category, goal, obligation, account, trade, registration, and settings forms feature by feature.
5. Remove confirmed-dead and replaced form code, then run repository searches for stale query keys and duplicate action forms.
6. Run component tests, type-check, lint, build, affected backend tests, and manual keyboard/mobile/accessibility verification.

Rollback keeps the shared primitives additive until each migrated feature is verified. If one feature regresses, its entry points can temporarily return to the previous component without reverting unrelated completed migrations; dead components are removed only at the final verified stage.
