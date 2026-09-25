# Proposal

## Why

Financial input behavior is currently spread across duplicated modals with different fields, defaults, validation, and accessibility semantics. Some controls silently discard values or submit classifications that contradict the selected category, making routine data entry inconsistent and error-prone across desktop and mobile.

## What Changes

- Establish one shared contract and component path for transaction capture, internal movement, account selection, monetary entry, dates, validation feedback, and submission state.
- Remove or consolidate duplicate and obsolete transaction, movement, and export forms so each action has one maintained implementation across entry points.
- Make Quick Capture inherit the selected category's Kakeibo classification unless the user deliberately overrides it, and expose receipt attachment through the supported receipt API.
- Complete recurring-rule management with create, edit, deactivate, delete, weekday selection, type-correct categories, explicit payroll inclusion, and clear auto-post behavior.
- Complete category management with income/expense kind, Kakeibo pillar, primary/optional status, budget, icon, color, and archive behavior.
- Align registration and settings fields with backend persistence, supported currency values, validation, and active query invalidation.
- Complete goal, obligation, account, trade, payroll, and ledger edit forms with the fields and constraints required by their domain contracts.
- Show editable seconds in transaction and movement date-time fields on create and edit while leaving compact ledger date displays unchanged.
- Give every form associated labels, stable names, appropriate autocomplete/input modes, field-level errors, live submission feedback, keyboard submission, and focus movement to the first invalid field.
- Upgrade shared dialogs with initial focus, focus containment, Escape handling, focus restoration, unique accessible names, scroll containment, and reduced-motion behavior.
- Expose selection state and accessible names for icon-only, color, category, account, and segmented-choice controls.
- Reduce repeated form captions and instructional paragraphs: use concise, persistent field labels; put genuinely necessary field guidance behind an accessible information control; keep errors and consequential financial warnings visible in context.

## Capabilities

### New Capabilities

- `accessible-financial-input-forms`: Defines shared form semantics, validation feedback, modal focus behavior, keyboard operation, responsive consistency, and reduced-motion requirements.

### Modified Capabilities

- `transaction-ledger-management`: Completes quick capture and ledger editing, including receipts, classification, seconds-precise date entry, and one canonical movement form.
- `automated-transactions`: Completes recurring-rule create/edit scheduling controls and type-correct selections.
- `payroll-allocation-flow`: Makes payroll inclusion explicit and aligns allocation validation and feedback with the shared form contract.
- `custom-category-management`: Exposes every persisted category attribute and filters category choices by transaction type.
- `financial-goals-tracker`: Distinguishes standalone progress adjustment from account-backed balance tracking in the user interface.
- `investment-trade-recording`: Presents funding destination, position limits, and settlement validation consistently for buys and sales.
- `currency-conversion-and-settings`: Aligns registration/profile persistence and restricts currency choices to supported values.
- `mobile-native-layout`: Keeps canonical forms functionally equivalent across modal and mobile bottom-sheet presentations.
- `obligations-debt-tracker`: Adds coherent cross-field validation and accessible debt-entry feedback.

## Impact

- Shared frontend primitives: Modal, Input, MoneyInput, account/category selectors, errors, buttons, and form state helpers.
- Feature UI: authentication, settings, quick capture, ledger, accounts, goals, obligations, investments, categories, recurring rules, and payroll allocation.
- Query cache keys and mutations are normalized so successful writes refresh the screens that display the changed data.
- Backend request models receive only compatibility and validation alignment required by the completed forms; core financial settlement changes remain owned by the separate integrity change.
- Verification adds component/integration tests plus keyboard, focus, responsive, and automated accessibility checks for affected forms.
