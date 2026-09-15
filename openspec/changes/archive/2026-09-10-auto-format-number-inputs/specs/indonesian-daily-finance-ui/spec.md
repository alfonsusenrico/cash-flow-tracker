## ADDED Requirements

### Requirement: Automatic Thousands Dot Separation on Number and Amount Inputs
The user interface SHALL automatically format all numeric currency and amount input fields with Indonesian thousand separator periods (`.`) in real time as the user types:
1. When typing digits, the displayed value SHALL dynamically group digits with dots (e.g. `20000000` is displayed as `20.000.000`).
2. When deleting digits, backspacing, or clearing the input, the dots SHALL automatically readjust to reflect the remaining digits.
3. Submitting the form or mutating state SHALL parse out all non-digit characters so that API payloads receive clean integer amounts.
4. If a user pastes numbers containing dots, commas, or spaces, the input SHALL extract only digits and reformat cleanly with thousand dots.
5. All numeric input fields across Goals, Obligations, Accounts, Dashboard Transfers, Ledger Edits, and Category Budgets SHALL utilize this automatic dot formatting.

#### Scenario: User enters large amount in goal target field
- **WHEN** a user enters `20000000` into the Target Nominal input field in the Goals modal
- **THEN** the input display automatically shows `20.000.000` in real time without requiring manual punctuation

#### Scenario: User edits or backspaces formatted number
- **WHEN** a user deletes the last digit from an input showing `20.000.000`
- **THEN** the input immediately reformats and displays `2.000.000`

#### Scenario: Form submission sends clean integer
- **WHEN** a user submits a goal or transaction with `20.000.000` in the amount input
- **THEN** the application sends integer `20000000` to the backend API
