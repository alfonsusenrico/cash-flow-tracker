## REMOVED Requirements

### Requirement: Default Bahasa Indonesia Localization
**Reason**: The owner made English the default interface language on 2026-11-06, with Indonesian as a per-account choice.
**Migration**: Replaced by `interface-language` ("Supported Interface Languages", "Interface Language Resolution", "Document Language"). Existing accounts are migrated to Indonesian, so their interface does not change.

## MODIFIED Requirements

### Requirement: Automatic Thousands Dot Separation on Number and Amount Inputs
Every currency and amount input SHALL group digits with the account region's thousands separator in real time: a dot for Indonesia (`20.000.000`), a comma for the United States (`20,000,000`). Deleting or clearing SHALL regroup the remaining digits, pasted dots, commas and spaces SHALL be reduced to digits, and every submission SHALL send a clean integer. This SHALL hold for every amount input in every language.

#### Scenario: Required behavior and constraints
- **WHEN** an owner types or pastes a numeric amount into a financial input
- **THEN** the following required behavior and constraints hold:

  1. When typing digits, the displayed value SHALL dynamically group digits with the region's separator (e.g. `20000000` is displayed as `20.000.000` in region Indonesia).
  2. When deleting digits, backspacing, or clearing the input, the separators SHALL automatically readjust to reflect the remaining digits.
  3. Submitting the form or mutating state SHALL parse out all non-digit characters so that API payloads receive clean integer amounts.
  4. If a user pastes numbers containing dots, commas, or spaces, the input SHALL extract only digits and reformat cleanly with the region's separator.
  5. All numeric input fields across Goals, Obligations, Accounts, Dashboard Transfers, Ledger Edits, and Category Budgets SHALL utilize this automatic grouping.

#### Scenario: User enters large amount in goal target field
- **WHEN** a user in region Indonesia enters `20000000` into the Target Nominal input field in the Goals modal
- **THEN** the input display automatically shows `20.000.000` in real time without requiring manual punctuation

#### Scenario: User edits or backspaces formatted number
- **WHEN** a user deletes the last digit from an input showing `20.000.000`
- **THEN** the input immediately reformats and displays `2.000.000`

#### Scenario: Form submission sends clean integer
- **WHEN** a user submits a goal or transaction with `20.000.000` in the amount input
- **THEN** the application sends integer `20000000` to the backend API

#### Scenario: Region United States
- **WHEN** a user in region United States types `20000000` into an amount input
- **THEN** the input SHALL show `20,000,000` and submit `20000000`

#### Scenario: Language does not change grouping
- **WHEN** an English account in region Indonesia types `20000000`
- **THEN** the input SHALL show `20.000.000`
