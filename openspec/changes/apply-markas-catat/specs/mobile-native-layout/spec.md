## MODIFIED Requirements

### Requirement: Mobile Bottom-Sheet Quick Capture
The application SHALL render the capture dialog, both its sentence and manual views, as a touch-first bottom sheet below 768 px.

#### Scenario: Required behavior and constraints
- **WHEN** an owner opens capture on a mobile screen
- **THEN** the following required behavior and constraints hold:

  The application SHALL render the capture dialog as a touch-first bottom sheet on mobile screens:
  1. The sheet SHALL have rounded top corners, a visible handle, contained scrolling, accessible dialog semantics, and safe focus behavior.
  2. Amount entry SHALL use high-visibility tabular figures and a numeric input mode without preventing hardware-keyboard entry.
  3. Category selection SHALL use the same select grouped by pillar as desktop, filtered by transaction type.
  4. Kakeibo choices SHALL be one-tap segment options whose selected state is exposed programmatically and whose default follows the selected category.
  5. Account, date, notes, goal or obligation context, receipt attachment, credit terms, errors, and submission results SHALL be functionally equivalent to desktop.
  6. On opening, focus SHALL go to the sheet's title so the soft keyboard does not cover the context.
  7. The sheet SHALL expose `Perpindahan` with eligible source and destination accounts, stacked below 480 px, amount, seconds-precise timestamp, notes, and movement validation through the same atomic movement contract as desktop.

#### Scenario: Opening quick capture on mobile
- **WHEN** a user opens Catat on a 390px viewport
- **THEN** the sentence view and, through "Isi manual", the complete manual fields are available in one touch-first bottom sheet without losing keyboard or assistive-technology operation

#### Scenario: Selecting a category on mobile
- **WHEN** a user selects a category classified as `want`
- **THEN** the Kakeibo segment announces "Keinginan" as selected

#### Scenario: Recording a movement on mobile
- **WHEN** a user selects `Perpindahan` and submits a valid movement in the mobile bottom sheet
- **THEN** the same atomic movement contract is used, with no transaction-only values included
