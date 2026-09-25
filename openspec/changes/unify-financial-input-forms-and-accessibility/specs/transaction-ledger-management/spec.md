# Spec Delta

## ADDED Requirements

### Requirement: Complete Canonical Transaction Capture
The application SHALL provide one canonical transaction-capture form that supports income and expense type, positive amount, active account, type-compatible category, transaction-level Kakeibo classification, timestamp, notes, mutually compatible goal or obligation context, and optional receipt attachment. Selecting a category SHALL default the transaction classification from category metadata unless the user explicitly changes it.

#### Scenario: Inheriting category classification
- **WHEN** a user selects an expense category classified as `want` and does not override it
- **THEN** the submitted transaction uses `want` rather than a hard-coded default

#### Scenario: Attaching a receipt during capture
- **WHEN** a user records a transaction with a valid receipt
- **THEN** the transaction is created and the receipt is attached through the supported receipt contract with visible success or failure feedback

### Requirement: Complete Ledger Editing
The transaction detail form SHALL expose every safely editable transaction property, including type where compatible, account, category, classification, date, notes, goal or obligation association, and receipt. Movement rows SHALL delegate source, target, amount, notes, and date changes to the canonical atomic movement operation.

Transaction capture, movement create/edit, and ordinary ledger edit SHALL show and allow editing seconds in their date-time control. Opening an existing record SHALL retain its stored second value, and submitting an unchanged control SHALL NOT silently round the timestamp to the minute. Compact ledger date/time display SHALL remain unchanged.

#### Scenario: Editing transaction classification
- **WHEN** a user changes a normal expense from `need` to `want`
- **THEN** the updated classification is persisted and affected analytics refresh

#### Scenario: Editing a timestamp with seconds
- **WHEN** a user opens an existing transaction or movement recorded at 12:34:27
- **THEN** the form shows 12:34:27 and submitting preserves that second rather than changing it to 12:34:00

### Requirement: Consistent Post-Mutation Refresh
After a successful financial form mutation, every active view whose displayed data changed SHALL refresh using the canonical cache identity for that resource.

#### Scenario: Updating an investment trade from its modal
- **WHEN** a trade succeeds
- **THEN** accounts, ledger, dashboard, and net-worth views invalidate their active canonical queries
