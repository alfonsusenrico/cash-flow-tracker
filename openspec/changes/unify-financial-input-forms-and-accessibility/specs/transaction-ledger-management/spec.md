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

### Requirement: Internal Movement Capture Mode
The canonical Quick Capture form SHALL offer `Pengeluaran`, `Pemasukan`, and `Perpindahan` modes.

#### Scenario: Required behavior and constraints
- **WHEN** an owner selects and submits an internal movement in Quick Capture
- **THEN** the following required behavior and constraints hold:

  The canonical Quick Capture form SHALL offer `Pengeluaran`, `Pemasukan`, and `Perpindahan` modes. Perpindahan SHALL collect a positive amount, distinct eligible liquid source and destination accounts, timestamp with seconds, and optional notes, and SHALL submit through the existing atomic movement creation contract rather than the ordinary transaction endpoint. It SHALL NOT submit transaction-only category, Kakeibo, goal, obligation, or receipt fields. Movement creation SHALL preserve the existing bilateral account-balance and consolidated-ledger behavior.

  The Accounts page SHALL NOT expose dedicated transfer controls in account menus, pocket actions, or page-level desktop/mobile actions. Home movement shortcuts SHALL open Quick Capture in Perpindahan mode. Movement editing and deletion from ledger detail SHALL continue to use the atomic movement edit/delete contract.

#### Scenario: Creating an internal movement from Quick Capture
- **WHEN** a user selects `Perpindahan`, chooses two different eligible liquid accounts, enters a positive amount, and saves
- **THEN** the client calls the existing atomic movement creation contract with source, destination, amount, date, and optional notes, and creates no ordinary transaction request

#### Scenario: Rejecting an invalid movement selection
- **WHEN** a user selects the same account as source and destination, or does not provide two eligible accounts or a positive amount
- **THEN** the form identifies the invalid field, focuses the first invalid control, and sends no movement request

#### Scenario: Opening movement capture from Home
- **WHEN** the user activates a Home movement shortcut
- **THEN** Quick Capture opens with `Perpindahan` selected and exposes the canonical movement fields

#### Scenario: Editing an existing ledger movement
- **WHEN** a user opens a consolidated movement row and edits it
- **THEN** the existing atomic movement edit flow updates both linked records while the ledger continues to show one consolidated row

### Requirement: Complete Ledger Editing
The transaction detail form SHALL expose every safely editable transaction property, including type where compatible, account, category, classification, date, notes, goal or obligation association, and receipt. Movement rows SHALL delegate source, target, amount, notes, and date changes to the canonical atomic movement operation.

#### Scenario: Required behavior and constraints
- **WHEN** an owner inspects or edits a ledger transaction
- **THEN** the following required behavior and constraints hold:

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
