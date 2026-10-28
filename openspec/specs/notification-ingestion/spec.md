# notification-ingestion Specification

## Purpose
Define supported banking notification ingestion and resolve named Bank Jago pocket movement endpoints.

## Requirements

### Requirement: Bank Jago Child Pocket Transfer Resolution
The system SHALL resolve source and destination child pockets when ingesting internal movement notifications from Bank Jago (`com.jago.digitalbanking` / `com.jago.digitalBanking`).

#### Scenario: Required behavior and constraints
- **WHEN** a Jago internal movement notification is ingested
- **THEN** the following required behavior and constraints hold:

  The system SHALL resolve source and destination child pockets when ingesting internal movement notifications from Bank Jago (`com.jago.digitalbanking` / `com.jago.digitalBanking`):
  1. When a notification matches pocket-to-pocket movement with `source_pocket` and `target_pocket`, the ingestion engine SHALL search the user's active child accounts (`parent_id = bank_jago_id`) for matching names.
  2. The matching algorithm SHALL support exact name matching, normalized name matching (ignoring the redundant word "Pocket"), and case-insensitive substring matching.
  3. If `source_pocket` matches a child pocket, `transactions.account_id` SHALL be assigned that child pocket's ID; otherwise, it SHALL fall back to the parent Bank Jago account ID.
  4. If `target_pocket` matches a child pocket, `transactions.transfer_target_account_id` SHALL be assigned that child pocket's ID.
  5. The created transaction SHALL have `type = 'transfer'` and `kakeibo_type = NULL`.

#### Scenario: Moving funds between Main Pocket and GoPay Tabungan Pocket
- **WHEN** Bank Jago sends a notification *"Rp500.000 has been moved from your Main Pocket Pocket to your GoPay Tabungan Pocket"*
- **THEN** the system SHALL create a transaction with `type = 'transfer'`
- **AND** `account_id` SHALL match the child pocket "Kantong Utama" / "Main" or parent Bank Jago
- **AND** `transfer_target_account_id` SHALL match the child pocket "GoPay Tabungan"
- **AND** `kakeibo_type` SHALL be `NULL`

#### Scenario: Moving funds between custom pockets
- **WHEN** Bank Jago sends a notification *"Rp50.000 has been moved from your Jajan Pocket to your Tabungan Pocket"*
- **THEN** the system SHALL link `account_id` to the "Jajan" child pocket and `transfer_target_account_id` to the "Tabungan" child pocket
- **AND** `type` SHALL be `'transfer'` and amount SHALL be `50000`

### Requirement: Multi-App Notification Ingestion Scope
The system SHALL support push notification ingestion from the 6 active registered financial companion apps.

#### Scenario: Required behavior and constraints
- **WHEN** a companion application submits a notification for ingestion
- **THEN** the following required behavior and constraints hold:

  The system SHALL support push notification ingestion from the 6 active registered financial companion apps:
  1. `myBCA` (`com.bca.mybca.omni.android`, `id.co.bca.mybca.omni.android`)
  2. `BCA mobile` (`com.bca`)
  3. `Bank Jago` (`com.jago.digitalbanking`, `com.jago.digitalBanking`)
  4. `GoPay` (`com.gojek.gopay`, `com.gojek.app`, `com.gopay.wallet`)
  5. `ShopeePay` (`com.shopeepay.id`)
  6. `Stockbit` (`com.stockbit.android`)
  The system SHALL NOT process unsupported apps (such as Blu by BCA) and SHALL postpone Bibit ingestion until real notification payload samples are provided.

#### Scenario: Ingesting valid financial notification from supported app
- **WHEN** an incoming batch contains a notification event from `com.gojek.app` for an outbound payment
- **THEN** the system SHALL parse the event, map it to the user's GoPay account, and record the transaction in the ledger

#### Scenario: Ingesting notification from unmapped source
- **WHEN** an incoming notification is received without a matching configured institution
- **THEN** the system SHALL fall back to the user's primary liquid account without crashing

### Requirement: Companion Ledger Edit Identity
The backend SHALL distinguish an event identifier and logical result key from the actual ledger identifier used for editing.

#### Scenario: Required behavior and constraints
- **WHEN** the companion opens a ledger edit from a notification result
- **THEN** the following required behavior and constraints hold:

  The backend SHALL distinguish an event identifier and logical result key from the actual ledger identifier used for editing. Recorded ordinary expense/income results SHALL optionally expose `transaction_id` only when a current owned ordinary transaction still exists and is not a movement leg or trade. Movement/trade, deleted, queued, ignored, review, and failed results SHALL NOT expose an editable ordinary transaction identifier. The field SHALL be additive to the approved compact result contract and SHALL NOT change record keys, amounts, financial effects, or owner isolation. Existing clients SHALL remain able to ignore it.

#### Scenario: Ordinary recorded result
- **WHEN** an owned ordinary expense or income has committed and remains an ordinary ledger record
- **THEN** its batch/lookup result includes the actual editable transaction UUID separately from `event_id` and `record_key`

#### Scenario: Movement or deleted record
- **WHEN** the result identifies a movement/trade or its ordinary ledger row no longer exists
- **THEN** the result does not provide an editable ordinary transaction UUID and no transaction is recreated

#### Scenario: Foreign event or pending work
- **WHEN** a caller requests another owner's event or a record has not committed
- **THEN** no editable transaction identity is revealed

#### Scenario: Older companion compatibility
- **WHEN** a companion ignores the additional identity field
- **THEN** existing batch counters and compact financial result fields keep their approved behavior
