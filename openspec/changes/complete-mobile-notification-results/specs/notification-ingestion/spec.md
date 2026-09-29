# Spec Delta

## ADDED Requirements

### Requirement: Companion Ledger Edit Identity
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
