# text-capture Specification

## Purpose
Record transactions the owner types as one short Indonesian sentence on the phone, asking only for details that cannot be worked out.

## Requirements

### Requirement: Deterministic Money Facts
The backend SHALL take the amount, kind and date of a typed capture only from deterministic parsing of the text. The model SHALL NOT set an amount or a date. Exactly one money expression SHALL be required for an amount.

#### Scenario: Indonesian shorthand amounts
- **WHEN** the text is "bayar parkir ptc 10rb", "kopi 25k", "servis motor 1,5jt" or "beli bensin Rp20.000"
- **THEN** the parsed amounts SHALL be 10.000, 25.000, 1.500.000 and 20.000.

#### Scenario: Missing or several amounts are asked
- **WHEN** the text is "bayar parkir" or "parkir 10rb dan kopi 25rb"
- **THEN** no transaction SHALL be recorded
- **AND** the response SHALL be `needs_detail` asking for the amount, or asking to send the items separately.

#### Scenario: Yesterday
- **WHEN** the text is "kemarin makan bakso 20rb" sent on 29 Oct
- **THEN** the transaction date SHALL be 28 Oct in Asia/Jakarta.

### Requirement: Confident Captures Are Recorded
`POST /api/capture/text` SHALL record the transaction immediately when the amount, kind, account and classification are certain. Classification is certain from owner merchant memory, or from a model confidence at or above the configured threshold (default 0.8). The recorded row SHALL have `source = capture`, an informative description, and `review_status = reviewed`.

#### Scenario: Known merchant from memory
- **GIVEN** the owner earlier recorded "parkir ptc" as Parkir / Transportasi / Kebutuhan, paid with Tunai
- **WHEN** the owner types "bayar parkir ptc 10rb"
- **THEN** a Rp 10.000 expense on Tunai SHALL be recorded with that classification, `classified_by = rule`, and no model call.

#### Scenario: New merchant with a confident model
- **GIVEN** no memory for "es teh manis" and an explicit "tunai" in the text
- **WHEN** the model returns a category and pillar with confidence 0.9
- **THEN** the expense SHALL be recorded with `classified_by = ai`.

### Requirement: Targeted Follow-up Questions
When a capture is not certain, the response SHALL be `needs_detail` with exactly one question for the first missing field, in this order: amount, kind, account, then category with pillar. `POST /api/capture/{id}/answer` SHALL validate the answer and either record or ask the next question. Answers SHALL be idempotent per reply id, and a stale question SHALL return 409 `stale_capture_question`.

#### Scenario: Unknown account is asked
- **GIVEN** no account mention, no memory and no clear history for "beli pulsa 50rb"
- **WHEN** the owner sends it
- **THEN** the question SHALL be "Dari rekening mana?" with up to 6 liquid accounts, most recently used first
- **AND** answering with GoPay SHALL record the expense on GoPay with `classified_by = owner` for the parts the owner answered.

#### Scenario: Provider unavailable
- **WHEN** the model cannot be reached
- **THEN** the capture SHALL ask for the category with up to 5 ranked category chips instead of guessing.

### Requirement: Own-Account Transfers From Text
Text describing a transfer between the owner's accounts SHALL be recorded as one bilateral movement with equal legs, never as spending or income. Examples are "tarik tunai", "setor tunai", "pindah … ke …" and "isi saldo … ke …". "tarik tunai" SHALL use the Cash account as target.

#### Scenario: Cash withdrawal
- **GIVEN** the owner usually withdraws from BCA Harian
- **WHEN** the owner types "tarik tunai 200rb"
- **THEN** a Rp 200.000 movement BCA Harian → Tunai SHALL be recorded, and neither leg SHALL count as spending.

### Requirement: Undo Window
A recorded capture SHALL be undoable through `POST /api/capture/{id}/undo` for 10 seconds after recording, plus 5 seconds of network grace, provided the transaction has not changed. Undo SHALL remove the created rows and restore balances. After the window the response SHALL be 409 `undo_expired`.

#### Scenario: Undo within the window
- **WHEN** the owner presses "Batalkan" 4 seconds after a capture was recorded
- **THEN** the transaction SHALL be deleted, the capture SHALL be `undone`, and balances SHALL be as before.

#### Scenario: Undo too late or after an edit
- **WHEN** undo arrives 20 seconds later, or after the owner edited the transaction
- **THEN** the response SHALL be 409 and nothing SHALL change.

### Requirement: Captures Teach Memory
A recorded capture SHALL append one classification feedback row of kind `capture`, with the local weekday and hour, amount, account and key. It SHALL also upsert merchant memory: as `owner` when the owner answered a question or named the merchant, otherwise as `ai`.

#### Scenario: Second capture needs no questions
- **GIVEN** the owner answered "Dari rekening mana?" with Tunai for "parkir ptc"
- **WHEN** "parkir ptc 10rb" is typed again
- **THEN** it SHALL be recorded without a question.

### Requirement: Capture Input Boundaries
The capture endpoints SHALL accept only the owner's own captures and SHALL bound the text to 1–200 characters. They SHALL be idempotent on the client capture id. Logs SHALL hold ids, status and reason codes only, never the text or amounts.

#### Scenario: Repeated submission
- **WHEN** the phone resends the same capture id after a timeout
- **THEN** the response SHALL describe the same capture, and no second transaction SHALL exist.

#### Scenario: Invalid or foreign requests
- **WHEN** the text is empty or longer than 200 characters, or the capture belongs to another user
- **THEN** the response SHALL be 422 or 404, and nothing SHALL change.
