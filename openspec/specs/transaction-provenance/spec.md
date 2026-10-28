# transaction-provenance Specification

## Purpose
Record, for every ledger transaction, where it came from, which merchant it was, whether the owner
still needs to review it and who classified it, so review, learning and reports can rely on it.

## Requirements

### Requirement: Transaction Provenance Fields
Every transaction SHALL carry `source` (manual, notification, recurring, payroll, movement, reconcile, investment or import), `review_status` (auto, needs_review or reviewed), `classified_by` (rule, ai, owner, system or empty), `classification_confidence` (empty or 0 to 1), `owner_edited_at` (empty or a time) and `merchant` (empty or a display name of 1–120 characters). The database SHALL reject any other value. `notes` SHALL remain the human description.

#### Scenario: Unknown source is rejected by the database
- **WHEN** any writer stores a transaction with `source = 'bot'`
- **THEN** the database SHALL reject the write
- **AND** no row SHALL change.

#### Scenario: Confidence outside the range is rejected
- **WHEN** any writer stores `classification_confidence = 1.5`
- **THEN** the database SHALL reject the write.

#### Scenario: Notes keep the description
- **GIVEN** a notification-recorded expense described "Bayar parkir"
- **WHEN** the owner sets its merchant to "Parkir Mal Contoh"
- **THEN** `notes` SHALL still be "Bayar parkir"
- **AND** `merchant` SHALL be "Parkir Mal Contoh".

### Requirement: Provenance Written by Every Recording Path
Each path that creates a transaction SHALL write its provenance explicitly: manual create → manual, reviewed, owner; movement → movement, reviewed, system; recurring → recurring, reviewed, system; payroll → payroll, reviewed, system; reconcile → reconcile, reviewed, system; investment trade or top-up → investment, reviewed, system; notification → notification, auto or needs_review (per the record-and-flag reasons), ai or rule (rule for memory hits).

#### Scenario: Manual expense from the web
- **WHEN** the owner records an expense through `POST /api/transactions`
- **THEN** the row SHALL have `source = manual`, `review_status = reviewed` and `classified_by = owner`.

#### Scenario: Both legs of a movement
- **WHEN** the owner moves Rp 500.000 between two pockets
- **THEN** both legs SHALL have `source = movement`, `review_status = reviewed` and `classified_by = system`.

#### Scenario: Scheduled recurring payment
- **WHEN** a recurring rule posts its occurrence
- **THEN** the posted row or legs SHALL have `source = recurring` and `classified_by = system`.

#### Scenario: Payroll allocation
- **WHEN** the owner executes a payroll allocation batch
- **THEN** every leg SHALL have `source = payroll` and `classified_by = system`.

#### Scenario: Reconcile adjustment
- **WHEN** the owner reconciles an account and an adjustment is recorded
- **THEN** the adjustment SHALL have `source = reconcile` and `classified_by = system`.

#### Scenario: Investment top-up and trade
- **WHEN** the owner records an amount top-up or a Beli/Jual trade
- **THEN** every leg SHALL have `source = investment` and `classified_by = system`.

#### Scenario: Notification classified by the model
- **WHEN** a notification naming a merchant is recorded from an AI interpretation with confidence 0.82 and review reason none
- **THEN** the row SHALL have `source = notification`, `review_status = auto`, `classified_by = ai` and `classification_confidence = 0.82`.

#### Scenario: Notification classified by deterministic rules
- **WHEN** a notification naming a merchant is recorded by the deterministic path with a category, or as a broker trade
- **THEN** every row it creates SHALL have `source = notification`, `review_status = auto`, `classified_by = rule` and no confidence.

#### Scenario: Owner resolves an unresolved notification
- **WHEN** the owner records a needs-review notification with their own type, account and category
- **THEN** the row SHALL have `source = notification`, `review_status = reviewed` and `classified_by = owner`.

#### Scenario: Notification recorded with a review reason
- **WHEN** a notification row is recorded without a category, below the confidence gate, through the model fallback, unpaired among ambiguous counterparts, or with a purpose question
- **THEN** the row SHALL have `source = notification` and `review_status = needs_review`.

#### Scenario: Notification classified from merchant memory
- **WHEN** a notification row is recorded from an owner merchant-memory entry
- **THEN** the row SHALL have `source = notification`, `review_status = auto`, `classified_by = rule` and no confidence.

### Requirement: Provenance Backfill for Existing Transactions
The migration SHALL assign provenance to every existing row once, deterministically, without changing `updated_at`, amount, date, account, category, notes or pillar. Source SHALL follow the first matching rule in the design's order; review status SHALL be auto only for notification rows never edited; classifier SHALL follow the event's interpretation source for notification rows.

#### Scenario: Notification row never edited
- **GIVEN** a row linked from a notification event whose interpretation source is `ai`, with `updated_at` 20 seconds after `created_at`
- **WHEN** the migration runs
- **THEN** the row SHALL get `source = notification`, `review_status = auto` and `classified_by = ai`.

#### Scenario: Notification row edited later
- **GIVEN** a row with idempotency key `notification:<event id>`, deterministic interpretation, and `updated_at` two days after `created_at`
- **WHEN** the migration runs
- **THEN** the row SHALL get `source = notification`, `review_status = reviewed` and `classified_by = rule`
- **AND** `updated_at` SHALL be unchanged.

#### Scenario: Key prefixes and links
- **GIVEN** rows with keys `recurring:…` and `payroll:…`, a row with only a recurring rule link, legs of an investment top-up, legs of a trade on an investment position, a "Penyesuaian Saldo" adjustment, legs of a plain movement and a plain row
- **WHEN** the migration runs
- **THEN** their sources SHALL be recurring, payroll, recurring, investment, investment, reconcile, movement and manual
- **AND** the plain row SHALL be classified by owner and the others by system, all reviewed.

#### Scenario: Backfill runs once
- **GIVEN** the migration has run and the owner has since edited rows
- **WHEN** the API starts and runs the startup mirror
- **THEN** no provenance value SHALL change.

### Requirement: Owner Edits Mark Rows Reviewed
When the owner changes a row's category, pillar, notes, merchant, type or offset category through a transaction, movement, merge or top-up edit, the system SHALL set `owner_edited_at` to now, `classified_by = owner`, clear the confidence and set `review_status = reviewed` unless the request sets a review status itself. Edits that change none of these fields SHALL leave provenance alone.

#### Scenario: Owner recategorises an AI row
- **GIVEN** an AI-classified row with `review_status = auto` and confidence 0.7
- **WHEN** the owner changes its category
- **THEN** the row SHALL have `review_status = reviewed`, `classified_by = owner`, no confidence and `owner_edited_at` set.

#### Scenario: Owner corrects only the amount
- **GIVEN** an AI-classified row with `review_status = auto`
- **WHEN** the owner changes only the amount
- **THEN** `review_status`, `classified_by` and `owner_edited_at` SHALL be unchanged.

#### Scenario: Owner renames a movement
- **WHEN** the owner edits a movement and changes its notes
- **THEN** both legs SHALL be marked owner-edited and reviewed.

#### Scenario: Automatic pairing is not an owner edit
- **WHEN** notification processing links a new leg to its counterpart as a movement
- **THEN** neither leg SHALL get `owner_edited_at` or `classified_by = owner`.

### Requirement: Review Status Control
The API SHALL accept `review_status` of `reviewed` or `needs_review` on create and update and SHALL reject `auto` and any other value with 422. An update that sets only `review_status` SHALL change only that field, SHALL be allowed on movement legs, and SHALL NOT set `owner_edited_at` or change `classified_by`.

#### Scenario: Mark reviewed without other changes
- **GIVEN** an AI-classified row with `review_status = auto`
- **WHEN** the owner sends `PATCH /api/transactions/{id}` with only `review_status = reviewed`
- **THEN** the row SHALL have `review_status = reviewed` and still `classified_by = ai`
- **AND** `owner_edited_at`, amount, category, notes and merchant SHALL be unchanged.

#### Scenario: Automation-only status is rejected
- **WHEN** a client sends `review_status = auto`
- **THEN** the API SHALL respond with 422 and no row SHALL change.

#### Scenario: Movement leg marked reviewed
- **WHEN** the owner marks one leg of a notification-recorded movement as reviewed
- **THEN** that leg SHALL become reviewed without a 409 error.

### Requirement: Merchant Input Validation
The API SHALL accept an optional `merchant` on create and update, trim it and collapse inner whitespace, store an empty result as no merchant, and reject a value longer than 120 characters after normalising with 422.

#### Scenario: Merchant is normalised
- **WHEN** the owner saves merchant "  Kopi   Contoh  "
- **THEN** the stored merchant SHALL be "Kopi Contoh".

#### Scenario: Blank merchant clears the field
- **WHEN** the owner saves merchant "   "
- **THEN** the stored merchant SHALL be empty.

#### Scenario: Overlong merchant is rejected
- **WHEN** a client sends a 121-character merchant
- **THEN** the API SHALL respond with 422.

### Requirement: Provenance in Ledger Responses and Filters
Ledger list and detail responses SHALL include source, merchant, review status, classifier, confidence and owner-edit time. The ledger list SHALL accept `source` and `review_status` filters, reject unknown values with 422, and SHALL match the search text against merchant as well.

#### Scenario: Filtering rows that need review
- **GIVEN** three rows: auto, needs_review and reviewed
- **WHEN** the client lists with `review_status=needs_review`
- **THEN** only the needs-review row SHALL be returned.

#### Scenario: Filtering by source
- **WHEN** the client lists with `source=notification`
- **THEN** only notification-sourced rows SHALL be returned.

#### Scenario: Unknown filter value
- **WHEN** the client lists with `source=bot`
- **THEN** the API SHALL respond with 422.

#### Scenario: Searching by merchant
- **GIVEN** a row with merchant "Kopi Contoh" and unrelated notes
- **WHEN** the client searches for "kopi"
- **THEN** the row SHALL be returned.

### Requirement: Provenance in the Ledger Edit Dialog
The ledger edit dialog SHALL show the row's source, review status and classifier in Indonesian, SHALL let the owner edit the merchant, and SHALL offer a keyboard-reachable "Tandai sudah ditinjau" action when the row is not reviewed.

#### Scenario: Reviewing an automatic row
- **GIVEN** an auto row from a bank notification
- **WHEN** the owner opens it in the ledger
- **THEN** the dialog SHALL show "Notifikasi bank" and "Otomatis, belum ditinjau"
- **AND** activating "Tandai sudah ditinjau" SHALL send only `review_status = reviewed`.

#### Scenario: Editing the merchant
- **WHEN** the owner types a merchant and saves the dialog
- **THEN** the update SHALL include the merchant.
