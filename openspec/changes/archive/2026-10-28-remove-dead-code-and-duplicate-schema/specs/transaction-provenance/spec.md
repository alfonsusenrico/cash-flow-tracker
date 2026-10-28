## MODIFIED Requirements

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
- **WHEN** the API restarts or the migrations are applied again
- **THEN** no provenance value SHALL change.
