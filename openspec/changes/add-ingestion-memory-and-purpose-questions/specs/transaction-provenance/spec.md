## MODIFIED Requirements

### Requirement: Provenance Written by Every Recording Path
Each path that creates a transaction SHALL write its provenance explicitly, by this table: manual create → manual, reviewed, owner; movement → movement, reviewed, system; recurring execution → recurring, reviewed, system; payroll → payroll, reviewed, system; reconcile → reconcile, reviewed, system; investment trade or top-up → investment, reviewed, system; notification → notification, auto or needs_review, ai or rule. A notification row SHALL be `needs_review` exactly when the record-and-flag rules give it at least one review reason; a merchant-memory hit SHALL be classified by `rule`.

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
