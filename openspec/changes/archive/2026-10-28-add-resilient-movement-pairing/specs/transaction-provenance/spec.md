## ADDED Requirements

### Requirement: Provenance of Rows Created by Movement Pairing
A transfer-fee row split off at automatic pairing SHALL have provenance movement, auto, system. A leg created by completing a one-sided transfer SHALL have provenance movement, reviewed, system. A notification row recorded as a one-sided self-transfer awaiting its counterpart SHALL have `review_status = needs_review`.

#### Scenario: Transfer fee split off at automatic pairing
- **WHEN** a Rp 102.500 debit pairs automatically with a Rp 100.000 credit
- **THEN** the Rp 2.500 fee row SHALL have `source = movement`, `review_status = auto` and `classified_by = system`.

#### Scenario: Leg created by completing a one-sided transfer
- **WHEN** the owner completes an awaiting outgoing leg with a target account
- **THEN** the created income leg SHALL have `source = movement`, `review_status = reviewed` and `classified_by = system`.

#### Scenario: Notification row awaiting its counterpart
- **WHEN** a one-sided self-transfer notification is recorded in "Internal Movement" without a counterpart
- **THEN** the row SHALL have `source = notification` and `review_status = needs_review`.
