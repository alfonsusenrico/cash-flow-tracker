## MODIFIED Requirements

### Requirement: Pure Transaction-Level Kakeibo Aggregation
The system SHALL resolve every expense's effective Kakeibo pillar from the transaction's own pillar, otherwise its category's pillar, otherwise no pillar ("Belum diklasifikasi", see `kakeibo-classification-completeness`). It SHALL NOT default a missing pillar to `need`. Ledger listing, pillar filtering, transaction detail and report aggregation SHALL use this shared rule. Category pillars SHALL remain optional.

#### Scenario: Aggregating mixed transactions under the same category
- **WHEN** a category contains a need expense of Rp 30.000 and a want expense of Rp 70.000
- **THEN** those amounts SHALL be counted under their respective effective pillars before applicable reimbursement offsets

#### Scenario: Transaction without its own pillar follows its category
- **WHEN** an expense has no transaction pillar but its category is `want`
- **THEN** the ledger and Kakeibo reports SHALL use `want`

#### Scenario: No pillar anywhere defaults to need
- **WHEN** an expense has no transaction or category pillar
- **THEN** it SHALL no longer default to `need`: its effective pillar SHALL be `null` and reports SHALL count it as "Belum diklasifikasi" (`unclassified`)
