## ADDED Requirements

### Requirement: One Arrival Seen by Two Apps
A new observed leg SHALL be recorded as a duplicate observation instead of a transaction when exactly one recorded observed leg of the owner has the same direction and amount, comes from another institution's app, has a post time at most 120 seconds away, is on a different account, and names the new leg's account (or its root) as its remote while the new leg names that leg's account (or its root). Such a leg SHALL NOT pair with, or count as a candidate for, any other leg.

#### Scenario: Top-up seen by GoPay and Jago
- **GIVEN** a GoPay income of Rp 500.000 on GoPay recorded from "Anda telah menerima Tabungan by Jago sebanyak Rp500.000.", whose remote is Jago · Kantong Utama
- **WHEN** Jago's "You've received Rp500.000 from GoPay." arrives 0.2 seconds later, resolving to Jago · Kantong Utama with remote GoPay
- **THEN** no transaction SHALL be created for it, its event SHALL be recorded with provenance `duplicate_observation` pointing at the GoPay leg, and Kantong Utama's balance SHALL be unchanged
- **AND** a myBCA "You spent IDR 500,000.00 at Payment." 3 seconds later SHALL see the GoPay income as its only compatible counterpart

#### Scenario: Kept leg is not the default-pocket fallback
- **GIVEN** Jago's "You've received Rp500.000 from GoPay." was recorded first, on the default pocket Jago · Kantong Utama with remote GoPay
- **WHEN** GoPay's "Anda telah menerima Tabungan by Jago sebanyak Rp500.000." arrives 1 second later on GoPay with remote Jago · Kantong Utama
- **THEN** the GoPay leg SHALL be kept, the Jago leg's transaction SHALL be removed in the same database transaction with its balance effect, and the Jago event SHALL become the `duplicate_observation`
- **AND** the removal SHALL happen only while the Jago leg is automatic, unlinked and not edited by the owner; otherwise the GoPay event SHALL be the duplicate

#### Scenario: Two real transfers of the same amount
- **WHEN** a GoPay income of Rp 500.000 and a Jago income of Rp 500.000 arrive 30 seconds apart and neither names the other's account as its remote
- **THEN** both SHALL be recorded as transactions

#### Scenario: Same app twice
- **WHEN** two GoPay incomes of Rp 500.000 that name each other's accounts arrive 5 seconds apart from the same app
- **THEN** the guard SHALL NOT apply and both SHALL follow the normal rules

#### Scenario: Outside the window
- **WHEN** the mirrored income arrives 121 seconds after the first
- **THEN** it SHALL be recorded as a transaction and flagged `needs_review` with `ambiguous_movement`

#### Scenario: Two possible originals
- **WHEN** two recorded legs match the guard for a new leg
- **THEN** the new leg SHALL be recorded as a transaction and flagged `needs_review` with `ambiguous_movement`, and no event SHALL become a duplicate observation

#### Scenario: Deferred pass
- **WHEN** the deferred re-pairing pass meets an unlinked automatic leg that the guard would have marked a duplicate
- **THEN** it SHALL apply the same rule, and repeated passes SHALL change nothing further

### Requirement: Linking Retires Automatic Memory
When a leg becomes part of a movement by automatic pairing, the owner's merge or the owner's completion, an automatic (`ai`) memory entry for that leg's match key whose category is not an internal-movement category SHALL be deleted in the same database transaction. Owner memory entries SHALL NOT be changed.

#### Scenario: GoPay credit linked by the owner
- **GIVEN** an `ai` memory entry "merchant:tabungan by jago" in "Pendapatan Lain"
- **WHEN** the owner links that GoPay income with a BCA debit as one movement
- **THEN** the entry SHALL be deleted, and the next GoPay "Tabungan by Jago" credit SHALL NOT be classified from it

#### Scenario: Owner entry kept
- **GIVEN** an owner memory entry for the leg's match key
- **WHEN** the leg is linked into a movement
- **THEN** the entry SHALL be unchanged
