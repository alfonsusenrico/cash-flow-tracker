## MODIFIED Requirements

### Requirement: One Arrival Seen by Two Apps
Without a deciding account link (Linked Account Reports), a new observed leg SHALL be a duplicate observation, not a transaction, when exactly one recorded observed leg of the owner, paired or not, has the same direction and amount, comes from another app, is at most 120 seconds away, is on another account, has absorbed no report from the new leg's app, and each names the other's account (or its root) as remote. It SHALL NOT pair with or be a candidate for any other leg.

#### Scenario: Top-up seen by GoPay and Jago
- **GIVEN** a GoPay income of Rp 500.000 on GoPay recorded from "Anda telah menerima Tabungan by Jago sebanyak Rp500.000.", whose remote is Jago · Kantong Utama
- **WHEN** Jago's "You've received Rp500.000 from GoPay." arrives 0.2 seconds later, resolving to Jago · Kantong Utama with remote GoPay
- **THEN** no transaction SHALL be created for it, its event SHALL be recorded with provenance `duplicate_observation` pointing at the GoPay leg, and Kantong Utama's balance SHALL be unchanged
- **AND** a myBCA "You spent IDR 500,000.00 at Payment." 3 seconds later SHALL see the GoPay income as its only compatible counterpart

#### Scenario: Second report after the first one paired
- **GIVEN** the GoPay income of Rp 500.000 already paired with a myBCA "You spent IDR 500,000.00 at Payment." into one movement BCA → GoPay
- **WHEN** Jago's "You've received Rp500.000 from GoPay." arrives 1 second after the GoPay report, with no account link
- **THEN** its event SHALL be a `duplicate_observation` of the GoPay leg waiting under "Terlihat dua kali", no transaction SHALL be created, the movement SHALL be unchanged and Kantong Utama's balance SHALL be unchanged

#### Scenario: A paired leg is never replaced
- **GIVEN** a Jago income on the default pocket Kantong Utama with remote GoPay, already paired into a movement
- **WHEN** GoPay's "Anda telah menerima Tabungan by Jago sebanyak Rp500.000." arrives 1 second later naming GoPay
- **THEN** the Jago leg and its movement SHALL be kept and the GoPay event SHALL be the `duplicate_observation`

#### Scenario: Possible transfer after the first report
- **GIVEN** a GoPay income of Rp 500.000 recorded from "Anda telah menerima Tabungan by Jago sebanyak Rp500.000." and, 60 seconds later, an unpaired GoPay expense of Rp 500.000 from "Rp500.000 udah dikirim ke Bank Jago Raka Purnama."
- **WHEN** Jago's "You've received Rp500.000 from GoPay." arrives 1 second after the expense
- **THEN** the guard SHALL NOT hold it, and the normal pairing SHALL decide, linking it with the GoPay expense as one movement

#### Scenario: Kept leg is not the default-pocket fallback
- **GIVEN** Jago's "You've received Rp500.000 from GoPay." was recorded first, on the default pocket Jago · Kantong Utama with remote GoPay
- **WHEN** GoPay's "Anda telah menerima Tabungan by Jago sebanyak Rp500.000." arrives 1 second later on GoPay with remote Jago · Kantong Utama
- **THEN** the GoPay leg SHALL be kept, the Jago leg's transaction SHALL be removed in the same database transaction with its balance effect, and the Jago event SHALL become the `duplicate_observation`
- **AND** the removal SHALL happen only while the Jago leg is automatic, unlinked, not in a movement and not edited by the owner; otherwise the GoPay event SHALL be the duplicate

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
- **WHEN** the deferred re-pairing pass meets an unlinked automatic leg that the guard would have marked a duplicate, including one whose original is already in a movement
- **THEN** it SHALL apply the same rule, dropping only the unpaired leg, and repeated passes SHALL change nothing further

#### Scenario: A kept leg absorbs one report per app
- **GIVEN** a GoPay income of Rp 500.000 paired with a myBCA debit into BCA → GoPay, whose Jago repeat is already a duplicate observation of it
- **WHEN** a second top-up of Rp 500.000 arrives 98 seconds later and its Jago "You've received Rp500.000 from GoPay." comes before its GoPay report
- **THEN** the Jago report SHALL NOT be a duplicate observation of the first GoPay income; it SHALL be decided against the second top-up's GoPay report, which keeps it out of the ledger, and the first movement SHALL be unchanged
