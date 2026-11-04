## ADDED Requirements

### Requirement: Redundant Notification Patterns
An owner's active skip pattern (redundant app, direction, kept app) SHALL make a new observed leg from the redundant app a skipped duplicate observation of exactly one recorded, unlinked observed leg from the kept app with the same direction and amount, on another account, at most 120 seconds away, when the new leg's remote resolves to that leg's account or its root. A skipped leg SHALL book nothing and ask nothing.

#### Scenario: Known pattern skips the Jago report
- **GIVEN** the pattern "Bank Jago, uang masuk, dipakai GoPay" and a GoPay income of Rp 500.000 recorded from "Anda telah menerima Tabungan by Jago sebanyak Rp500.000."
- **WHEN** Jago's "You've received Rp500.000 from GoPay." arrives 0.2 seconds later, resolving to Jago · Kantong Utama with remote GoPay
- **THEN** no transaction SHALL be created, the event SHALL be a `duplicate_observation` of the GoPay leg with the pattern recorded and `reviewed_at` set, no Tinjau item SHALL wait for it, and Kantong Utama's balance SHALL be unchanged
- **AND** a myBCA "You spent IDR 500,000.00 at Payment." 3 seconds later SHALL pair with the GoPay income

#### Scenario: No report from the kept app
- **GIVEN** the same pattern
- **WHEN** Jago's "You've received Rp500.000 from GoPay." arrives and no GoPay income of Rp 500.000 is recorded within 120 seconds
- **THEN** the Jago report SHALL be recorded as a transaction by the normal rules

#### Scenario: Different amount or direction
- **WHEN** a Jago income of Rp 450.000, or a Jago expense of Rp 500.000, arrives 1 second after a GoPay income of Rp 500.000 under that pattern
- **THEN** the pattern SHALL NOT apply

#### Scenario: Remote not resolved
- **WHEN** a Jago income of Rp 500.000 whose counterparty resolves to no owned account arrives 1 second after a GoPay income of Rp 500.000 under that pattern
- **THEN** the pattern SHALL NOT apply, and the one-arrival guard and the normal rules SHALL decide

#### Scenario: Two possible kept reports
- **WHEN** two recorded GoPay incomes of Rp 500.000 match the pattern for one Jago report
- **THEN** the Jago report SHALL be recorded as a transaction flagged `needs_review` with `ambiguous_movement`, and nothing SHALL be skipped

#### Scenario: Stopped pattern
- **WHEN** the owner has stopped the pattern and the same Jago report arrives
- **THEN** only the one-arrival guard SHALL apply, and a duplicate SHALL wait in "Terlihat dua kali"

#### Scenario: Without a pattern
- **GIVEN** no skip pattern
- **WHEN** the incident sequence arrives
- **THEN** the one-arrival guard SHALL make the Jago report a duplicate observation that waits in "Terlihat dua kali", as before

### Requirement: Kept Report Arrives After the Skipped One
When the kept app's report arrives after a recorded report that an active pattern names as redundant, the kept report SHALL be recorded and the redundant leg SHALL be removed in the same database transaction and become its skipped duplicate, but only while that leg is automatic, unlinked, unanswered and not edited by the owner.

#### Scenario: Jago first, then GoPay
- **GIVEN** the pattern "Bank Jago, uang masuk, dipakai GoPay" and Jago's "You've received Rp500.000 from GoPay." recorded on Jago · Kantong Utama
- **WHEN** GoPay's "Anda telah menerima Tabungan by Jago sebanyak Rp500.000." arrives 1 second later
- **THEN** the GoPay leg SHALL be kept, the Jago transaction SHALL be removed with its balance effect, and the Jago event SHALL become the skipped duplicate of the GoPay leg

#### Scenario: Owner already edited the Jago leg
- **WHEN** the owner edited the Jago leg before GoPay's report arrives
- **THEN** the Jago leg SHALL stay, and the GoPay event SHALL become an ordinary duplicate observation waiting in "Terlihat dua kali"

#### Scenario: Deferred pass
- **WHEN** the deferred re-pairing pass meets legs that the pattern would have decided
- **THEN** it SHALL apply the same rule, and repeated passes SHALL change nothing further

### Requirement: Learning a Skip Pattern
"Benar, satu transaksi" on a duplicate observation whose kept leg came from another app's notification SHALL create the active pattern (the duplicate's app, its direction, the kept leg's app) for the owner unless it exists, and SHALL say so with a "Batalkan" action that stops it. Learning SHALL be idempotent.

#### Scenario: First confirmation
- **GIVEN** no pattern and a Jago report waiting in "Terlihat dua kali" as the duplicate of a GoPay income
- **WHEN** the owner chooses "Benar, satu transaksi"
- **THEN** the pattern "Bank Jago, uang masuk, dipakai GoPay" SHALL be created with source `confirmation`, and Tinjau SHALL show "Laporan Bank Jago serupa akan dilewati otomatis." with "Batalkan"

#### Scenario: Confirming again
- **WHEN** the owner confirms a second Jago duplicate while that pattern is active
- **THEN** no second pattern SHALL be created

#### Scenario: Kept leg not from a notification
- **WHEN** the kept leg of a confirmed duplicate was typed or entered by hand
- **THEN** no pattern SHALL be created
