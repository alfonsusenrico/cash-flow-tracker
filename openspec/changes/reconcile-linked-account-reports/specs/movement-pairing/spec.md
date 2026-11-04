## ADDED Requirements

### Requirement: Linked Account Reports
An owner's active account link (account X, institution I) SHALL make a new observed leg the skipped second report of exactly one recorded, unlinked observed leg when they come from different institutions, have the same direction and amount, have post times at most 300 seconds apart, one leg is on X and the other leg, from I, is on X or names X (or, for a root X, an account under it) as its remote. The second report SHALL book nothing and ask nothing.

#### Scenario: Linked GoPay Tabungan
- **GIVEN** the link "Gopay juga dilaporkan Bank Jago" and a GoPay income of Rp 500.000 on Gopay recorded from "Anda telah menerima Tabungan by Jago sebanyak Rp500.000."
- **WHEN** Jago's "You've received Rp500.000 from GoPay." arrives 0.2 seconds later on Jago · Kantong Utama with remote Gopay
- **THEN** no transaction SHALL be created for it, its event SHALL be a `duplicate_observation` of the GoPay leg with the link and `reviewed_at` set, no Tinjau item SHALL wait for it, and Kantong Utama's balance SHALL be unchanged
- **AND** a myBCA "You spent IDR 500,000.00 at Payment." 3 seconds later SHALL pair with the GoPay income as one movement

#### Scenario: Late report
- **GIVEN** the same link and the GoPay income
- **WHEN** Jago's report is posted 4 minutes after GoPay's
- **THEN** it SHALL be skipped the same way

#### Scenario: Beyond the window
- **WHEN** Jago's report is posted 6 minutes after GoPay's
- **THEN** it SHALL be recorded as a transaction and flagged `needs_review` with `ambiguous_movement`, and nothing SHALL be skipped

#### Scenario: A real Jago income
- **GIVEN** the same link
- **WHEN** a Jago income of Rp 500.000 "from GoPay" arrives and no GoPay income of Rp 500.000 on Gopay is recorded within 300 seconds
- **THEN** it SHALL be recorded as a transaction by the normal rules

#### Scenario: Possible transfer
- **GIVEN** the link, a GoPay income of Rp 500.000 on Gopay and a GoPay expense of Rp 500.000 on Gopay recorded within the pairing window
- **WHEN** a Jago income of Rp 500.000 with remote Gopay arrives
- **THEN** nothing SHALL be skipped, and the Jago leg SHALL be recorded and decided by the normal pairing rules, so a real GoPay → Jago transfer pairs as one movement

#### Scenario: Two top-ups of the same amount
- **GIVEN** the link and two GoPay incomes of Rp 500.000 on Gopay 2 minutes apart, the first already holding Jago's skipped report
- **WHEN** Jago's second report arrives
- **THEN** it SHALL be skipped as the second report of the second GoPay income only

#### Scenario: Opposite direction is a transfer
- **WHEN** a GoPay expense of Rp 500.000 on Gopay and a Jago income of Rp 500.000 with remote Gopay arrive 2 seconds apart under the link
- **THEN** the link SHALL NOT apply, and the two legs SHALL pair as one movement

#### Scenario: Unlinked account
- **GIVEN** no account link
- **WHEN** the 1 November sequence arrives
- **THEN** the one-arrival guard SHALL make the Jago report a duplicate observation waiting in "Terlihat dua kali", as before

### Requirement: Second Report Recorded First
When the report on the linked account arrives after a recorded second report under an active link, the new report SHALL keep the money and the earlier leg SHALL be removed in the same database transaction and become its skipped second report, but only while that leg is automatic, unlinked, unanswered and not edited by the owner; otherwise the new report SHALL wait in "Terlihat dua kali". The deferred re-pairing pass SHALL apply the same rule, and repeated passes SHALL change nothing further.

#### Scenario: Jago first, then GoPay
- **GIVEN** the link "Gopay juga dilaporkan Bank Jago" and Jago's "You've received Rp500.000 from GoPay." recorded on Jago · Kantong Utama
- **WHEN** GoPay's "Anda telah menerima Tabungan by Jago sebanyak Rp500.000." arrives 1 second later on Gopay
- **THEN** the GoPay leg SHALL be kept, the Jago transaction SHALL be removed with its balance effect, and the Jago event SHALL become the skipped second report of the GoPay leg

#### Scenario: Owner already edited the Jago leg
- **WHEN** the owner edited the Jago leg before GoPay's report arrives
- **THEN** the Jago leg SHALL stay, and the GoPay event SHALL become a duplicate observation waiting in "Terlihat dua kali"

### Requirement: Learning an Account Link
"Benar, satu transaksi" on a duplicate observation whose kept leg came from another app's notification SHALL create the active link of the named account and the other report's institution for the owner unless it exists, and SHALL say so with "Batalkan", which stops it. The named account SHALL be the kept leg's account unless that leg was a default-pocket fallback and the duplicate named its own account. Learning SHALL be idempotent and SHALL use only that owner's answers.

#### Scenario: First confirmation
- **GIVEN** no link and Jago's report waiting in "Terlihat dua kali" as the duplicate of a GoPay income on Gopay
- **WHEN** the owner chooses "Benar, satu transaksi"
- **THEN** the link "Gopay juga dilaporkan Bank Jago" SHALL be created with source `confirmation`, and Tinjau SHALL show "Gopay juga dilaporkan Bank Jago. Laporan serupa dilewati otomatis." with "Batalkan"

#### Scenario: Confirming again
- **WHEN** the owner confirms a second Jago duplicate while that link is active
- **THEN** no second link SHALL be created

#### Scenario: Kept leg not from a notification
- **WHEN** the kept leg of a confirmed duplicate was typed or entered by hand
- **THEN** no link SHALL be created

### Requirement: Account Link Management
`GET /api/ingest/account-links` SHALL list the owner's active links, optionally for one account, with the account and app labels, the source and how many reports each skipped; `DELETE /api/ingest/account-links/{id}` SHALL stop one. Both SHALL require a web session. The Rekening detail SHALL show "Juga dilaporkan" with each linked app and "Lepaskan" behind a two-step confirmation. Links SHALL NOT be added by hand.

#### Scenario: Stopping a link
- **WHEN** the owner chooses "Lepaskan" on "Bank Jago" in Gopay's detail and confirms
- **THEN** the link SHALL stop matching from that moment, reports it skipped earlier SHALL stay skipped, and the next mirror SHALL wait in "Terlihat dua kali"

#### Scenario: Another owner's link
- **WHEN** another owner requests `DELETE /api/ingest/account-links/{id}` for that link
- **THEN** the response SHALL be 404 `account_link_not_found` and the link SHALL stay active

#### Scenario: Companion key
- **WHEN** the companion's API key requests `GET /api/ingest/account-links`
- **THEN** the response SHALL be 403 `session_required`

### Requirement: Skip Patterns Converted
Migration V47 SHALL convert each active skip pattern into the link of the single account its kept app's notification names resolve to and the pattern's redundant app, SHALL point the events that pattern skipped at that link, and SHALL stop every active pattern. A pattern whose kept app resolves to no single account SHALL be stopped without a link.

#### Scenario: Known pattern
- **GIVEN** the active pattern "Bank Jago, uang masuk, dipakai GoPay" and GoPay's notification names resolving only to Gopay
- **WHEN** V47 runs
- **THEN** the link "Gopay juga dilaporkan Bank Jago" SHALL exist with source `converted`, the pattern SHALL be stopped, and its skipped events SHALL name the link

#### Scenario: Unresolved pattern
- **WHEN** a pattern's kept app resolves to no account or to several
- **THEN** the pattern SHALL be stopped, no link SHALL be created, and its next mirror SHALL wait in "Terlihat dua kali"

## MODIFIED Requirements

### Requirement: One Arrival Seen by Two Apps
When no account link decides (Linked Account Reports), a new observed leg SHALL be recorded as a duplicate observation instead of a transaction when exactly one recorded observed leg of the owner has the same direction and amount, comes from another institution's app, has a post time at most 120 seconds away, is on a different account, and names the new leg's account (or its root) as its remote while the new leg names that leg's account (or its root). Such a leg SHALL NOT pair with, or count as a candidate for, any other leg.

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

## REMOVED Requirements

### Requirement: Redundant Notification Patterns
**Reason**: Replaced by Linked Account Reports. Patterns mapped app pairs by hand, and the owner chose to convert and retire them.
**Migration**: V47 converts each active pattern into an account link where it resolves and stops every pattern.

### Requirement: Kept Report Arrives After the Skipped One
**Reason**: Replaced by Second Report Recorded First, which applies the same guarded swap to account links.
**Migration**: None. The swap rules are unchanged.

### Requirement: Learning a Skip Pattern
**Reason**: Replaced by Learning an Account Link.
**Migration**: Existing patterns are converted by V47.
