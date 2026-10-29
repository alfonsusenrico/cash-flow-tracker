# movement-pairing Specification

## Purpose
Link two recorded legs of one own transfer into a single movement automatically, tolerating known
transfer fees and late or hand-entered legs, while never linking unrelated rows and never breaking the
rule that a movement is two linked legs of equal amount.

## Requirements

### Requirement: Pairing Candidates
Automatic pairing SHALL consider only unlinked expense or income rows on active liquid accounts without goal, debt, recurring or fee links, that are either recorded from a notification and not reclassified by the owner, or filed in "Internal Movement" (including manual rows). Rows the owner split out of a movement SHALL never be candidates.

#### Scenario: Manual Internal Movement row
- **GIVEN** the owner entered a manual BCA expense of Rp 1.502.500 in "Internal Movement"
- **WHEN** a ShopeePay notification "RAKA PURNAMA mengirimkan dana sebesar Rp1.500.000 ke ShopeePay-mu" is recorded 40 minutes later
- **THEN** the re-pairing pass SHALL link the two rows as one movement.

#### Scenario: Manual row in an ordinary category
- **GIVEN** a manual GoPay expense of Rp 1.500.000 in an ordinary spending category
- **WHEN** pairing runs
- **THEN** that row SHALL NOT be linked or changed.

#### Scenario: Notification row the owner reclassified
- **GIVEN** the owner changed the category of a notification expense
- **WHEN** a matching inbound leg is recorded later
- **THEN** the reclassified row SHALL NOT be linked automatically.

### Requirement: Transfer Fee Tolerance
An outbound leg SHALL pair with an inbound leg whose amount is lower by a configured known fee (default Rp 2.500, Rp 6.500, Rp 1.000 and Rp 2.900), never by more than the configured maximum difference (default Rp 10.000), and only with strong evidence. Any other difference SHALL NOT pair.

#### Scenario: Each known fee
- **GIVEN** a GoPay notification "Rp102.500 udah dikirim ke BCA Raka Purnama." from the owner Raka Purnama
- **WHEN** a BCA notification of Rp 100.000 from "RA** PUR**MA" is recorded a minute later
- **THEN** the two legs SHALL be linked as one movement, and the same SHALL hold for differences of Rp 6.500, Rp 1.000 and Rp 2.900.

#### Scenario: Unknown difference
- **WHEN** the outbound leg is Rp 112.500 and the inbound leg Rp 100.000
- **THEN** the legs SHALL stay unlinked.

#### Scenario: Fee above the maximum
- **GIVEN** the fee table contains Rp 15.000 while the maximum difference is Rp 10.000
- **WHEN** the settings are loaded
- **THEN** the configuration SHALL be invalid, fee tolerance and re-pairing SHALL be disabled, and no fee SHALL be tolerated.

#### Scenario: Fee with weak evidence
- **WHEN** a myBCA "You spent IDR 51,000.00 at Shopping." and a ShopeePay top-up of Rp 50.000 are recorded 30 seconds apart
- **THEN** the legs SHALL stay unlinked.

### Requirement: Fee Recorded as a Separate Expense
When legs pair with a fee, the movement SHALL be created for the inbound amount, both legs SHALL be equal, and the fee SHALL be recorded as an expense on the source account, dated as the outbound leg, in the owner's "Biaya Admin & Transfer" category (pillar need, counted in budgets), with source `movement`, classifier `system`, and a link to the movement.

#### Scenario: Balances after a fee pair
- **WHEN** a Rp 102.500 debit pairs with a Rp 100.000 credit
- **THEN** both legs SHALL be Rp 100.000, a Rp 2.500 fee expense SHALL exist on the source account linked to the movement
- **AND** the source account's total debits SHALL still be Rp 102.500.

#### Scenario: Fee category on first use
- **GIVEN** the owner has no "Biaya Admin & Transfer" category
- **WHEN** the first fee is recorded
- **THEN** the category SHALL be created as an expense category with pillar need, not excluded from budgets.

#### Scenario: Fee row cannot be merged
- **WHEN** the owner asks to merge a fee row with another transaction as a movement
- **THEN** the request SHALL be rejected with 409 and nothing SHALL change.

### Requirement: Fee Undone With Its Movement
Splitting a movement SHALL delete its fee row and return the fee to the outbound leg; deleting a movement SHALL delete its fee row.

#### Scenario: Split a fee pair
- **WHEN** the owner splits a movement created with a Rp 2.500 fee
- **THEN** the outbound leg SHALL be Rp 102.500 again, the fee row SHALL be gone, and both legs SHALL be unlinked.

#### Scenario: Delete a fee pair
- **WHEN** the owner deletes that movement
- **THEN** both legs and the fee row SHALL be deleted.

### Requirement: Evidence Strength and Uniqueness
A pair SHALL be linked only when each leg is the other's single compatible match. A link supported only by the legs being on different institutions without a third party, with neither leg naming the owner or the other leg's institution, is weak: it SHALL need an exact amount, the immediate pairing window, and no other candidate of that amount in the window.

#### Scenario: Unique weak pair
- **WHEN** a myBCA "You spent IDR 7,490,557.00 at Shopping." and a ShopeePay top-up of Rp 7.490.557 are the only candidates of that amount
- **THEN** they SHALL be linked as one movement.

#### Scenario: Weak pair with a repeated amount
- **GIVEN** a GoPay payment of Rp 50.000 to a third party
- **WHEN** a ShopeePay top-up of Rp 50.000 and a myBCA "You spent IDR 50,000.00 at Shopping." follow within a minute
- **THEN** nothing SHALL be linked, and the top-up and the debit SHALL be flagged `needs_review` with the reason `ambiguous_movement`
- **AND** the third-party payment SHALL keep its review status.

#### Scenario: Strong pair with a repeated amount
- **GIVEN** an unrelated Jago payment of Rp 100.000 to a merchant
- **WHEN** a GoPay transfer of Rp 100.000 to the owner's BCA and the BCA credit from the owner follow
- **THEN** the transfer legs SHALL be linked.

#### Scenario: Two compatible counterparts
- **WHEN** a new leg has two compatible counterparts
- **THEN** nothing SHALL be linked and every automatically classified leg involved SHALL be flagged `needs_review` with `ambiguous_movement`.

### Requirement: Deferred Re-pairing
A background pass SHALL run at a configured interval (default 10 minutes) over unlinked candidate legs recorded within a configured lookback (default 48 hours) and link them with the same rules, allowing strongly evidenced legs up to a configured gap (default 6 hours) apart. It SHALL be idempotent, link each leg at most once under concurrent runs, and log only counts.

#### Scenario: Late leg
- **WHEN** the BCA credit of a GoPay transfer to the owner's BCA is recorded 40 minutes after the GoPay debit
- **THEN** it SHALL be recorded unpaired, and the next pass SHALL link both legs, show both notification results as one movement with one record key, and set both legs to `auto`.

#### Scenario: Weak pair outside the immediate window
- **WHEN** a myBCA Shopping debit and a ShopeePay top-up of the same amount are 40 minutes apart
- **THEN** the pass SHALL NOT link them.

#### Scenario: Concurrent passes
- **WHEN** two passes run at the same time over one linkable pair
- **THEN** exactly one movement and at most one fee row SHALL exist.

#### Scenario: Repeated pass
- **WHEN** the pass runs again with nothing new
- **THEN** it SHALL link nothing and change no row.

#### Scenario: Legs older than the lookback
- **WHEN** both legs were recorded 49 hours ago
- **THEN** the pass SHALL NOT link them.

#### Scenario: Existing movements and reports
- **WHEN** the pass runs over an owner with existing movements and no linkable legs
- **THEN** no transaction SHALL change and the overview and pulse figures SHALL be identical.

### Requirement: Owner Splits Are Respected
Splitting a movement SHALL record the split on both legs, and automatic pairing SHALL never link either leg again; the owner MAY still merge them manually.

#### Scenario: Split pair and the re-pairing pass
- **GIVEN** the owner split an automatically paired movement
- **WHEN** the re-pairing pass runs
- **THEN** both legs SHALL stay unlinked.

### Requirement: Complete a One-Sided Transfer
`POST /api/movements/complete` with `transaction_id` and exactly one of `target_account_id` (outgoing leg) or `source_account_id` (incoming leg) SHALL complete the transfer on the named account and mark both legs reviewed. It SHALL link an existing matching leg instead of creating a second one, and SHALL create a new leg only when none matches or the owner sends `create_new: true`. Only the owner's unlinked "Internal Movement" rows SHALL be completable.

#### Scenario: Complete an outgoing leg
- **GIVEN** an unpaired GoPay "Rp500.000 udah dikirim ke BCA Raka Purnama." awaiting its counterpart
- **WHEN** the owner completes it with the BCA account as target
- **THEN** a Rp 500.000 BCA income with source `movement` SHALL be created and linked, both legs SHALL be `reviewed`, and the notification result SHALL show one movement.

#### Scenario: Complete an incoming leg
- **WHEN** the owner completes an awaiting Jago income of Rp 125.000 with the BCA account as source
- **THEN** a Rp 125.000 BCA expense SHALL be created and linked.

#### Scenario: Repeated completion
- **WHEN** the same completion is sent again
- **THEN** the response SHALL be 200 with the same movement and `idempotent = true`, and no row SHALL be created.

#### Scenario: Already linked to another account
- **WHEN** a completed leg is completed again with a different account
- **THEN** the response SHALL be 409 `already_linked`.

#### Scenario: Invalid completion requests
- **WHEN** the body names neither or both accounts, the side does not match the leg's direction, the transaction or account belongs to no such owner, the account is the leg's own account, an investment position, or a parent account with pockets, or the row is not an unlinked "Internal Movement" row
- **THEN** the response SHALL be 422 (body, side, same account, investment, pockets, not completable) or 404 (unknown or foreign transaction or account), and nothing SHALL change.

#### Scenario: Existing Internal Movement leg is linked
- **GIVEN** an awaiting GoPay send of Rp 500.000 and an unlinked BCA "Internal Movement" income of Rp 500.000 one day later that the owner never split
- **WHEN** the owner completes the send with the BCA account as target
- **THEN** the two rows SHALL be linked as one movement, no transaction SHALL be created, and the response SHALL carry `linked_transaction_id` with `created_transaction_id = null`
- **AND** repeating the request SHALL return `idempotent = true` without a new row.

#### Scenario: Other matches need the owner's choice
- **GIVEN** the BCA account holds an unlinked income of Rp 500.000 within 3 days in an ordinary category, or two such "Internal Movement" incomes
- **WHEN** the owner completes the send with the BCA account as target
- **THEN** the response SHALL be 409 `counterpart_candidates` listing the matching rows, and nothing SHALL change
- **AND** the same request with `create_new: true` SHALL create the new leg.

#### Scenario: Rows outside the window or amount do not block
- **WHEN** the only BCA income of Rp 500.000 is 5 days away, or the only nearby one is Rp 499.000
- **THEN** completion SHALL create the new leg as before.

### Requirement: Pairing Settings
The fee table, maximum difference, re-pairing interval, lookback and gap SHALL be configurable; an invalid value SHALL disable fee tolerance and the re-pairing pass with the code `movement_pairing_configuration_invalid` instead of applying a silent default, while exact-amount pairing continues.

#### Scenario: Valid custom fee table
- **WHEN** `MOVEMENT_TRANSFER_FEES` is `1000, 2500`
- **THEN** only those two fees SHALL be tolerated.

#### Scenario: Invalid interval
- **WHEN** `MOVEMENT_REPAIR_INTERVAL_SECONDS` is `5`
- **THEN** the configuration error SHALL be `movement_pairing_configuration_invalid` and no fee SHALL be tolerated.

### Requirement: Late Leg Confirms a Completed Transfer
A notification for a leg the owner already completed SHALL be recorded as that movement's role confirmation instead of a second transaction. The leg matches when it has the same account, direction and amount, falls within the pairing window, and has no notification of its own. With more than one such leg, the event SHALL be held as `ambiguous_existing_movement`.

#### Scenario: Late Jago leg after completion
- **GIVEN** a BCA income of Rp 500.000 from the owner, completed on Tinjau with Main Pocket as source
- **WHEN** Jago's "You've transferred Rp500.000 to RAKA PURNAMA." arrives 2 seconds or 10 minutes later and its pocket is confirmed as Main Pocket
- **THEN** no transaction SHALL be created
- **AND** the event SHALL be recorded as `role_confirmation` with role `outbound` on the completed movement and its created leg.

#### Scenario: Two completed transfers could match
- **GIVEN** two completed Rp 500.000 transfers from Main Pocket within the pairing window
- **WHEN** one late Jago leg of Rp 500.000 arrives
- **THEN** the event SHALL be held with `ambiguous_existing_movement` and no transaction SHALL be created.
