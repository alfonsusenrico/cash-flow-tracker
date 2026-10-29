## MODIFIED Requirements

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
