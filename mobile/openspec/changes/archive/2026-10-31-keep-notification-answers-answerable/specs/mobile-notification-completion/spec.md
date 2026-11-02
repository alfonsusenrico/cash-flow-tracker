## ADDED Requirements

### Requirement: Refused account confirmations from the notification stay answerable

When an account-proposal confirmation sent from its notification does not go through, because the server refuses it, the network is unavailable, the result is invalid, or the question changed, the companion SHALL show the event's open proposal again in the same slot with its actions and a statement that the confirmation was not saved. When the event no longer awaits that confirmation, it SHALL show only the message.

#### Scenario: Server unavailable for a confirmation
- **WHEN** the owner taps the proposed account in the notification and the server answers 503
- **THEN** the event stays awaiting confirmation and the notification still offers the proposed account and "Pilih lain"

#### Scenario: Confirmation while offline
- **WHEN** the owner confirms from the notification without a connection
- **THEN** the notification still offers the confirmation and says it was not saved, and a later lookup leaves it answerable

#### Scenario: Confirmation for a changed question
- **WHEN** a confirmation for an earlier question of the event arrives
- **THEN** nothing is sent and the event's open proposal is offered with its actions

### Requirement: Account confirmation attempts are traced privately

The companion SHALL record each step of every account confirmation attempt from the notification or the app with the same private, bounded reason-code record that sender answers use, with the answer kind `account`. Tanya SHALL state that a confirmation from the notification was not saved while the newest settled attempt for the shown proposal is a refused notification confirmation.

#### Scenario: Refused confirmation is traced
- **WHEN** a confirmation from the notification receives a 503
- **THEN** the record holds a received step and a `server_refused` step for that event's question

#### Scenario: Tanya shows an unsaved confirmation
- **WHEN** the owner opens Tanya on an account proposal whose newest settled attempt is a refused notification confirmation
- **THEN** the card says the confirmation from the notification was not saved and keeps the account choice available
