## MODIFIED Requirements

### Requirement: Restricted reply handling and safe upgrade

Reply actions SHALL be restricted to the companion's receiver and require device unlock before accepting a financial confirmation on the lock screen. Replies SHALL NOT be exposed through logs or backup exports. An app upgrade SHALL preserve pairing, existing captured events, completion and alert receipts, and unrelated local data, while adding durable answer storage. The question controls in Tanya SHALL retain usable labels, focus, touch targets, and layout with enlarged text.

#### Scenario: Locked device
- **WHEN** the owner attempts a sender reply or unknown-sender confirmation from a locked device
- **THEN** authentication is required before that financial confirmation is accepted

#### Scenario: Populated app upgrade
- **WHEN** the revised app upgrades the current populated companion database
- **THEN** pairing and original records are preserved, answer storage is added safely, and historical events are not automatically replayed or alerted

#### Scenario: Large text and long name
- **WHEN** the owner views a sender question in Tanya with enlarged text and a long valid sender name
- **THEN** labels, input, pending/error states, and both actions remain readable and reachable

### Requirement: The detail sheet answers sender questions

Tapping a sender question notification SHALL open that event's question in Tanya, below the original notification, where equivalent name and unknown-sender controls SHALL be available. The event's detail page SHALL link to the same question.

#### Scenario: Notification body tapped
- **WHEN** the owner taps the body of a sender question notification
- **THEN** the app opens Tanya at that event's question with a name field and a `Catat tanpa nama` action

### Requirement: Account-selection questions keep their controls

Account-selection questions SHALL retain their controls alongside sender questions: in Tanya the proposed account and its alternatives are offered, any other account can be chosen, and the owner confirms the choice.

#### Scenario: Account question arrives
- **WHEN** a result asks the owner to confirm an account
- **THEN** Tanya offers the account-confirmation controls, not the sender reply actions
