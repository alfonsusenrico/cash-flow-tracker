## MODIFIED Requirements

### Requirement: Invalid reply input stays correctable

Blank or invalid reply input SHALL NOT be queued and SHALL remain correctable. A reply from the question notification that the companion refuses or fails to store SHALL leave that question answerable from its notification, with both actions and a statement that the reply was not saved, unless an answer for that question is already stored or the question is no longer open.

#### Scenario: Blank name
- **WHEN** the owner submits a blank name
- **THEN** nothing is queued and the owner can enter a name and send again

#### Scenario: Refused notification reply stays answerable
- **WHEN** the owner replies to a sender question from its notification and the reply is not stored, for example because the device was locked or the reply text did not arrive
- **THEN** nothing is queued, the notification still offers `Balas nama` and `Catat tanpa nama` and says the reply was not saved, and a later lookup of the same question leaves it answerable

#### Scenario: Refused reply after a stored answer
- **WHEN** a second notification action arrives for a question whose answer is already stored
- **THEN** the stored answer is kept and the notification reports it without offering the actions again

## ADDED Requirements

### Requirement: Sender answer attempts are recorded privately

The companion SHALL record each step of every sender answer attempt, from the notification or the app, in bounded local storage that is excluded from backup: that the attempt was received, then that it was queued or why it was refused. A record SHALL hold only reason codes, channel, identifiers, time and app version, never a name, typed text or amount.

#### Scenario: Refused attempt is recorded
- **WHEN** a notification reply is refused because the device is locked
- **THEN** the record holds a received step and a locked step for that question, without the typed name

#### Scenario: Attempt that never completes
- **WHEN** an attempt is received but the app stops before storing or refusing it
- **THEN** the record holds only the received step, which distinguishes it from an attempt that never reached the app

#### Scenario: Bounded record
- **WHEN** more than 200 attempt steps have been recorded
- **THEN** only the newest 200 are kept

### Requirement: Tanya shows an unsaved notification reply

Tanya SHALL state that a notification reply was not saved while the newest settled attempt for the shown sender question is a refused notification reply, and SHALL keep the question answerable.

#### Scenario: Tanya shows an unsaved notification reply
- **WHEN** the owner opens Tanya on a sender question whose newest settled attempt is a refused notification reply
- **THEN** the card says the reply from the notification was not saved and keeps the name field and both actions available

#### Scenario: Newer answer replaces the notice
- **WHEN** a newer attempt for the same question settles after the refused notification reply
- **THEN** Tanya no longer shows the unsaved-reply notice
