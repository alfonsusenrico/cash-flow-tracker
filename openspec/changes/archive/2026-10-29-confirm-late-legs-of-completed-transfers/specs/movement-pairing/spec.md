## ADDED Requirements

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
