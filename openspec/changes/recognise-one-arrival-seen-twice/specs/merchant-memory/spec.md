## ADDED Requirements

### Requirement: Transfers Teach No Memory
An owner correction or purpose answer that puts a row in an internal-movement category SHALL write feedback but SHALL NOT create or change a memory entry.

#### Scenario: Transfer answer teaches nothing
- **WHEN** the owner answers the purpose question of Jago's "You've received Rp500.000 from GoPay." in "Internal Movement"
- **THEN** a feedback row SHALL be written and no memory entry for "signature:jago|income|unknown" SHALL be created or changed.
