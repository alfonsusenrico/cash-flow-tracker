## ADDED Requirements

### Requirement: Transfers Teach No Memory
An owner correction or purpose answer that puts a row in an internal-movement category SHALL write feedback but SHALL NOT create or change a memory entry.

#### Scenario: Transfer answer teaches nothing
- **WHEN** the owner answers the purpose question of Jago's "You've received Rp500.000 from GoPay." in "Internal Movement"
- **THEN** a feedback row SHALL be written and no memory entry for "signature:jago|income|unknown" SHALL be created or changed.

#### Scenario: Transfer memory learned earlier is not applied
- **GIVEN** an owner memory entry "merchant:gopay" in "Internal Movement", learned before transfers stopped teaching memory
- **WHEN** a notification with that match key is classified
- **THEN** the entry SHALL be neither applied nor shown to the model, and the entry SHALL stay stored
