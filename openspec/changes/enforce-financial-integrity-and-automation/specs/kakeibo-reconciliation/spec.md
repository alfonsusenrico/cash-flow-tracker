# Spec Delta

## MODIFIED Requirements

### Requirement: Net Fresh Savings Calculation
The system SHALL classify savings from the economic direction of a durably linked movement:
1. A movement from an operational liquid account into a savings or investment destination SHALL increase net fresh savings.
2. A movement from a savings or investment source back into an operational liquid account SHALL reduce net fresh savings.
3. A movement between operational liquid accounts SHALL not affect savings.
4. A trade between a funding account and an investment instrument position SHALL remain excluded from Kakeibo turnover.
5. Manual, recurring, payroll, and ingested movements SHALL use the same classification rule; automation SHALL NOT mark every transfer as saving.

#### Scenario: Counting fresh investment funding
- **WHEN** a user moves money from an operational bank account into an investment funding account without executing a trade
- **THEN** the amount increases net fresh savings

#### Scenario: Excluding an operational recurring transfer
- **WHEN** a recurring rule moves money between two operational pockets
- **THEN** neither record contributes to the Kakeibo savings pillar

#### Scenario: Subtracting a savings withdrawal
- **WHEN** a user moves money from a savings account back to an operational bank account
- **THEN** the amount reduces net fresh savings for the active period

#### Scenario: User transfers funds from Bank to Investment account
- **WHEN** a user moves fresh funds from an operational bank account to an investment funding account without executing a trade
- **THEN** the movement increases net fresh savings

#### Scenario: User transfers funds between checking or e-wallet accounts
- **WHEN** a user moves funds between operational checking accounts, pockets, or e-wallets
- **THEN** the movement does not affect the savings pillar
