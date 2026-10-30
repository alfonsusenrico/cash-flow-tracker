# Spec Delta

## MODIFIED Requirements

### Requirement: Net Fresh Savings Calculation
The system SHALL classify savings from the economic direction of a durably linked movement.

#### Scenario: Required behavior and constraints
- **WHEN** savings are calculated from linked internal movements
- **THEN** the following required behavior and constraints hold:

  The system SHALL classify savings from the economic direction of a durably linked movement:
  1. A savings destination is an account linked to a goal or an investment product. Generic movements cannot target an investment position; an investment product is funded through a trade or an investment top-up.
  2. A movement from an operational liquid account into a savings destination SHALL increase net fresh savings.
  3. A movement from a savings source back into an operational liquid account SHALL reduce net fresh savings.
  4. A movement between operational liquid accounts, including a liquid investment-funding account such as an RDN, SHALL not affect savings.
  5. A trade between a funding account and an investment instrument position SHALL remain excluded from Kakeibo turnover.
  6. Manual, plan-recorded, captured and ingested movements SHALL use the same classification rule; automation SHALL NOT mark every transfer as saving.

#### Scenario: User transfers funds from Bank to Investment account
- **WHEN** a user tops up an amount-tracked investment product from an operational bank account
- **THEN** the top-up SHALL be recognized as monthly savings

#### Scenario: Moving money into a goal-linked pocket
- **WHEN** a user moves money from an operational bank account into a pocket linked to a savings goal
- **THEN** the amount increases net fresh savings

#### Scenario: Subtracting a savings withdrawal
- **WHEN** a user moves money from a goal-linked savings pocket back to an operational bank account
- **THEN** the amount reduces net fresh savings for the active period

#### Scenario: User transfers funds between checking or e-wallet accounts
- **WHEN** a user moves funds between operational checking accounts, pockets, e-wallets or a liquid RDN funding account
- **THEN** the movement does not affect the savings pillar
