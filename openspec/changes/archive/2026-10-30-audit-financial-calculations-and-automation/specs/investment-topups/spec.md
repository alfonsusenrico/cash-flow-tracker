## MODIFIED Requirements

### Requirement: Product entry and ledger presentation
The web interface SHALL expose Top up on eligible products with product context, funding selector, amount, date/time, notes, and an optional non-posting monthly plan shortcut.

#### Scenario: Required behavior and constraints
- **WHEN** an owner opens an eligible investment product or inspects its contribution in the ledger
- **THEN** the form SHALL omit units and NAB and default to the eligible product funding account, then its parent's funding account
- **AND** contributions SHALL appear once as a consolidated investment transfer on desktop and mobile and SHALL be excluded from ordinary income and daily expense totals
- **AND** investment allocation summaries SHALL count the amount once
- **AND** controls SHALL support keyboard operation, labeled inputs, visible errors, focus restoration and responsive layouts
- **AND** creating or reaching a monthly plan SHALL NOT post a contribution

#### Scenario: Enter a top-up from a product
- **WHEN** the owner opens Top up on an eligible product with a configured funding account
- **THEN** the form defaults to that account, accepts a rupiah amount without units or NAB, and shows one investment top-up row after successful recording

#### Scenario: Unit-tracked product remains explicit
- **WHEN** the owner views a unit-tracked mutual-fund product
- **THEN** Beli/Jual retain their unit-based behavior and the account list omits persistent eligibility explanation

#### Scenario: Failure remains actionable
- **WHEN** a contribution submission fails
- **THEN** the form stays open with entered values and an understandable error, and no success state is shown
