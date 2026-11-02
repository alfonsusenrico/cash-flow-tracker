## MODIFIED Requirements

### Requirement: Product entry and ledger presentation
The web interface SHALL expose Top up on eligible products with product context, funding selector, amount, date/time, notes, and an optional non-posting monthly plan shortcut.

#### Scenario: Required behavior and constraints
- **WHEN** an owner opens an eligible investment product or inspects its contribution in the ledger
- **THEN** the form SHALL omit units and NAB and default to the eligible product funding account, then its parent's funding account
- **AND** contributions SHALL appear once as a consolidated investment transfer on desktop and mobile and SHALL be excluded from ordinary income and daily expense totals
- **AND** every `recent_transactions` row of `GET /api/dashboard/overview` SHALL carry `investment_topup_id`, the contribution's id on both of its legs and `null` on every other row, with the same meaning as in the ledger, and SHALL be produced without an additional database statement per row
- **AND** the Beranda "Aktivitas Terkini" on desktop and at phone width SHALL paint a contribution row and its detail dialog lavender, not sky, and SHALL name it "Top up Investasi" in the row, also when the row shows its own note, and in the dialog
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

#### Scenario: Top-up in the home feed
- **WHEN** the owner records a 250,000 IDR top-up, a 200,000 IDR "Pindah Saldo" and a 15,000 IDR expense and opens Beranda at 390 or 1280 px, in the light or the dark theme
- **THEN** both top-up legs in `recent_transactions` carry the top-up's `investment_topup_id`, the other three rows carry `null`, the top-up rows show "Top up Investasi" with a lavender icon tile and amount, the transfer rows stay sky and the expense stays coral

#### Scenario: Home feed statements do not grow with top-ups
- **WHEN** the home feed holds one top-up and later four top-ups
- **THEN** `GET /api/dashboard/overview` runs the same number of database statements in both cases
