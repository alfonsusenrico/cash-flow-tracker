# currency-conversion-and-settings Specification

## Purpose
Define user profile settings and currency display conversion using maintained exchange-rate information.

## Requirements

### Requirement: Streamlined User Settings & Profile Information
The system SHALL persist and present user settings and profile information through aligned registration and settings contracts.

#### Scenario: Required behavior and constraints
- **WHEN** an owner registers, views or updates profile and settings information
- **THEN** registration SHALL persist a submitted display name or omit the control, and settings SHALL display username and permit updating display name
- **AND** settings SHALL support salary-source configuration, IDR and USD, an emergency-fund multiplier and an optional monthly spending budget
- **AND** unsupported currencies and invalid numeric ranges SHALL be rejected by both form and server without changing saved settings
- **AND** successful changes SHALL refresh all affected consumers and the interface SHALL omit redundant toggles and unconfigured integrations

#### Scenario: Registering with a display name
- **WHEN** a new user submits a valid optional display name during registration
- **THEN** it is persisted and appears in authenticated profile settings

#### Scenario: Rejecting an unsupported currency
- **WHEN** a client attempts to save a currency other than IDR or USD
- **THEN** the server rejects the request and preserves the existing currency

#### Scenario: Updating user profile settings
- **WHEN** a user updates the emergency multiplier and monthly budget
- **THEN** both values are persisted and the affected coverage and budget views are refreshed

### Requirement: Automated Daily Currency Conversion
The system SHALL support dynamic currency conversion and formatting between `IDR` and `USD`.

#### Scenario: Required behavior and constraints
- **WHEN** the system refreshes exchange rates or displays a currency amount
- **THEN** the following required behavior and constraints hold:

  The system SHALL support dynamic currency conversion and formatting between `IDR` and `USD`:
  1. The currency selector in Settings SHALL offer a dropdown containing `IDR` (default) and `USD`, accompanied by the current exchange rate hint (e.g. `1 USD ≈ Rp 16.500`).
  2. The backend SHALL fetch the daily exchange rate for `USDIDR=X` from market data, cache it, and expose it via `/auth/currency/rates`.
  3. When the user selects `USD` as primary currency:
     - All monetary balances, transaction amounts, goals, obligations, and budget metrics stored in base IDR SHALL be converted dynamically (`value_usd = value_idr / usdidr_rate`).
     - Converted values SHALL be formatted using US dollar currency notation (e.g., `$1,208.27` or `$1,208.00`).
     - If balance hiding is enabled (`hideBalances = true`), values SHALL be masked as `$ ••••••` instead of `Rp ••••••`.
     - Chart axes and tooltips SHALL format values using dollar scale benchmarks ($k, $M).
  4. When the user selects `IDR` as primary currency:
     - Monetary values SHALL be formatted using Indonesian Rupiah notation (`Rp 19.936.520`) with dot thousand separators and no decimal places.
     - If balance hiding is enabled (`hideBalances = true`), values SHALL be masked as `Rp ••••••`.

#### Scenario: Switching primary currency to USD
- **WHEN** the user selects "USD" in the currency dropdown and saves settings
- **THEN** the system updates the user's currency preference, refetches exchange rates, and all balances across the dashboard, goals, accounts, transactions, and insights are converted and displayed with the "$" symbol and 2 decimal places

#### Scenario: Viewing savings goals in USD
- **WHEN** a user with primary currency set to "USD" views the Goals page
- **THEN** all savings goal amounts (current amount, target amount, linked account balances, monthly target pace) and obligation balances are rendered in USD (e.g. "$1,208.27 of $1,212.12") instead of IDR

#### Scenario: Switching back to IDR
- **WHEN** the user reverts their primary currency preference to "IDR"
- **THEN** all balances and metrics immediately re-render in Indonesian Rupiah with "Rp" prefix and dot thousand separators without conversion loss
