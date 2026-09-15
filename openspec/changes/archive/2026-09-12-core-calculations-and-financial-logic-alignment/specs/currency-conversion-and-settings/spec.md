## ADDED Requirements

### Requirement: Streamlined User Settings & Profile Information
The system SHALL expose user settings and profile management in a unified interface:
1. The settings interface SHALL display the user's `username` and display name `name`.
2. The settings interface SHALL permit configuring:
   - `payday_day` (payday cycle day: 1 to 31).
   - `currency` (primary display currency: selected from supported currencies).
   - `emergency_fund_multiplier` (target emergency coverage multiplier, default 6x, e.g. 8x).
   - `monthly_spending_budget` (overall monthly spending budget ceiling).
3. The settings interface SHALL NOT display redundant view toggles (such as hide-balance checkboxes) or unconfigured third-party integrations.

#### Scenario: Updating user profile settings
- **WHEN** the user updates their emergency fund target multiplier to 8x and monthly spending budget to 12,000,000 IDR
- **THEN** the system persists the settings and recalculates runway and safe-to-spend benchmarks accordingly

### Requirement: Automated Daily Currency Conversion
The system SHALL support dynamic currency conversion between `IDR` and `USD`:
1. The currency selector in Settings SHALL offer a dropdown containing `IDR` (default) and `USD`.
2. The system SHALL fetch the daily exchange rate for `USDIDR=X` from Yahoo Finance market data and cache it.
3. When the user sets their primary currency to `USD`, monetary values SHALL be converted and displayed using the cached exchange rate with proper currency formatting.

#### Scenario: Switching primary currency to USD
- **WHEN** the user selects "USD" in the currency dropdown
- **THEN** the system updates the user's currency preference and applies the latest daily exchange rate
