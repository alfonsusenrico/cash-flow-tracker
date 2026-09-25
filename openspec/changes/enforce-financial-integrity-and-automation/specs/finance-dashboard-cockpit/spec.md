# Spec Delta

## MODIFIED Requirements

### Requirement: Executive KPI Summary Ribbon
The dashboard SHALL display an executive summary ribbon containing:
1. `Liquid Balance (Kas & Bank)`: total spendable balance across active cash, bank, e-wallet, and wallet accounts, excluding investment instruments and any amount below zero that is pending reconciliation from available-to-spend calculations.
2. `Total Inflow`: actual non-transfer income recorded within the active timeframe.
3. `Total Outflow`: actual non-transfer expenses recorded within the active timeframe.
4. `Surplus Arus Kas`: `Inflow - Outflow` for the active timeframe.
5. `Ketahanan Dana`: emergency fund coverage against the configured target multiplier.
6. Aggregate net worth and investment values SHALL be returned and labeled separately from liquid balance.

#### Scenario: Separating liquid and investment assets
- **WHEN** a user has 10,000,000 IDR in bank accounts and 25,000,000 IDR in investment instruments
- **THEN** the liquid KPI reports 10,000,000 IDR and does not report the 35,000,000 IDR aggregate as liquid

#### Scenario: Displaying executive KPIs
- **WHEN** the user views the Overview dashboard
- **THEN** liquid balance excludes investments, inflow and outflow exclude internal movements, and Ketahanan Dana uses the configured multiplier
