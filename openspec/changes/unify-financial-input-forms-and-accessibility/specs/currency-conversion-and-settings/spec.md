# Spec Delta

## MODIFIED Requirements

### Requirement: Streamlined User Settings & Profile Information
The system SHALL persist and present user settings and profile information through aligned registration and settings contracts:
1. Registration SHALL either persist the submitted display name or omit that control; it SHALL NOT accept and silently discard it.
2. Settings SHALL display username and permit updating the persisted display name.
3. Settings SHALL permit configuring payday day from 1 through 31, a supported currency from the explicit `IDR` and `USD` set, emergency-fund multiplier, and optional monthly spending budget.
4. Unsupported currency values and invalid numeric ranges SHALL be rejected by both form and server without changing saved settings.
5. Successful changes SHALL refresh every view that consumes the affected user settings.
6. The interface SHALL NOT display redundant view toggles or unconfigured integrations.

#### Scenario: Registering with a display name
- **WHEN** a new user submits a valid optional display name during registration
- **THEN** the name is persisted and appears in authenticated profile settings

#### Scenario: Rejecting an unsupported currency
- **WHEN** a client attempts to save a currency other than IDR or USD
- **THEN** the server rejects the request and preserves the existing currency

#### Scenario: Updating user profile settings
- **WHEN** a user updates the emergency-fund multiplier to 8 and monthly spending budget to 12,000,000 IDR
- **THEN** the system persists both values and refreshes runway and safe-to-spend consumers
