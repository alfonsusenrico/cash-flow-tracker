## MODIFIED Requirements

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
