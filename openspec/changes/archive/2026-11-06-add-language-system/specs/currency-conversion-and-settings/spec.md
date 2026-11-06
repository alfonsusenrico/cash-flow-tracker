## MODIFIED Requirements

### Requirement: Streamlined User Settings & Profile Information
The system SHALL persist and present user settings and profile information through aligned registration and settings contracts, including the account's interface language (`en` or `id`) and region (`ID` or `US`), which registration, sign-in, the profile endpoint and the settings endpoint SHALL return or accept.

#### Scenario: Required behavior and constraints
- **WHEN** an owner registers, views or updates profile and settings information
- **THEN** registration SHALL persist a submitted display name or omit the control, and settings SHALL display username and permit updating display name
- **AND** settings SHALL support salary-source configuration, IDR and USD, an emergency-fund multiplier, an optional monthly spending budget, the interface language and the region
- **AND** unsupported currencies, languages, regions and invalid numeric ranges SHALL be rejected by both form and server without changing saved settings
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

#### Scenario: Registering with language and region
- **WHEN** a person registers from the English sign-up page with region Indonesia
- **THEN** `/auth/me` SHALL return `language` `en` and `region` `ID`

#### Scenario: Registering without a language
- **WHEN** a client registers without `language` and with `Accept-Language: id-ID`
- **THEN** the account's language SHALL be `id` and its region `ID`

#### Scenario: Rejecting an unsupported language or region
- **WHEN** a client saves `language` `fr` or `region` `SG`
- **THEN** the server SHALL respond 422 with `detail.code` `invalid_language` or `invalid_region`, and the saved language and region SHALL be unchanged

#### Scenario: Another account's settings
- **WHEN** one account saves its language
- **THEN** no other account's language or region SHALL change
