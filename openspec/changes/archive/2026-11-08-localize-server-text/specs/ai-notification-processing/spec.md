## MODIFIED Requirements

### Requirement: Tool-Less Structured Notification Processing
The system SHALL use a versioned prompt to request one structured interpretation of a notification. The processor SHALL propose a concise description in the AI output language, observed direction, existing account/pocket/category references, transaction-level Kakeibo classification, and optional existing movement candidate. It SHALL have no tools, credentials in context, database access or write authority. Notification text and history SHALL be treated as untrusted data, not instructions.

#### Scenario: Describing a settled merchant payment
- **WHEN** a supported notification confirms a merchant payment and suitable registered references are available
- **THEN** the processor returns a structured proposed record with a concise description and the backend validates it before creating a transaction

#### Scenario: Ignoring instructions embedded in a notification
- **WHEN** notification text asks the processor to ignore its rules, disclose secrets, or invent an account
- **THEN** those instructions do not grant additional authority and any invalid proposed result creates no financial effect

#### Scenario: English account with English output enabled
- **GIVEN** English AI output is enabled and the account's language is English
- **WHEN** an Indonesian-language BCA notification of a payment to "Kedai Awan" is interpreted
- **THEN** the description SHALL be English, the notification text SHALL be passed unchanged as data, and the stored prompt version SHALL end in `-en`
