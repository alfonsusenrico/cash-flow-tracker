## MODIFIED Requirements

### Requirement: Suggestion Storage and Privacy
Each suggestion SHALL store the transaction, the run, the source (`memory`, `rule` or `ai`), the proposed merchant, notes, category, pillar and offset, the confidence (model only), a short reason in the account's language at creation (the model's reason follows the AI output language), the row's values when proposed, and a status `pending`, `accepted`, `rejected` or `superseded`.

#### Scenario: Redacted model input
- **GIVEN** a row whose notes contain the owner's name
- **WHEN** the row is sent to the model
- **THEN** the input SHALL contain the redaction marker instead of the name.

#### Scenario: Logs
- **WHEN** a run completes
- **THEN** its log line SHALL contain counts and codes and no merchant, notes or model output.

#### Scenario: Template reason in English
- **WHEN** a memory-sourced suggestion is stored for an English account
- **THEN** its reason SHALL be English and SHALL carry its reason code and parameters, and an Indonesian account's reason SHALL be byte-identical to today's wording
