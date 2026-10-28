## MODIFIED Requirements

### Requirement: Independent Transaction Kakeibo Selection
The Quick Capture interface and the ledger edit form SHALL allow the user to choose any Kakeibo pillar (`need`, `want`, `culture`, `unexpected` or `saving`) independently of the selected category. Selecting a category SHALL NOT overwrite a pillar the user has chosen.

#### Scenario: Categorizing a discretionary dining expense
- **GIVEN** the Quick Capture modal is open for an expense
- **WHEN** the user selects the `[👑 Want]` pill
- **AND** selects the category "Makanan & Minuman"
- **THEN** the Kakeibo pill SHALL remain `[👑 Want]`
- **AND** submitting the transaction SHALL save `kakeibo_type = 'want'` with the chosen category ID.

#### Scenario: Recording a culture expense
- **GIVEN** the user records a sports court booking as an expense
- **WHEN** the user selects the culture pillar and submits
- **THEN** the transaction SHALL be saved with `kakeibo_type = 'culture'`.

#### Scenario: AI chooses an unexpected pillar
- **GIVEN** AI notification processing proposes `kakeibo = 'unexpected'` for a settled expense
- **WHEN** the backend records the transaction
- **THEN** the insert SHALL succeed with `kakeibo_type = 'unexpected'`
- **AND** the notification event SHALL end in `recorded`, not `failed`.

#### Scenario: Unknown pillar is rejected
- **WHEN** a client creates or updates a transaction or category with `kakeibo_type = 'luxury'`
- **THEN** the API SHALL respond with a 422 validation error
- **AND** no row SHALL change.

---

## ADDED Requirements

### Requirement: Kakeibo Pillar Vocabulary
The backend SHALL accept exactly the pillars `need`, `want`, `culture`, `unexpected` and `saving` wherever a transaction or category pillar is written: manual transactions, category defaults, notification resolution and AI interpretation. It SHALL reject any other value with a validation error.

#### Scenario: Category default accepts a new pillar
- **WHEN** a client updates a category with `kakeibo_type = 'culture'`
- **THEN** the category SHALL be stored with the `culture` pillar.

#### Scenario: Retired pillar name is rejected
- **WHEN** a client creates a transaction with `kakeibo_type = 'investment'`
- **THEN** the API SHALL respond with a 422 validation error and no row SHALL be created.
