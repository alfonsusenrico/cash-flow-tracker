## MODIFIED Requirements

### Requirement: Forms keep a row unclassified until the owner chooses
Recording or recategorising a transaction into a category without a pillar SHALL store no transaction pillar, so that the row follows the category. The web ledger edit form and the category form SHALL show "Belum diklasifikasi" for a row or category without a pillar and SHALL NOT store `need` unless the owner chooses it. A typed capture SHALL keep a model pillar that meets the capture confidence threshold, an explicit classification and not a default.

#### Scenario: Editing notes of an unclassified row
- **WHEN** the owner changes only the notes of an unclassified expense in the ledger edit form and saves
- **THEN** the row SHALL stay unclassified

#### Scenario: Typed capture into a category without a pillar
- **GIVEN** an expense category without a pillar
- **WHEN** a typed capture is recorded in it and neither the owner nor the model chose a pillar
- **THEN** the transaction SHALL be stored with no pillar, the capture's category option and result SHALL carry `kakeibo: null`, and reports SHALL count it as "Belum diklasifikasi"

#### Scenario: Typed capture with a chosen pillar
- **WHEN** the owner answers a capture's category question with a category without a pillar and the pillar `culture`
- **THEN** the transaction SHALL be stored with `kakeibo_type = 'culture'`

#### Scenario: Category created without a pillar
- **WHEN** an expense category is created through `POST /api/categories` without `kakeibo_type`, with `is_primary` true or false
- **THEN** it SHALL be stored and listed with `kakeibo_type: null`

#### Scenario: Typed capture with a confident model pillar
- **GIVEN** an expense category without a pillar
- **WHEN** a typed capture is recorded in it and the model proposes that category and the pillar `want` with a category confidence that meets the capture confidence threshold
- **THEN** the transaction SHALL be recorded with `kakeibo_type = 'want'` and classifier `ai`
