## MODIFIED Requirements

### Requirement: Forms keep a row unclassified until the owner chooses
Recording or recategorising a transaction into a category without a pillar SHALL store no transaction pillar, so that the row follows the category. The web ledger edit form and the category form SHALL show "Belum diklasifikasi" for a row or category without a pillar and SHALL NOT store `need` unless the owner chooses it.

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

## ADDED Requirements

### Requirement: Category edits teach habit learning
A change of a reportable expense category's pillar through `PATCH /api/categories/{id}` (Settings and Analisis) SHALL write `classification_feedback` exactly as the "Lengkapi klasifikasi" category fix does: one `correction` row per expense of that category without its own pillar whose effective pillar changed, with owner merchant memory upserted, while the rows' review state and purpose questions stay unchanged. The response SHALL report the count as `corrections`.

#### Scenario: Pillar changed in the category form
- **GIVEN** category "Lain-lain" without a pillar holding two unclassified rows and one row with its own `want` pillar
- **WHEN** the owner saves the category with pillar `need` through `PATCH /api/categories/{id}`
- **THEN** two feedback rows SHALL exist with `kind = correction`, `before_kakeibo_type` null and `after_kakeibo_type` `need`, the third row SHALL stay `want`, and the response SHALL report 2 corrections

#### Scenario: Edit without a pillar change
- **WHEN** the owner saves a category with its current pillar, or changes only its name or budget
- **THEN** no feedback row SHALL be written and the response SHALL report 0 corrections

#### Scenario: Category that is not spending
- **WHEN** the owner changes the pillar of a category that reports never count as spending, such as "Investasi"
- **THEN** no feedback row SHALL be written

#### Scenario: Another owner's category
- **WHEN** a request edits a category of another owner
- **THEN** it SHALL answer 404 and write no feedback
