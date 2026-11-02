## ADDED Requirements

### Requirement: An explicit null clears a category pillar
`PATCH /api/categories/{id}` SHALL clear an expense category's stored pillar when the request sends `kakeibo_type: null`, and SHALL keep it when the field is left out. The web category form SHALL offer "Belum diklasifikasi" for every expense category and SHALL send an explicit null only when the owner chooses it. An income category SHALL accept a null pillar and reject any other.

#### Scenario: Clearing a pillar
- **GIVEN** category "Belanja Dapur" with pillar `need`, one expense in it without its own pillar, and one with its own `want` pillar
- **WHEN** the owner sends `PATCH /api/categories/{id}` with `kakeibo_type: null`
- **THEN** the category SHALL be listed with `kakeibo_type: null`, the first expense SHALL count as "Belum diklasifikasi", the second SHALL stay `want`, and the response SHALL report 1 correction

#### Scenario: Pillar field left out
- **WHEN** the owner updates a category with pillar `unexpected` with only its name, budget, icon and colour, or kind
- **THEN** it SHALL keep `unexpected` and the response SHALL report 0 corrections

#### Scenario: Choosing "Belum diklasifikasi" in the category form
- **GIVEN** the Analisis category form open on a category with pillar `want`
- **WHEN** the owner chooses "Belum diklasifikasi" and saves
- **THEN** the request SHALL carry `kakeibo_type: null` and the category SHALL have no pillar; when the owner saves without touching the pillar, the request SHALL carry `want`

#### Scenario: Clearing another owner's pillar
- **WHEN** a request sends `kakeibo_type: null` for a category of another owner
- **THEN** it SHALL answer 404, and that category's pillar and feedback SHALL stay unchanged

#### Scenario: Income category saved from the form
- **WHEN** the owner saves an income category from the web form, which sends `kind: income`, `monthly_budget: null` and `kakeibo_type: null`
- **THEN** it SHALL be saved without a pillar or budget, and a later request with a non-null pillar SHALL answer 422

## MODIFIED Requirements

### Requirement: Category edits teach habit learning
A change of a reportable expense category's pillar through `PATCH /api/categories/{id}` (Settings and Analisis), clearing included, SHALL write `classification_feedback` exactly as the "Lengkapi klasifikasi" category fix does: one `correction` row per expense of that category without its own pillar whose effective pillar changed, with owner merchant memory upserted, while the rows' review state and purpose questions stay unchanged. The response SHALL report the count as `corrections`.

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

#### Scenario: Pillar cleared in the category form
- **GIVEN** category "Makan Bareng" with pillar `want` holding one expense without its own pillar
- **WHEN** the owner clears the pillar
- **THEN** one feedback row SHALL exist with `kind = correction`, `before_kakeibo_type` `want` and `after_kakeibo_type` null, and clearing it again SHALL write nothing and report 0 corrections
