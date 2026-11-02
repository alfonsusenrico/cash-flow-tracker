# kakeibo-classification-completeness Specification

## Purpose
Report spending without a Kakeibo pillar honestly as "Belum diklasifikasi" instead of counting it as need, and let the owner complete classifications quickly through the owner-edit path so habit learning keeps learning.

## Requirements

### Requirement: Unclassified spending bucket
An expense with no transaction pillar and no category pillar SHALL be unclassified. Its effective pillar SHALL be `null` on every row the API returns (ledger, review queue, capture results, `top_categories`) and SHALL never be reported as `need`. Aggregates SHALL report it as a sixth bucket `unclassified`: the Kakeibo breakdown SHALL return `unclassified_spent` and `unclassified_pct`, comparison pillar lists SHALL include it, and reimbursement offsets SHALL apportion to it like any pillar.

#### Scenario: Missing pillar is not need
- **GIVEN** a Rp 40.000 expense in a category without a pillar and a Rp 60.000 `need` expense
- **WHEN** the Kakeibo breakdown is requested
- **THEN** `need_spent` SHALL be Rp 60.000 and `unclassified_spent` Rp 40.000, and the ledger row of the first expense SHALL carry `kakeibo_type: null`

#### Scenario: Category pillar applies later
- **GIVEN** an unclassified expense in a category without a pillar
- **WHEN** the category is given the pillar `want`
- **THEN** the expense SHALL count as `want` and no longer as unclassified

#### Scenario: Phone compatibility
- **WHEN** the cash-flow summary lists a category without a pillar in `top_categories`
- **THEN** its `kakeibo_type` SHALL be `null`, never the string `unclassified`

### Requirement: Unclassified bucket in the web
The web SHALL show "Belum diklasifikasi" with its amount wherever the Kakeibo split is shown and the bucket holds spending, with a text label rather than colour alone and a link to its rows.

#### Scenario: Beranda and Analisis
- **WHEN** the selected period holds Rp 40.000 of unclassified spending
- **THEN** the Kakeibo views SHALL list "Belum diklasifikasi" Rp 40.000 beside the five pillars, linked to the ledger with `kakeibo=unclassified`

### Requirement: Unclassified ledger filter
`GET /api/transactions` SHALL accept `kakeibo_type=unclassified`, returning the unclassified expenses, and the ledger SHALL accept `kakeibo=unclassified` in its URL and show it as an active filter "Belum diklasifikasi". Its summary SHALL apply reimbursement offsets of the unclassified bucket as it does for a pillar.

#### Scenario: Filter equals the bucket
- **WHEN** the ledger is filtered to a month, `type=expense` and `kakeibo_type=unclassified`
- **THEN** the summary outflow SHALL equal the month's `unclassified_spent`

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

### Requirement: Lengkapi klasifikasi
The system SHALL offer "Lengkapi klasifikasi", reachable from the report's data-quality box, listing for a window the active expense categories without a pillar, with the count and amount of their unclassified rows, and the unclassified rows themselves (`GET /api/classification/gaps`). Every classification route SHALL be owner-scoped.

#### Scenario: Gaps listed
- **GIVEN** a category without a pillar holding two unclassified rows and one uncategorised unclassified row in October
- **WHEN** the gaps are requested for October
- **THEN** the category SHALL be listed with 2 rows and their amount, and all three rows SHALL be listed

### Requirement: Category and row pillar fixes
The owner SHALL be able to set a category's pillar (`POST /api/classification/categories/{id}`), which applies to its rows without their own pillar, and to set the pillar of 1 to 200 unclassified rows at once (`POST /api/classification/transactions`, all or nothing). Movement legs SHALL be rejected with 409, and income rows or income categories with 422.

#### Scenario: Category fix
- **GIVEN** category "Langganan" without a pillar holding two unclassified rows and one row with its own `want` pillar
- **WHEN** the owner sets "Langganan" to `need`
- **THEN** the two rows SHALL count as `need`, the third SHALL stay `want`, and the response SHALL report 2 corrections

#### Scenario: Several rows at once
- **WHEN** the owner selects three uncategorised unclassified rows and chooses `unexpected`
- **THEN** all three SHALL be stored with `kakeibo_type = 'unexpected'` and `review_status = reviewed`

#### Scenario: Another owner's rows
- **WHEN** a request names a category or transaction of another owner
- **THEN** it SHALL answer 404 and change nothing

### Requirement: Classification fixes teach habit learning
Every row whose effective pillar a "Lengkapi klasifikasi" action changes SHALL produce exactly one append-only `classification_feedback` row of kind `correction`, written as the existing owner edit paths write it, and SHALL upsert owner merchant memory as they do. A row-level fix SHALL mark the row owner-edited and reviewed; a category-level fix SHALL leave the rows' review state and purpose questions unchanged.

#### Scenario: Feedback rows written
- **WHEN** the owner sets the pillar of two unclassified rows through "Lengkapi klasifikasi"
- **THEN** two feedback rows SHALL exist with `kind = correction`, `before_kakeibo_type` null and `after_kakeibo_type` the chosen pillar

#### Scenario: Nothing changed, nothing written
- **WHEN** the owner sets a category to the pillar it already has
- **THEN** no feedback row SHALL be written

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
