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
Recording or recategorising a transaction into a category without a pillar SHALL store no transaction pillar, so that the row follows the category. The web ledger edit form and the category form SHALL show "Belum diklasifikasi" for a row or category without a pillar and SHALL NOT store `need` unless the owner chooses it.

#### Scenario: Editing notes of an unclassified row
- **WHEN** the owner changes only the notes of an unclassified expense in the ledger edit form and saves
- **THEN** the row SHALL stay unclassified

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
