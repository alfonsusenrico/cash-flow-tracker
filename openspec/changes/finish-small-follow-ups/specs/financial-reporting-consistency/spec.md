## ADDED Requirements

### Requirement: Top spending categories in the cash-flow summary
`GET /api/dashboard/cash-flow-summary`, for `period` `today` or `month` and with session or Bearer authentication, SHALL return `top_categories`: at most 3 of the owner's categories with the most net spending, as `category_id`, `name`, `kakeibo_type` and integer rupiah `outflow`, sorted by outflow descending, then name. The list SHALL use the same window and spending rules as the response's `outflow`, SHALL omit a category at 0 or less, and SHALL be `[]`, never `null`, without spending.

#### Scenario: Reimbursement netted per category
- **GIVEN** today's expenses of Rp 300.000 in "Makan Bareng" and Rp 200.000 in "Belanja Dapur", and a reimbursement of Rp 200.000 that offsets "Makan Bareng"
- **WHEN** the phone requests the summary for `today`
- **THEN** `outflow` SHALL be Rp 300.000 and `top_categories` SHALL list "Belanja Dapur" Rp 200.000, then "Makan Bareng" Rp 100.000

#### Scenario: Fully reimbursed category
- **WHEN** a reimbursement covers all of a category's spending in the window
- **THEN** that category SHALL NOT be listed

#### Scenario: Movements, investments and fees
- **WHEN** the window holds internal movements, investment trades, expenses in "Investasi" or "Internal Movement", a grocery expense and a transfer fee
- **THEN** only the grocery category and the fee category SHALL be listed, and their amounts SHALL add up to `outflow`

#### Scenario: Listed amounts never exceed the outflow
- **WHEN** any summary is returned
- **THEN** the sum of the listed `outflow` amounts SHALL be at most the response's `outflow`; spending without a category counts in `outflow` but SHALL NOT be listed

#### Scenario: Today and month
- **WHEN** the owner spent today in one category and earlier this month in another
- **THEN** `today` SHALL list only today's category and `month` SHALL list both, each from the month's first local day to now

#### Scenario: No spending
- **WHEN** the window holds no reportable spending
- **THEN** `top_categories` SHALL be an empty list

#### Scenario: Limit and ties
- **WHEN** four categories have the same outflow
- **THEN** the first three by name SHALL be listed

#### Scenario: Owner scope and authentication
- **WHEN** another owner spends in a category of the same name, or the phone calls with its Bearer key
- **THEN** the other owner's spending SHALL NOT be listed, and the Bearer response SHALL equal the session response; an unknown key SHALL get HTTP 401

#### Scenario: Older clients
- **WHEN** a client that does not know `top_categories` reads the summary
- **THEN** every other field SHALL keep its name, type and value
