## MODIFIED Requirements

### Requirement: Inline Category Budget Editing
Analisis SHALL let the owner set or clear any expense category's monthly budget from "Atur anggaran", a centered dialog with one amount per expense category that reports count, and from the category form in "Kelola kategori". Saving SHALL call `PATCH /api/categories/{id}` for each changed category and SHALL refresh the budget bars, shares and over-budget marks at once.

#### Scenario: Setting a category budget limit
- **WHEN** the owner opens "Atur anggaran", enters 3.000.000 for "Makanan & Minuman" and saves
- **THEN** the category's `monthly_budget` SHALL become 3,000,000 IDR, and its row SHALL show the bar and "{n}% dari Rp 3.000.000" at once

#### Scenario: Clearing a budget
- **WHEN** the owner empties the amount of a category in "Atur anggaran" and saves
- **THEN** the category SHALL have no budget and its row SHALL read "tanpa anggaran"
