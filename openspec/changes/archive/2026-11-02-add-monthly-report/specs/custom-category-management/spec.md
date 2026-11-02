## MODIFIED Requirements

### Requirement: Category Budget Variance Filtering
The `/insights` category budget matrix SHALL strictly filter and display categories of kind `expense`:
1. Income categories (e.g. "Gaji", "Pendapatan Lain") SHALL NOT be displayed in the Category Spending Variance or Budget table.
2. Users SHALL be able to click directly on any category's budget cell to edit or set its `monthly_budget` ceiling via an inline modal.
3. While no expense category has a budget, the table SHALL show no budget figure or column and be titled "Pengeluaran per Kategori".

#### Scenario: Viewing the category budget matrix
- **WHEN** the user views the Category Budget table on the `/insights` page
- **THEN** only expense categories are shown, each with their `Pokok` or `Opsional` badge, actual spending, and budget ceiling

#### Scenario: No budget set
- **WHEN** no expense category has a monthly budget
- **THEN** the table SHALL list categories with their spending only, without "Tanpa Batas" chips, budget columns or "Atur batas" buttons
