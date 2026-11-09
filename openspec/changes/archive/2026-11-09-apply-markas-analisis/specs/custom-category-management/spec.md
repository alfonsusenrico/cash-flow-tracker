## MODIFIED Requirements

### Requirement: Category Budget Variance Filtering
Analisis's spending by category SHALL list expense categories only; income categories SHALL appear only in the "Pemasukan" group of "Kelola kategori". Budgets SHALL be set in "Atur anggaran" or the category form, never inline in the list. While no expense category has a budget, the list SHALL show spending only, without budget bars, shares or "tanpa anggaran".

#### Scenario: Viewing the category budget matrix
- **WHEN** the owner views spending by category on Analisis
- **THEN** only expense categories SHALL be listed, grouped by pillar, each with its spending and, when budgeted, its bar and share of the budget

#### Scenario: No budget set
- **WHEN** no expense category has a monthly budget
- **THEN** the list SHALL show categories with their spending only, without bars, shares, "tanpa anggaran" or budget buttons in the rows

#### Scenario: Managing categories in the dialog
- **WHEN** the owner opens "Kelola kategori"
- **THEN** a centered dialog SHALL list the expense categories by pillar, the income categories and the archived ones, offer "Ubah" and "Arsipkan" per active category and "Pulihkan" per archived one, and add categories with "Tambah kategori" and "Tambah kategori pemasukan"
