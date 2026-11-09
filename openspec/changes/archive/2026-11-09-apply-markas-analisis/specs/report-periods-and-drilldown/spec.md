## MODIFIED Requirements

### Requirement: Non-Editable Insight Rows
Archived and "Tanpa kategori" rows in Analisis's spending by category SHALL link to the ledger and SHALL offer no budget, edit or archive action. In "Kelola kategori" an archived category SHALL offer only "Pulihkan", and "Tanpa kategori" SHALL NOT be listed.

#### Scenario: Uncategorised row
- **WHEN** Analisis lists "Tanpa kategori"
- **THEN** that row SHALL link to the ledger but offer no budget, edit or archive action.

#### Scenario: Archived category with spending
- **WHEN** a category archived after the period still has spending in it
- **THEN** it SHALL be listed under its pillar with "diarsipkan", link to the ledger, and appear in "Kelola kategori" only under "Diarsipkan" with "Pulihkan"
