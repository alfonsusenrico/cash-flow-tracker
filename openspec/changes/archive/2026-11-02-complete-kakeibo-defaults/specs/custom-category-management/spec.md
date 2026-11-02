## ADDED Requirements

### Requirement: Starter Category Pillars
At registration, the seeded expense categories SHALL carry default Kakeibo pillars: "Makanan & Minuman", "Belanja Harian", "Transportasi", "Tagihan & Utilitas" and "Kesehatan" `need`; "Jajan & Nongkrong", "Belanja" and "Hiburan" `want`. "Investasi", both "Internal Movement" categories and the income categories SHALL carry none. Seeding SHALL apply only to a user without categories and SHALL NOT change any existing user's categories or transactions.

#### Scenario: New user starter pillars
- **WHEN** a new user registers
- **THEN** the seeded categories SHALL carry exactly these pillars, and an expense in "Transportasi" SHALL count as `need` without any classification step

#### Scenario: Existing user unchanged
- **GIVEN** an existing user whose "Makanan & Minuman" category has no pillar
- **WHEN** the starter defaults are applied again
- **THEN** the category SHALL keep no pillar and no category SHALL be added
