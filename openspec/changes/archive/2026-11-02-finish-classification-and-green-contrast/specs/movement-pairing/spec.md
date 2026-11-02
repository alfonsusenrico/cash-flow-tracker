## MODIFIED Requirements

### Requirement: Fee Recorded as a Separate Expense
When legs pair with a fee, the movement SHALL be created for the inbound amount, both legs SHALL be equal, and the fee SHALL be recorded as an expense on the source account, dated as the outbound leg, in the owner's "Biaya Admin & Transfer" category (counted in budgets), with source `movement`, classifier `system`, and a link to the movement. The fee SHALL take the category's pillar, or none when the category has none, never a fallback `need`.

#### Scenario: Balances after a fee pair
- **WHEN** a Rp 102.500 debit pairs with a Rp 100.000 credit
- **THEN** both legs SHALL be Rp 100.000, a Rp 2.500 fee expense SHALL exist on the source account linked to the movement
- **AND** the source account's total debits SHALL still be Rp 102.500.

#### Scenario: Fee category on first use
- **GIVEN** the owner has no "Biaya Admin & Transfer" category
- **WHEN** the first fee is recorded
- **THEN** the category SHALL be created as an expense category with pillar need, not excluded from budgets.

#### Scenario: Fee row cannot be merged
- **WHEN** the owner asks to merge a fee row with another transaction as a movement
- **THEN** the request SHALL be rejected with 409 and nothing SHALL change.

#### Scenario: Fee category without a pillar
- **GIVEN** the owner's "Biaya Admin & Transfer" expense category has no pillar
- **WHEN** a Rp 102.500 debit pairs with a Rp 100.000 credit
- **THEN** the Rp 2.500 fee SHALL be stored with no pillar, the category SHALL keep no pillar, and the Kakeibo breakdown SHALL report Rp 2.500 as `unclassified_spent` and nothing as `need_spent`.
