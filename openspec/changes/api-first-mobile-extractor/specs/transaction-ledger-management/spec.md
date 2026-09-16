## ADDED Requirements

### Requirement: Flexible Transaction Creation Contract
The transaction creation API SHALL support both explicit UUID references and human-readable string references:
1. `account_id` OR `account_name` for source liquid cash or investment accounts.
2. `transfer_target_account_id` OR `target_account_name` for transfer destinations.
3. `category_id` OR `category_name` for spending/income classifications.
4. An optional `idempotency_key` (up to 64 characters) ensuring duplicate requests return the original transaction without double-counting ledger balances.

#### Scenario: Creating an expense using category name and account name
- **WHEN** client posts an expense with `account_name = "GoPay"`, `category_name = "Makanan & Minuman"`, and `amount = 25000`
- **THEN** server resolves the names to corresponding IDs, writes the expense to the ledger, and returns HTTP 201
