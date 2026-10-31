## MODIFIED Requirements

### Requirement: Flexible Transaction Creation Contract
The transaction creation API SHALL support both explicit UUID references and human-readable string references.

#### Scenario: Required behavior and constraints
- **WHEN** a client submits transaction references and an optional retry identifier
- **THEN** the following required behavior and constraints hold:

  The transaction creation API SHALL support both explicit UUID references and human-readable string references:
  1. `account_id` OR `account_name` for source liquid cash or investment accounts.
  2. `target_account_id` OR `target_account_name` for the position or funding account of an investment trade. Internal movements between accounts are recorded through `POST /api/movements`.
  3. `category_id` OR `category_name` for spending/income classifications.
  4. An optional `idempotency_key` (up to 64 characters) ensuring duplicate requests return the original transaction without double-counting ledger balances.
  5. A string reference SHALL resolve only by the exact, owner-scoped rules of the idempotent-public-transactions requirement "Name-Based Entity Resolution on Transaction Creation"; an unresolved or ambiguous name SHALL be answered with HTTP 422 and candidates, never by creating an entity.

#### Scenario: Creating an expense using category name and account name
- **WHEN** client posts an expense with `account_name = "GoPay"`, `category_name = "Makanan & Minuman"`, and `amount = 25000`, and the owner has exactly one account and one expense category with those names
- **THEN** server resolves the names to corresponding IDs, writes the expense to the ledger, and returns HTTP 200 with the new transaction id
