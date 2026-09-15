# investment-source-linking Specification

## Purpose
TBD - created by archiving change investment-source-account-linking. Update Purpose after archive.
## Requirements
### Requirement: Investment Account Default Funding Source Configuration
The system SHALL support configuring an optional `default_funding_account_id` for any account where `type = 'investment'`:
1. The `default_funding_account_id` SHALL reference an existing, non-archived account owned by the same user.
2. The endpoint `GET /api/accounts` SHALL include `default_funding_account_id` and `default_funding_account_name` in the account response payload.
3. The endpoints `POST /api/accounts` and `PATCH /api/accounts/{id}` SHALL accept `default_funding_account_id`.
4. Deleting a linked funding account SHALL set `default_funding_account_id` to NULL without deleting the investment account.

#### Scenario: Configuring RDN BCA as default source for Stockbit
- **WHEN** user edits the "Stockbit" investment account and selects "RDN BCA" as the default funding account
- **THEN** system updates the account record and returns `default_funding_account_id` pointing to the RDN BCA account

#### Scenario: Clearing default funding account
- **WHEN** user clears the default funding account on an investment account
- **THEN** system updates `default_funding_account_id` to NULL

### Requirement: Automated Investment Notification Debit Routing
The system SHALL route financial trade notifications from investment brokers (e.g. Stockbit) based on funding account configuration:
1. If the target investment account has a non-null `default_funding_account_id`, the system SHALL set the created transaction's `account_id` to that `default_funding_account_id`.
2. The transaction `type` SHALL be recorded as `expense` for buy executions (`Pembelian Fully Match`) and `income` for sell executions (`Penjualan Fully Match`).
3. The transaction category SHALL resolve to `Investasi` (`is_excluded_from_budget = TRUE`, `kakeibo_type = 'saving'`).
4. If the target investment account has no `default_funding_account_id` configured, the system SHALL fallback to assigning `account_id` to the investment account itself.

#### Scenario: Stockbit buy execution routed to RDN BCA
- **WHEN** mobile companion pushes an order match notification for "Pembelian 10 lot BBRI match di harga Rp3.340" from Stockbit, and Stockbit's default funding account is set to "RDN BCA"
- **THEN** system records a transaction of Rp 3.340.000 debited from "RDN BCA", categorized as "Investasi", with daily living budget excluded

#### Scenario: Stockbit buy execution without configured funding account
- **WHEN** mobile companion pushes an order match notification from Stockbit, and Stockbit has no default funding account configured
- **THEN** system records the transaction on the "Stockbit" account directly with category "Investasi"

### Requirement: Interactive Funding Account Reassignment
The system SHALL allow users to easily reassign the funding account of an ingested investment transaction:
1. In the Web UI activity feed and transaction modal, the transaction source account SHALL be editable with a single selection.
2. Updating the transaction's `account_id` SHALL adjust ledger balances immediately so the newly chosen account reflects the debit.

#### Scenario: Switching debited account from RDN BCA to RDN Jago
- **WHEN** user opens an ingested Stockbit transaction and changes the account from "RDN BCA" to "RDN Jago"
- **THEN** system updates the transaction `account_id` to RDN Jago, restoring Rp 3.340.000 to RDN BCA and deducting Rp 3.340.000 from RDN Jago

