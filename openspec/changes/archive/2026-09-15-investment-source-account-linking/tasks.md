## 1. Database Schema & Backend Account APIs

- [x] 1.1 Add `default_funding_account_id` column to `accounts` table in `init_db.py` and run migration on active Postgres DB
- [x] 1.2 Update `AccountCreate`, `AccountUpdate`, and queries in `backend/app/routers/accounts.py` to accept and serialize `default_funding_account_id`
- [x] 1.3 Update `get_accounts_with_balances` to return `default_funding_account_id` and joined `default_funding_account_name`

## 2. Ingest Router Routing & Investment Attribution

- [x] 2.1 Update `backend/app/routers/ingest.py` to check `default_funding_account_id` when processing investment notifications (e.g. Stockbit)
- [x] 2.2 Deduct cash from `default_funding_account_id` (e.g. `RDN BCA`), set category to `Investasi` (`is_excluded_from_budget = true`), and preserve holding notes
- [x] 2.3 Link `Stockbit` account to `RDN BCA` via `default_funding_account_id` in database

## 3. Frontend UI Configuration & Source Reassignment

- [x] 3.1 Update `AccountModal.tsx` to display a "Sumber Dana / RDN Default" dropdown when account type is `investment`
- [x] 3.2 Update account cards / list in `AccountsView.tsx` to display the linked default funding account
- [x] 3.3 Verify transaction detail modal and activity feed support 1-tap account switching for investment transactions

## 4. Verification & Testing

- [x] 4.1 Add automated pytest unit tests for investment account funding link configuration and notification ingest debit routing
- [x] 4.2 Run full backend test suite (`pytest tests/`) and frontend type-checks (`npm run type-check`)
- [x] 4.3 Validate change with `openspec validate investment-source-account-linking`
