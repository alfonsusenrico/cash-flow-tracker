## Why

Stock trading and automated investment notifications (such as Stockbit order executions) reflect securities purchases where liquid cash is deducted directly by the broker from a custodian/RDN bank account (e.g., `RDN BCA`), while the asset units/shares belong to an investment portfolio account (e.g., `Stockbit`). Because broker push notifications only contain order details (ticker, lot count, execution price) and omit the bank/RDN source, the system cannot deduce which cash account was debited. Furthermore, users may utilize different RDN accounts or funding banks across different investment platforms. 

By allowing investment accounts to specify a default funding source account, transactions ingested from investment notifications can automatically debit the correct liquid cash account while keeping the holding categorized under the investment portfolio, with quick 1-tap re-assignment for exceptions.

## What Changes

- **Schema & Model:** Add optional `default_funding_account_id` (foreign key to `accounts(id)`) to the `accounts` table, applicable to accounts of type `investment`.
- **Account API:** Update `AccountCreate`, `AccountUpdate`, and account response serialization in `backend/app/routers/accounts.py` to support `default_funding_account_id`.
- **Automated Ingestion Pipeline:** In `backend/app/routers/ingest.py`, when parsing financial notifications from investment platforms (e.g., `Stockbit`), resolve the cash source from the investment account's `default_funding_account_id`. Deduct liquid cash from the funding account while attributing the investment category (`Investasi`), ticker, lot count, and holding details to the investment portfolio.
- **Frontend Account Settings:** In account management modals (`AccountModal.tsx`), display a "Sumber Dana / RDN Default" dropdown selector when an account's type is `investment`.
- **Feed Source Reassignment:** In the transaction list and modal, ensure transactions generated from investment events clearly indicate the debited cash source and allow 1-tap source switching if a trade used an alternate RDN.

## Capabilities

### New Capabilities
- `investment-source-linking`: Associates investment accounts with a default liquid funding/RDN account so trade execution notifications automatically debit cash balances from the funding account, link holdings to the investment account, and support user-override switching.

### Modified Capabilities
None.

## Impact

- **Database:** Column `default_funding_account_id` (UUID, nullable, FK to `accounts.id` ON DELETE SET NULL) added to `accounts`.
- **Backend APIs:**
  - `GET /api/accounts`: Returns `default_funding_account_id` and nested `default_funding_account_name`.
  - `POST /api/accounts`, `PATCH /api/accounts/{id}`: Accepts `default_funding_account_id`.
  - `POST /api/ingest/notifications`: Uses `default_funding_account_id` as the debit account for investment executions.
- **Frontend Components:** `AccountModal.tsx`, `AccountsView.tsx`, and `TransactionModal.tsx`.
- **Dependencies:** None.
