## Context

Stock purchases through brokerage apps like Stockbit in Indonesia are funded directly from an associated Rekening Dana Nasabah (RDN, e.g., `RDN BCA`), while the resulting shares/units reside in the broker's investment account (e.g., `Stockbit`). Because Android push notifications from brokers contain only execution details (ticker, lot count, price per share) and omit bank/RDN information, automated transaction capture previously defaulted to attributing the debit to the investment account itself or was unable to identify which liquid bank account lost cash.

Users may have multiple RDN accounts or different funding bank accounts associated with various investment platforms (e.g., Stockbit with RDN BCA, Bibit with Bank Jago). This design establishes a dedicated relationship between investment accounts and their default funding cash accounts while keeping manual overrides trivial.

## Goals / Non-Goals

**Goals:**
- Provide a persistent link (`default_funding_account_id`) on any account where `type = 'investment'`, pointing to the user's cash/bank account (e.g., `RDN BCA`).
- When an investment execution notification arrives (e.g., Stockbit order match), the ingest router automatically uses `default_funding_account_id` as the transaction's cash account (`account_id`), decrementing the liquid cash balance.
- Attribute the transaction to the `Investasi` category (`is_excluded_from_budget = TRUE`, `kakeibo_type = 'saving'`) and link the destination investment portfolio account so daily living allowances are unaffected.
- Provide a user-facing selector in the web frontend's Account modal to configure and update the default funding account for any investment account.
- Allow 1-click reassignment of the cash account on both the web UI and mobile inbox for trades executed against an alternate funding account.

**Non-Goals:**
- Automated RDN balance scraping via private banking APIs (not feasible without banking credentials).
- Complex multi-currency FX conversion for offshore brokers (out of scope for local IDR accounts).
- Modifying non-investment account types (`bank`, `cash`, `wallet` do not require default funding accounts).

## Decisions

### 1. Database Model: Self-Referential Column on `accounts` Table
- **Decision:** Add `default_funding_account_id UUID REFERENCES accounts(id) ON DELETE SET NULL` to the `accounts` table.
- **Rationale:** Keeps the 5-table core data model intact without creating unnecessary join tables. If a funding account is deleted, the foreign key safely cascades to `NULL`.
- **Alternatives Considered:** 
  - *Separate `investment_settings` table:* Violates the strict 5-table architecture baseline established in `AGENTS.md`.
  - *Hardcoding in Python router:* Inflexible; prevents users with multiple RDNs from configuring their own source mapping.

### 2. Transaction Dual-Leg Attribution
- **Decision:** 
  - For a buy execution: `account_id` is set to `default_funding_account_id` (cash decreases), `type = 'expense'`, `category = 'Investasi'` (`is_excluded_from_budget = true`), and notes record `Stockbit: $TICKER ($LOTS lot @ Rp$PRICE)`.
  - If no `default_funding_account_id` is configured on the investment account, fallback to the investment account itself with a clear review prompt.
- **Rationale:** Ensures accurate liquid ledger balances for `RDN BCA` while preventing duplicate spending inflation or budget leakage.

### 3. Frontend UX in `AccountModal.tsx`
- **Decision:** Render a "Sumber Dana / RDN Default" dropdown only when `type === 'investment'`. The options list active accounts of type `bank`, `wallet`, or `cash`.
- **Rationale:** Intuitive progressive disclosure; users only see the funding account field when creating or editing an investment account.

## Risks / Trade-offs

- **[Risk] A trade was funded by a different bank or RDN than the default.**
  → *Mitigation:* The transaction card in the Activity Feed and the Mobile Companion feed explicitly displays the debited account with a 1-tap dropdown to reassign the source.
- **[Risk] User deletes or archives the linked funding account.**
  → *Mitigation:* The database foreign key uses `ON DELETE SET NULL`, and backend queries gracefully fallback to unassigned if the funding account is archived.

## Migration Plan

1. Execute schema migration on `accounts`:
   ```sql
   ALTER TABLE accounts ADD COLUMN default_funding_account_id UUID REFERENCES accounts(id) ON DELETE SET NULL;
   ```
2. Automatically backfill `Stockbit.default_funding_account_id` with user's `RDN BCA` account ID where applicable.
3. Deploy updated FastAPI backend and restart services.
4. Verify tests pass and test an order notification ingestion.
