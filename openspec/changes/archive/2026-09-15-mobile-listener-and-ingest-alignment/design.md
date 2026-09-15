## Context

The system ingests mobile push notifications from `financial-tracker-mobile-listener` through `POST /api/ingest/notifications`. The mobile app acts as an on-device privacy gate that discards irrelevant apps and syncs batched notifications for 6 supported financial apps (`myBCA`, `BCA mobile`, `Bank Jago`, `GoPay`, `ShopeePay`, and `Stockbit`).
Recent backend changes introduced portfolio trade Kakeibo decoupling and hierarchical account/pocket models. However, the ingestion router (`backend/app/routers/ingest.py`) still assigns category-level `kakeibo_type` indiscriminately to all created transactions (even trades and transfers), and account resolution only queries top-level accounts without resolving child pockets (`parent_id`) for Bank Jago.

## Goals / Non-Goals

**Goals:**
- Decouple investment trades and transfers from Kakeibo pillars in `ingest.py` by persisting `kakeibo_type = None`.
- Enable Bank Jago child pocket resolution so that internal movements (e.g. Main Pocket to GoPay Tabungan) link `source_account_id` and `transfer_target_account_id` directly to their respective child pockets.
- Provide fuzzy and keyword-tolerant matching for child pockets (e.g. handling "Main Pocket", "Pocket", "GoPay Tabungan").
- Enforce the 6-app scope in backend test coverage, keeping Bibit deferred and Blu by BCA excluded as instructed by the user.

**Non-Goals:**
- Adding Bibit parser logic or notification listeners until actual notification samples are provided.
- Implementing Blu by BCA (explicitly excluded).
- Modifying the Android listener app's code, since its `FinancialSourceRegistry.kt` and `NotificationProcessor.kt` already filter and forward the correct 6 services.

## Decisions

### 1. Prefetch Child Accounts with `parent_id`
- *Decision:* Update account query in `ingest.py` from `SELECT id, name, type, default_funding_account_id FROM accounts` to `SELECT id, name, type, default_funding_account_id, parent_id FROM accounts`.
- *Rationale:* Ingestion processes notifications in batches. Pre-fetching `parent_id` allows in-memory mapping between parent institutions (e.g. Bank Jago) and their child pockets without issuing N+1 database queries.
- *Alternatives Considered:* Performing dynamic SQL lookups per notification. Rejected because batch ingestion performs much better with single in-memory lookups.

### 2. Multi-tier Child Pocket Name Resolution
- *Decision:* Implement `_match_pocket_account(pocket_name, child_accounts, parent_account)`:
  1. Exact string match against child account names (case-insensitive).
  2. Normalized match stripping the word "Pocket" (e.g. "GoPay Tabungan Pocket" matches "GoPay Tabungan").
  3. Substring containment match (e.g. "Jajan" matches "Kantong Jajan").
  4. Fallback to parent account if the name refers to "Main", "Utama", "Kantong Utama", or if no child pocket matches.
- *Rationale:* Push notification texts from Bank Jago vary between "Main Pocket Pocket", "GoPay Tabungan Pocket", and custom pocket names. Multi-tier resolution ensures high hit rates while cleanly falling back to the parent account if a specific pocket has not yet been provisioned in the ledger.

### 3. Explicit `kakeibo_type = None` for Trades and Transfers
- *Decision:* When assembling the `INSERT INTO transactions` payload:
  - If `is_investment` is `True` or `tx_type == "transfer"`: set `kakeibo_type = None`.
  - For normal expenses: retain `cat_res.get("kakeibo_type", "need")`.
- *Rationale:* Conforms directly to the `kakeibo-reconciliation` specification. Internal transfers and portfolio investments represent asset restructuring, not monthly living consumption.

## Risks / Trade-offs

- *[Risk]* User has Bank Jago pockets with identical names across different parent accounts.
  - *Mitigation:* Child pocket resolution filters specifically for `parent_id == matched_account["id"]`.
- *[Risk]* Target pocket not found in database for a Jago pocket transfer.
  - *Mitigation:* If target pocket is not resolved, fallback to recording as an internal movement with `transfer_target_account_id = None`, maintaining auditability without throwing a 500 error.
