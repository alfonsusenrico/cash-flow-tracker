## Why

The Android mobile companion app (`financial-tracker-mobile-listener`) reliably captures and batches notifications from 6 core Indonesian financial services (myBCA, BCA mobile, Bank Jago, GoPay, ShopeePay, Stockbit) to `POST /api/ingest/notifications`. However, the backend ingestion logic has fallen out of alignment with recent core architectural improvements:
1. **Kakeibo Pollution on Ingest:** Ingested investment transactions (e.g. Stockbit stock purchase/sale orders) and internal account transfers currently default to `kakeibo_type = "saving"` or `"need"`, violating the recent portfolio trade Kakeibo decoupling contract and artificially inflating monthly savings/spending metrics.
2. **Bank Jago Child Pocket Resolution:** When Bank Jago emits a pocket-to-pocket transfer notification (e.g. *"Rp 50.000 has been moved from your Main Pocket to your GoPay Tabungan Pocket"*), `backend/app/routers/ingest.py` queries only top-level accounts without resolving child pockets (`parent_id`), leaving `transfer_target_account_id = None` and breaking balance transfers across pockets.
3. **App Scope Enforcement:** Ensure that the ingestion pipeline and test fixtures strictly match the confirmed scope (maintaining the 6 active apps, deferring Bibit until real notification payloads are captured, and omitting Blu by BCA).

## What Changes

- **Kakeibo Decoupling on Ingest:** Update `backend/app/routers/ingest.py` so that all investment transactions (`is_investment=True` or `parsed.investment_action is not None`) and internal account transfers (`tx_type == "transfer"`) persist `kakeibo_type = None`, preserving 50/30/20 budget integrity.
- **Bank Jago Child Pocket Linking:** Enhance account prefetching and resolution in `ingest.py` to retrieve `parent_id` and match child pockets under the parent institution (Bank Jago). Accurately set `source_account_id` to the origin child pocket and `transfer_target_account_id` to the destination child pocket for pocket moves.
- **Robust Pocket String Matching:** Implement fuzzy and keyword-tolerant matching for pocket names (e.g., handling "Main Pocket", "Kantong Utama", "GoPay Tabungan", "Jajan") with graceful fallback to the parent account if no child pocket is found.
- **Comprehensive Pytest Verification:** Add test cases in `backend/tests/test_ingest.py` validating Bank Jago child pocket transfers, Stockbit trade ingestion with `kakeibo_type = None`, and verified handling across all 6 registered financial apps.

## Capabilities

### New Capabilities
- `notification-ingestion`: Automated ingestion and smart reconciliation of mobile push notifications into ledger transactions, child pocket transfers, and investment order entries with strict Kakeibo isolation.

### Modified Capabilities
- `kakeibo-reconciliation`: Ingested transactions from external notification channels (trades and transfers) must also enforce `kakeibo_type = NULL` to prevent monthly savings inflation.

## Impact

- **Backend:** `backend/app/routers/ingest.py` updated for pocket resolution and Kakeibo nullification.
- **Tests:** `backend/tests/test_ingest.py` expanded with Bank Jago child-pocket transfer and Kakeibo isolation test assertions.
- **Mobile Listener:** Confirmed aligned with current 6-app registry in `FinancialSourceRegistry.kt` (no mobile code modifications needed; Bibit deferred, Blu by BCA excluded).
