## 1. Backend Ingestion & Account Hierarchy Alignment

- [x] 1.1 Update accounts query in `backend/app/routers/ingest.py` to select `parent_id`
- [x] 1.2 Implement pocket matching helper `_match_pocket_account` with normalized string matching for child accounts
- [x] 1.3 Map Bank Jago pocket movements to child `source_account_id` and `transfer_target_account_id`
- [x] 1.4 Enforce `kakeibo_type = None` on ingested investment trades and transfer transactions

## 2. Automated Testing & Scope Verification

- [x] 2.1 Add test cases in `backend/tests/test_ingest.py` for Bank Jago child pocket-to-pocket transfers
- [x] 2.2 Add test assertions confirming `kakeibo_type` is None for Stockbit trades and internal transfers
- [x] 2.3 Verify test coverage across all 6 registered companion apps and confirm non-inclusion of Blu by BCA and Bibit
- [x] 2.4 Run full pytest test suite and execute `openspec validate mobile-listener-and-ingest-alignment`
