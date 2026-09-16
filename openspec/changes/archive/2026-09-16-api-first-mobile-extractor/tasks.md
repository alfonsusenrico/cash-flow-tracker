## 1. Backend Database & Category Seeding

- [x] 1.1 Add `idempotency_key` column and unique index to `transactions` table in `backend/app/db/init_db.py`
- [x] 1.2 Add `"Internal Movement"` and `"Investasi"` to `DEFAULT_CATEGORIES` in `backend/app/db/init_db.py`

## 2. Backend Public API Enhancement

- [x] 2.1 Update `TransactionCreate` schema in `backend/app/routers/transactions.py` with `account_name`, `target_account_name`, `category_name`, and `idempotency_key`
- [x] 2.2 Implement idempotency check returning existing transaction with HTTP 200 if `idempotency_key` matches
- [x] 2.3 Implement account name resolution with case-insensitive matching, pocket synonyms, and auto-provisioning under parent institutions
- [x] 2.4 Implement category name resolution with automatic seeding of `"Internal Movement"` and `"Investasi"` if missing
- [x] 2.5 Add validation ensuring `transfer` has target account, returning HTTP 400 on unresolvable names

## 3. Mobile Companion App API Client & Extraction

- [x] 3.1 Add `createTransaction` endpoint to `IngestionApiService.kt` and `TransactionCreateRequest` DTO in `ApiModels.kt`
- [x] 3.2 Update `NotificationProcessor.kt` to extract transaction details and construct `TransactionCreateRequest` for all banking scenarios (Bank Jago single/dual pocket, GoPay, BCA, ShopeePay, Stockbit)
- [x] 3.3 Update `NotificationSyncWorker.kt` to execute `apiService.createTransaction(request)` and handle status updates
- [x] 3.4 Update mobile unit tests in `NotificationProcessorTest.kt`

## 4. Verification & Testing

- [x] 4.1 Write comprehensive unit tests in `backend/tests/test_transactions.py` covering name resolution, transfer linking, category auto-seed, and idempotency
- [x] 4.2 Run backend pytest suite and verify 100% pass
- [x] 4.3 Run mobile Gradle tests and verify 100% pass
- [x] 4.4 Run OpenSpec validate to verify change artifacts
