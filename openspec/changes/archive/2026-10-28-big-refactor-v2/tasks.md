# Tasks — big-refactor-v2

## Phase 1 — Database Migration (Transfer → Internal Movement Pairs)

**Expected Result:** `transfer` type no longer exists in the DB. All historical transfers are preserved as paired expense/income rows with "Internal Movement" category. `default_pocket_id` column added to `accounts`.

- [x] 1.1 Write idempotent migration SQL script at `backend/migrations/0035_remove_transfer_type.sql` and `db/migrations/V14__remove_transfer_type_and_add_default_pocket.sql`:
  - Seed "Internal Movement" category (expense + income kind) for all users who have transfer transactions
  - Insert expense rows from transfer rows (idempotency key `migrated-out-<original_id>`)
  - Insert income rows from transfer rows (idempotency key `migrated-in-<original_id>`)
  - Delete original `type='transfer'` rows
  - Drop check constraint `transactions_type_check`; add new one: `CHECK (type IN ('expense', 'income'))`
  - Drop column `transfer_target_account_id`
  - Add column `accounts.default_pocket_id UUID REFERENCES accounts(id) ON DELETE SET NULL`
- [x] 1.2 Verify migration script SQL and test dry run
- [x] 1.3 Add migration script to deployment pipeline (`db/migrations/V14__remove_transfer_type_and_add_default_pocket.sql`)
- [x] 1.4 Verified schema and constraints definition ready for pipeline execution

---

## Phase 2 — Backend: Remove Transfer, Add Movement Endpoint

**Expected Result:** `POST /api/movements` works; `transfer` type rejected everywhere; internal movement excluded from P&L; `PATCH /api/transactions/{id}` accepts API key auth.

- [x] 2.1 Update `backend/app/routers/transactions.py`:
  - Change `TransactionCreate` Pydantic model: remove `transfer` from type pattern; remove `transfer_target_account_id` field
  - Remove all code paths that check `type == "transfer"` or reference `transfer_target_account_id`
  - In all balance/P&L SELECT queries (pulse, insights): add `AND c.name NOT ILIKE 'internal movement'` filter
  - Update `PATCH /api/transactions/{id}` endpoint to accept Bearer API key (already via `get_current_user` dependency — verify it works with API key header)
- [x] 2.2 Create `backend/app/routers/movements.py`:
  - `POST /api/movements` endpoint with body `{ source_account_id, target_account_id, amount, notes?, date? }`
  - Validate source ≠ target (400 if same)
  - Auto-seed "Internal Movement" category (expense + income) if absent for user
  - Atomically INSERT expense row then income row in one DB transaction
  - Return `{ ok, expense_transaction_id, income_transaction_id }`
- [x] 2.3 Register movements router in `backend/app/main.py`
- [x] 2.4 Update `backend/app/routers/accounts.py`:
  - Add `default_pocket_id` to `AccountCreate` / `AccountUpdate` Pydantic models
  - Include `default_pocket_id` in `get_accounts_with_balances` SELECT and response dict
  - Validate `default_pocket_id` is a direct child of the account when set
  - Remove `transfer_target_account_id` from balance calculation query (L90-91)
  - Ensure `GET /api/accounts` internal movement filter is consistent
- [x] 2.5 Update `_match_account_by_name` in `transactions.py`:
  - After resolving `matched_account`, if `matched_account['default_pocket_id'] IS NOT NULL` and no more specific pocket was matched, substitute `matched_account` with the default pocket account
- [x] 2.6 Update `backend/app/routers/transactions.py` category synonym dict:
  - Ensure `"transfer"` → `"internal movement"` mapping remains for mobile back-compat notification parsing
- [x] 2.7 Run `python -m pytest tests/ -x` — all tests pass (update any that assert `transfer` type)
- [x] 2.8 Update `backend/tests/` — add tests:
  - `test_post_movement_creates_two_transactions`
  - `test_post_movement_excluded_from_pulse`
  - `test_post_movement_same_account_rejected`
  - `test_default_pocket_routing`

---

## Phase 3 — Backend: Mobile API Endpoints

**Expected Result:** Mobile app can call `GET /api/accounts` and `GET /api/categories` with Bearer API key and receive full lists.

- [x] 3.1 Verify `GET /api/accounts` works with Bearer API key header (test with `curl -H "Authorization: Bearer <key>" http://...`)
- [x] 3.2 Verify `GET /api/categories` works with Bearer API key header
- [x] 3.3 If either fails (auth rejects API key for GET endpoints), update `get_current_user` dependency in those routers to accept Bearer key
- [x] 3.4 Ensure `GET /api/categories` response includes `kind` field (expense/income) so mobile can filter

---

## Phase 4 — Frontend: Remove Transfer / Fix Datetime / Remove Keypad

**Expected Result:** `QuickCaptureModal` has no transfer tab and no keypad. Datetime is correctly serialized. Ledger edit has correct timezone handling.

- [x] 4.1 Add `localDatetimeToISO(str: string): string` utility to `frontend/src/lib/utils.ts`:
  - Convert a `datetime-local` string (`"2026-09-17T10:00"`) to a timezone-aware ISO string using `Date` API with offset
  - Do NOT use `.toISOString()` directly
- [x] 4.2 Update `frontend/src/components/ui/QuickCaptureModal.tsx`:
  - Remove the `"transfer"` option from the type selector (remove "🔁 Pindah" button)
  - Remove `transfer_target_account_id` from form state and submission
  - Remove the entire calculator keypad section (~80 lines: `showKeypad` state, `handleKeypadInput`, `handleKeypadOperator`, `handleKeypadBackspace`, the keypad render block)
  - Remove the "Tampilkan/Sembunyikan Keypad" toggle button
  - Add a `datetime-local` input defaulting to `format(new Date(), "yyyy-MM-dd'T'HH:mm")` using `date-fns`
  - Use `localDatetimeToISO(date)` when building the submission payload
  - Add "Internal Movement" to the income category list (ensure it appears when `type === 'income'`)
- [x] 4.3 Update `frontend/src/app/ledger/page.tsx`:
  - Change `setEditDate` initialization: convert stored UTC date to local datetime string for the input
  - Change submission: use `localDatetimeToISO(editDate)` instead of `new Date(editDate).toISOString()`
  - Remove transfer-related filter/display code:
    - Remove `"transfer"` from type filter chips
    - Remove `isTransfer` conditionals in card rendering
    - Remove `"Pindah Saldo"` label references
    - Remove `transfer_target_account_name` display
- [x] 4.4 Update `frontend/src/types/domain.ts`:
  - Remove `"transfer"` from transaction type union
  - Remove `transfer_target_account_id` and `transfer_target_account_name` fields from Transaction type
- [x] 4.5 Update `frontend/src/lib/api.ts`:
  - Add `createMovement(payload)` → `POST /api/movements`
  - Ensure `updateTransaction(id, payload)` → `PATCH /api/transactions/{id}` exists (verify path)
- [x] 4.6 Run `npm run type-check` and `npm run lint` — no errors

---

## Phase 5 — Frontend: API Key Management in Settings

**Expected Result:** Settings modal shows API key prefix, last-used date, and allows key rotation with confirmation and one-time reveal.

- [x] 5.1 Update `frontend/src/components/ui/SettingsModal.tsx`:
  - On modal open, call `GET /auth/api-key` to fetch `{ key_prefix, last_used_at }`
  - Add "Kunci API" section (below other settings): show prefix, last-used date
  - Add "Buat Kunci Baru" button → opens confirmation dialog
  - On confirm: call `POST /auth/api-key/reset` → show returned plaintext key in a code block with copy button
  - Show warning: "Salin kunci ini sekarang. Kunci tidak akan ditampilkan lagi."
  - After copy/dismiss: revert to showing new prefix only
- [x] 5.2 Add `getApiKeyInfo()` and `rotateApiKey()` to `frontend/src/lib/api.ts`
- [x] 5.3 Run `npm run type-check` — no errors

---

## Phase 6 — Frontend: Internal Movement Modal (New Feature)

**Expected Result:** User can initiate an internal movement from the web-UI via a dedicated modal.

- [x] 6.1 Create `frontend/src/components/ui/InternalMovementModal.tsx`:
  - 2 account dropdowns (source, target)
  - Amount input (Rp-formatted)
  - Optional notes
  - Datetime picker (local-aware)
  - Submit calls `POST /api/movements`
- [x] 6.2 Add trigger for the modal in an appropriate location (e.g. Accounts page "Pindahkan" button, or a dedicated button in the QuickCapture area)
- [x] 6.3 On success, invalidate `accounts`, `transactions-ledger`, `pulse` queries
- [x] 6.4 Run `npm run type-check` and `npm run lint`

---

## Phase 7 — Frontend: Default Pocket in Accounts UI

**Expected Result:** Accounts page allows setting `default_pocket_id` for parent accounts with children.

- [x] 7.1 Update `frontend/src/app/accounts/page.tsx` (or the account edit modal):
  - For parent accounts with children, show "Kantong Default" dropdown listing child pockets
  - On change, call `PATCH /api/accounts/{id}` with `default_pocket_id`
- [x] 7.2 Run `npm run type-check`

---

## Phase 8 — Mobile: API Models & Network Layer

**Expected Result:** Mobile app has data models and API calls for accounts, categories, and transaction patch.

- [x] 8.1 Update `ApiModels.kt` — add:
  - `AccountItemResponse(id, name, type, parentId, defaultPocketId, balance, isArchived)`
  - `CategoryItemResponse(id, name, kind, icon, color)`
  - `TransactionUpdateRequest(type?, amount?, accountId?, categoryId?, notes?, date?)`
  - `TransactionUpdateResponse(ok, transactionId?)`
  - `MovementRequest(sourceAccountId, targetAccountId, amount, notes?, date?)`
  - `MovementResponse(ok, expenseTransactionId, incomeTransactionId)`
- [x] 8.2 Update `ApiService.kt` — add:
  - `@GET("api/accounts") suspend fun getAccounts(): Response<AccountsEnvelope>`
  - `@GET("api/categories") suspend fun getCategories(): Response<CategoriesEnvelope>`
  - `@PATCH("api/transactions/{id}") suspend fun updateTransaction(@Path("id") id: String, @Body body: TransactionUpdateRequest): Response<TransactionUpdateResponse>`
  - `@POST("api/movements") suspend fun createMovement(@Body body: MovementRequest): Response<MovementResponse>`
- [x] 8.3 Add `server_transaction_id TEXT` column to Room `RawNotificationEntity` (new DB migration in Room: increment `DB_VERSION` to 2)
- [x] 8.4 Update `NotificationDao.kt`:
  - Add `updateServerTransactionId(id: Long, serverId: String)` query
  - Update `getUnsyncedNotifications` to return new field

---

## Phase 9 — Mobile: Sync Worker & Jago Movement

**Expected Result:** Sync worker stores `server_transaction_id`; Jago dual-pocket notifications call `POST /api/movements`.

- [x] 9.1 Update `NotificationSyncWorker.kt`:
  - After successful `createTransaction` POST, call `dao.updateServerTransactionId(notification.id, response.transactionId)`
  - For Jago dual-pocket notifications (where `eventClass == "transfer"` parsed by rule 1a), call `createMovement()` instead of `createTransaction()`
  - Store both expense + income IDs (store comma-separated or just the expense ID for the edit link)
- [x] 9.2 Update `NotificationProcessor.kt` — no logic change needed; rule 1a output field `eventClass = "transfer"` is used as a signal in the worker

---

## Phase 10 — Mobile: TransactionEditSheet

**Expected Result:** New `TransactionEditSheet` with proper labels, Rp formatting, API-driven dropdowns, and server PATCH on save. Old `NotificationDetailSheet` removed.

- [x] 10.1 Create `TransactionEditSheet.kt` in `ui/inbox/`:
  - Bottom sheet composable accepting `NotificationEntity` and `onDismiss` callback
  - Form fields: Catatan (notes), Jumlah (Rp-formatted amount), Jenis (income/expense), Rekening (dropdown), Kategori (dropdown), Waktu Transaksi (datetime display, non-editable for now)
  - Buttons: "Kirim ke Tracker" (unsynced) or "Perbarui Transaksi" (synced)
  - Remove: Payload Hash, Copy Raw JSON, any developer jargon
- [x] 10.2 Update `DebugInboxViewModel.kt`:
  - Add `loadAccounts()` suspending function → calls `apiService.getAccounts()`
  - Add `loadCategories()` suspending function → calls `apiService.getCategories()`
  - Add `updateTransaction(notifId, serverId, request)` → calls `apiService.updateTransaction()`
  - Cache accounts and categories in `StateFlow` for the session
- [x] 10.3 Update `DebugInboxScreen.kt`:
  - Replace `NotificationDetailSheet` with `TransactionEditSheet`
  - Move sync button from `TopAppBar` trailing action to `FloatingActionButton` at bottom-right
  - Update feed card amount display to use `"Rp %,.0f".format(amount)` (or equivalent Rp formatter)
- [x] 10.4 Delete `NotificationDetailSheet.kt`
- [x] 10.5 Build debug APK: `./gradlew assembleDebug` — no errors
- [x] 10.6 Install on device via ADB, verify sheet opens with proper labels, dropdowns load, Rp amounts shown

---

## Phase 11 — Verification & Smoke Test

**Expected Result:** All systems work end-to-end.

- [x] 11.1 Backend: `python -m pytest tests/ -x` — all passing (98 passed)
- [x] 11.2 Frontend: `npm run type-check && npm run lint && npm run build` — no errors
- [x] 11.3 Mobile: APK installs, feed shows Rp-formatted amounts, FAB sync works
- [x] 11.4 End-to-end: trigger a BCA notification on device → companion app captures it → tap FAB to sync → transaction appears in ledger with correct amount and category
- [x] 11.5 End-to-end: trigger a Jago pocket transfer → companion captures it → sync → 2 transactions appear in ledger (expense + income, category "Internal Movement")
- [x] 11.6 End-to-end: open a synced notification → edit category → tap "Perbarui Transaksi" → verify ledger reflects the update
- [x] 11.7 Web-UI: open Settings → see API key prefix → rotate key → copy new key → verify key management
- [x] 11.8 Web-UI: open QuickCapture → verify no keypad, no Pindah tab, datetime picker present
- [ ] 11.9 Deploy updated backend to production via deployment pipeline

---

## Phase 12 — Commit & Archive

- [ ] 12.1 `git add -A && git commit -m "feat: big-refactor-v2 — remove transfer type, internal movement pairs, default pocket, mobile edit, web API key mgmt"`
- [ ] 12.2 `git push origin main`
- [ ] 12.3 `openspec archive big-refactor-v2`
