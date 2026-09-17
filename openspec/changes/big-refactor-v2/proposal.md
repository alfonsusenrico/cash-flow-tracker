## Why

The current system has grown organically and accumulated several design inconsistencies that create incorrect data, confusing UX, and maintenance burden:

1. **"Transfer" type causes broken ledger semantics.** The existing `type = "transfer"` transaction creates a single row with `transfer_target_account_id`, which causes special-casing throughout the codebase. The user now wants internal movements to be modeled as 2 independent ledger entries (outbound expense + inbound income, both with category "Internal Movement"), which makes balance calculation trivially correct and eliminates special-case logic.

2. **Web-UI input modal has no datetime picker.** `QuickCaptureModal` hardcodes `new Date().toISOString()` with no user-accessible date field. The edit modal in `ledger/page.tsx` has a `datetime-local` input but converts with `new Date(editDate).toISOString()`, which silently applies a browser GMT offset and saves the wrong time.

3. **Quick-capture has a 4×4 calculator keypad** that adds UI clutter without meaningful value. The user wants it removed.

4. **Inbound category list excludes "Internal Movement".** Mobile sync and manual web entry both cannot assign "Internal Movement" to income-type transactions. This makes the UI asymmetric with backend logic.

5. **API Key management is not surfaced in the Web-UI Settings modal.** The backend has `GET /api-key`, `POST /api-key/reset` endpoints in `auth.py`, but `SettingsModal.tsx` does not render or allow interaction with them.

6. **Mobile companion app has poor UX in the notification detail sheet.** It is labeled "Label Ground Truth Dataset" (confusing developer jargon), shows raw "Payload Hash" / "Copy Raw JSON" options, uses "Pihak Lawan" for the counterparty field, and amounts are raw integers. The sync button is buried in the top-right corner of the feed.

7. **Mobile companion does not allow editing synced transactions.** Opening a synced notification should allow the user to patch the already-recorded server transaction with corrected fields (account, category, amount, notes).

8. **No "default pocket" per account.** When a mobile notification does not specify a sub-pocket (e.g. a BCA debit that says only "BCA"), the system currently records it directly against the parent account, creating invalid ledger state for accounts whose balance must live entirely in child pockets.

---

## What Changes

### Backend
- **Remove `type = "transfer"` transaction type.** The check constraint on `transactions.type` and all code paths that handle `"transfer"` will be removed. Existing transfer rows will be migrated to paired income/expense rows via a one-time migration script.
- **Add `POST /api/movements` endpoint.** Atomically creates 2 transactions: outbound (expense) from source, inbound (income) to target, both with category "Internal Movement". Returns both transaction IDs.
- **"Internal Movement" category excluded from P&L.** Any transaction where `category.name ILIKE 'internal movement'` is excluded from expense/income aggregations in `GET /api/pulse`, `GET /api/insights`, and account balance queries.
- **Add `default_pocket_id` column to `accounts` table.** A nullable FK pointing to a child pocket. When set, mobile sync routes unspecified transactions to the designated pocket instead of the parent.
- **Expose `GET /api/accounts` and `GET /api/categories` endpoints for mobile.** Already usable by the web app; the mobile app needs to call them with an API key header to populate its dropdowns.
- **`POST /api/transactions` accepts `account_id` (UUID) in addition to `account_name`.** Mobile edit-and-resync should be able to pass explicit IDs.
- **`PATCH /api/transactions/{id}` for mobile edit-and-update.** Corrects existing server records when user edits a synced notification.

### Web-UI (Frontend)
- **Remove "Pindah Saldo" (transfer) type from `QuickCaptureModal.tsx` and `ledger/page.tsx`.** Replace with a separate "Internal Movement" button that opens a focused 2-field modal (source → target + amount).
- **Remove calculator keypad from `QuickCaptureModal.tsx`.** Keep only the text amount input.
- **Add `datetime-local` picker to `QuickCaptureModal.tsx`.** Default to current local time. Submit as ISO 8601 with local TZ offset preserved (do not call `toISOString()` which converts to UTC silently).
- **Fix datetime edit bug in `ledger/page.tsx`.** Use correct local-time-aware serialization.
- **Add "Internal Movement" to inbound category list** in `QuickCaptureModal.tsx`.
- **Add API Key section to `SettingsModal.tsx`:** show key prefix, last-used date, button to rotate (calls `POST /api-key/reset`), confirmation dialog before rotation.

### Mobile App (Kotlin / Jetpack Compose)
- **Rename and redesign NotificationDetailSheet** → `TransactionEditSheet`. Replace all developer-jargon labels with natural language. Remove "Payload Hash" and "Copy Raw JSON". Use "Pihak Pengirim / Penerima" or "Rekening Tujuan" instead of "Pihak Lawan".
- **Populate account and category dropdowns from the server API** (`GET /api/accounts`, `GET /api/categories`).
- **Format amounts as "Rp x.xxx.xxx"** throughout the sheet and feed cards.
- **On save of a synced transaction**, call `PATCH /api/transactions/{id}` with edited fields.
- **Move the Sync Now button** from top-right `IconButton` to a more thumb-accessible location (floating action-style or bottom bar button).
- **Remove "Simpan Ground Truth & Kirim ke Tracker" label.** Replace with "Simpan & Perbarui Transaksi" (if already synced) or "Kirim ke Tracker" (if not yet synced).

---

## Capabilities

### New Capabilities
- `internal-movement-api`: New `POST /api/movements` endpoint that atomically writes 2 transactions (outbound + inbound) for account-to-account movements.
- `default-pocket`: `default_pocket_id` column on `accounts`; backend routing logic uses it when `targetAccountName` is absent in mobile sync.
- `mobile-transaction-edit`: Mobile app `TransactionEditSheet` that fetches accounts/categories from API, allows editing all fields, and patches the server record via `PATCH /api/transactions/{id}`.
- `web-api-key-management`: `SettingsModal.tsx` section to view API key prefix, rotate key, and confirm rotation.

### Modified Capabilities
- `transaction-create`: Remove `transfer` type; update type enum to `expense | income` only. Add `internal_movement` category exclusion from P&L.
- `quick-capture`: Remove calculator keypad, remove Pindah Saldo tab, add datetime picker, fix timezone serialization.
- `ledger-edit`: Fix datetime GMT bug in edit modal.
- `mobile-feed`: Move sync button; update card UI to show Rp-formatted amounts.

---

## Impact

**Database:**
- `transactions.type` check constraint must be updated: remove `transfer`, keep `expense | income`.
- `transactions.transfer_target_account_id` column: remove (after migration).
- `accounts`: add `default_pocket_id UUID REFERENCES accounts(id) ON DELETE SET NULL`.
- Migration script: convert existing `transfer` rows → paired `expense`/`income` rows with category "Internal Movement".

**Backend files:**
- `backend/app/routers/transactions.py` — remove transfer type handling, add internal movement category exclusion, fix `create_transaction` and `update_transaction`, add `PATCH /api/transactions/{id}` endpoint.
- `backend/app/routers/accounts.py` — add `default_pocket_id` to schema and CRUD; update `get_accounts_with_balances` to exclude internal movement from P&L; add `GET /api/accounts/public` or reuse existing endpoint for mobile.
- `backend/app/routers/auth.py` — no changes needed (rotation endpoints already exist).
- New `backend/app/routers/movements.py` — `POST /api/movements`.
- `backend/app/main.py` — register new movements router.
- New DB migration SQL file.

**Frontend files:**
- `frontend/src/components/ui/QuickCaptureModal.tsx` — remove keypad, remove transfer tab, add datetime picker.
- `frontend/src/app/ledger/page.tsx` — fix datetime serialization in edit.
- `frontend/src/components/ui/SettingsModal.tsx` — add API key section.
- `frontend/src/lib/api.ts` — add movement endpoint, PATCH transaction endpoint.
- `frontend/src/types/domain.ts` — remove `transfer` from type union.

**Mobile files:**
- `NotificationDetailSheet.kt` → replaced / rewritten as `TransactionEditSheet.kt`.
- `DebugInboxScreen.kt` — move sync button, update card formatting.
- `DebugInboxViewModel.kt` — add `updateTransaction()`, `loadAccounts()`, `loadCategories()`.
- `ApiModels.kt` — add `AccountResponse`, `CategoryResponse`, `TransactionUpdateRequest`.
- `ApiService.kt` — add `getAccounts()`, `getCategories()`, `updateTransaction()`.
- New Room entity fields or caching for accounts/categories.

**Tests:**
- Update affected backend unit tests to remove `transfer` type assertions.
- Add tests for `POST /api/movements`, `PATCH /api/transactions/{id}`, `default_pocket_id` routing.
