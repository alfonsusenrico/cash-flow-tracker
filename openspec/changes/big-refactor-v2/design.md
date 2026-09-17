## Context

Cash Flow Tracker currently models account-to-account movements as a single `type = "transfer"` transaction row with a `transfer_target_account_id` FK. This required special-casing in every query touching balances, P&L, and the ledger view. The user wants movements to be 2 independent ledger rows (outbound expense + inbound income) both categorised as "Internal Movement", making the accounting model uniform.

The current DB schema for `transactions` has columns: `id, user_id, account_id, category_id, type (expense|income|transfer), transfer_target_account_id, amount, notes, date, receipt_path, kakeibo_type, idempotency_key`.

The `accounts` table has `default_funding_account_id` (already present, used for investment accounts) but no `default_pocket_id` for routing unspecified mobile transactions.

The frontend `QuickCaptureModal` has a 4×4 calculator keypad and a "Pindah" (transfer) tab. It hardcodes `new Date().toISOString()` (UTC). The ledger edit modal uses `datetime-local` input but converts with `new Date(editDate).toISOString()` which silently converts local time to UTC, causing displayed times to shift by timezone offset.

The `SettingsModal` (256 lines) has no API key section; the backend already has `GET /auth/api-key` and `POST /auth/api-key/reset`.

The mobile companion app `NotificationDetailSheet.kt` (416 lines) is labeled "Label Ground Truth Dataset" and uses developer-jargon field names. It does not fetch accounts/categories from the server. It cannot patch an already-synced transaction.

---

## Goals / Non-Goals

**Goals:**
- Remove `transfer` type completely; replace with 2-row "Internal Movement" pattern.
- Add `POST /api/movements` atomic endpoint.
- Add `default_pocket_id` to accounts, use it in mobile sync routing.
- Add `PATCH /api/transactions/{id}` for mobile edit-and-resync.
- Expose `GET /api/accounts` and `GET /api/categories` with Bearer API key auth for mobile.
- Web-UI: remove keypad, fix datetime, remove Pindah Saldo, add API key management in settings.
- Mobile: rewrite notification detail as TransactionEditSheet with proper labels, Rp formatting, account/category dropdowns, and server PATCH on save.
- Move mobile sync button to accessible location.

**Non-Goals:**
- No multi-user or sharing features.
- No new account types or categories beyond what exists.
- No redesign of the feed page layout (user approved it).
- No change to Stockbit investment parsing logic.
- No change to Jago dual-pocket detection (it is the only valid dual-target notification and remains).

---

## Decisions

### D1 — Movement as 2 Transactions (not transfer type)

**Decision:** Remove `type = "transfer"` from the DB enum/check constraint. Add `POST /api/movements` that atomically inserts 2 rows: `(expense, source_account, "Internal Movement")` then `(income, target_account, "Internal Movement")`.

**Rationale:** Single-row transfers required every balance query to SUM over `transfer_target_account_id`, creating maintenance burden and risk of double-counting. Two uniform rows let all aggregations use the same simple pattern (`SUM WHERE type='income'`) without special cases.

**"Internal Movement" excluded from P&L:** Any query that computes net income/expense MUST additionally filter `WHERE c.name NOT ILIKE 'internal movement'` (or `category_id NOT IN (SELECT id FROM categories WHERE name ILIKE 'internal movement')`).

**Migration:** A one-time idempotent SQL script converts existing `transfer` rows to pairs. The script runs inside a transaction; if it fails the DB is unchanged.

```sql
-- Pseudocode for migration
BEGIN;
-- 1. Ensure "Internal Movement" categories exist for each user
INSERT INTO categories (user_id, name, kind, ...)
SELECT DISTINCT user_id, 'Internal Movement', 'expense', ...
FROM transactions WHERE type = 'transfer'
ON CONFLICT (user_id, name) DO NOTHING;

-- 2. Create expense rows from transfer rows
INSERT INTO transactions (user_id, account_id, category_id, type, amount, notes, date, idempotency_key)
SELECT t.user_id, t.account_id,
  (SELECT id FROM categories WHERE user_id=t.user_id AND name ILIKE 'internal movement' AND kind='expense'),
  'expense', t.amount, t.notes, t.date,
  'migrated-out-' || t.id::text
FROM transactions t WHERE t.type = 'transfer';

-- 3. Create income rows from transfer rows
INSERT INTO transactions (user_id, account_id, category_id, type, amount, notes, date, idempotency_key)
SELECT t.user_id, t.transfer_target_account_id,
  (SELECT id FROM categories WHERE user_id=t.user_id AND name ILIKE 'internal movement' AND kind='income'),
  'income', t.amount, t.notes, t.date,
  'migrated-in-' || t.id::text
FROM transactions t WHERE t.type = 'transfer';

-- 4. Delete original transfer rows
DELETE FROM transactions WHERE type = 'transfer';

-- 5. Update check constraint
ALTER TABLE transactions DROP CONSTRAINT IF EXISTS transactions_type_check;
ALTER TABLE transactions ADD CONSTRAINT transactions_type_check
  CHECK (type IN ('expense','income'));

-- 6. Drop transfer_target_account_id column (after migration)
ALTER TABLE transactions DROP COLUMN IF EXISTS transfer_target_account_id;
COMMIT;
```

### D2 — `default_pocket_id` Column on `accounts`

**Decision:** Add `default_pocket_id UUID REFERENCES accounts(id) ON DELETE SET NULL` to `accounts`. Only valid on parent accounts that have child pockets. Accounts without pockets are their own default.

**Routing in `_match_account_by_name`:** After resolving the `account_name` to a parent account, if `account.default_pocket_id IS NOT NULL`, substitute the resolved account with the default pocket for the final ledger row.

**UI:** Accounts page in web-UI gets a "Default Pocket" dropdown (only visible for accounts that have children). Mobile `TransactionEditSheet` uses server-provided account list (already hierarchical).

### D3 — Fix Datetime Serialization

**Problem:** `new Date(editDate).toISOString()` (called in ledger edit) shifts time by user's UTC offset. E.g., user sets `2026-09-17T10:00`, but `toISOString()` yields `2026-09-17T03:00:00.000Z` (WIB = UTC+7).

**Fix:** Convert `datetime-local` value to a proper offset string: use `new Date(editDate)` to get a Date object, then format with offset: append `+07:00` explicitly, or use a helper that reads `new Date().getTimezoneOffset()` and formats accordingly. Both the create and edit paths must use the same helper `localDatetimeToISO(str)`.

**QuickCaptureModal:** Add a `datetime-local` field defaulting to `format(new Date(), "yyyy-MM-dd'T'HH:mm")` (using `date-fns` already in the project). Submit via `localDatetimeToISO()`.

### D4 — Remove Calculator Keypad

**Decision:** Remove the entire 4×4 keypad block from `QuickCaptureModal`. The amount input field is already a `MoneyInput` text field. Removing the keypad simplifies the component by ~80 lines and the modal height shrinks.

### D5 — API Key Management in SettingsModal

**Decision:** Add a "Kunci API" (API Key) collapsible section to `SettingsModal`. Display the key prefix (e.g. `cfk_MeJl`) and last-used date. Button "Buat Kunci Baru" triggers confirmation dialog → calls `POST /auth/api-key/reset` → shows the new plaintext key once with copy button.

**Security:** The full key is only shown immediately after reset. Do not persist it; inform the user to save it immediately.

### D6 — Mobile TransactionEditSheet

**Decision:** Replace `NotificationDetailSheet.kt` with a new `TransactionEditSheet.kt`. Architecture:
- ViewModel fetches accounts (`GET /api/accounts`) and categories (`GET /api/categories`) on first open, caches in memory for session.
- Form fields: Transaction Name (notes), Amount (Rp-formatted), Type (income/expense), Account (dropdown), Category (dropdown), Date (parsed from notification or current time).
- If `notification.isSynced == true`: Show "Perbarui Transaksi" button → calls `PATCH /api/transactions/{serverId}`. The `serverId` must be persisted in the Room entity when sync succeeds.
- If `notification.isSynced == false`: Show "Kirim ke Tracker" button → normal sync flow.
- Remove all fields: Payload Hash, Copy Raw JSON, Pihak Lawan, "Label Ground Truth Dataset", "Simpan Ground Truth...".

**Room entity change:** Add `server_transaction_id TEXT` column to `NotificationEntity` to persist the returned `transaction_id` after successful sync.

### D7 — Mobile Sync Button Relocation

**Decision:** Move the sync `IconButton` from the `TopAppBar` trailing content to a `FloatingActionButton` (FAB) at the bottom-right of the feed screen, using a "sync" icon. This follows Material 3 patterns and is thumb-accessible. The FAB shows a loading spinner while sync is in progress.

### D8 — PATCH `/api/transactions/{id}` Endpoint

**Decision:** Add `PATCH /api/transactions/{id}` to `transactions.py`. Accepts a partial body: `type`, `amount`, `account_id`, `category_id`, `notes`, `date`. Authenticated by session cookie or Bearer API key. Only the owner can patch.

**Note:** An edit endpoint (`PUT`) already exists in `ledger/page.tsx` — confirm its path in `api.ts`. If it is `PATCH /api/transactions/{id}`, mobile can reuse the same endpoint. Check if it already accepts API key auth.

### D9 — Jago Dual-Pocket Exception Retained

**Decision:** The existing `NotificationProcessor.kt` rule `1a` (Jago pocket-to-pocket dual regex) is **kept unchanged**. When triggered, it will now build a movement request (`POST /api/movements`) instead of a `transfer`-type transaction. The `TransactionCreateRequest` class will be augmented with a `movementMode: Boolean` flag, or a separate `MovementRequest` data class will be used.

### D10 — "Internal Movement" in Income Category List

**Decision:** The backend `categories` table already has distinct `kind` column (`expense`/`income`). A category named "Internal Movement" will be seeded for both `kind = 'expense'` AND `kind = 'income'` for each user. The `QuickCaptureModal` and `TransactionEditSheet` both show the category list filtered by the selected transaction type, so "Internal Movement" will appear in both expense and income category dropdowns.

---

## Risks / Trade-offs

| Risk | Mitigation |
|------|-----------|
| Migration breaks existing balances | Run migration in a single atomic transaction; validate SUM(transfer amounts) == SUM(paired expense amounts) == SUM(paired income amounts) before committing |
| `transfer_target_account_id` column drop loses referential data | Migration preserves all data in new paired rows before drop |
| Timezone fix changes historical time display | Only affects new entries; old rows already stored in UTC |
| Mobile app must handle `PATCH` failures gracefully | Show error toast; keep local edit state so user can retry |
| Removing keypad breaks fat-finger users | Users still have native keyboard; mobile use case doesn't use web anyway |
| `default_pocket_id` circular reference | DB FK points child→parent direction; `default_pocket_id` on parent → child, which is fine. Validate in PATCH that `default_pocket_id` must be a direct child of the account |
