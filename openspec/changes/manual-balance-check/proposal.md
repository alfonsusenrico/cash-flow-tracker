## Why

The research on redundant notifications (2026-11-04) recommended a balance audit as a safety net. It would catch what matching misses: a double booking, a phantom movement leg, a report the app skipped or held, or a transaction the phone never captured. The owner answered: "I think audit should be done by user manually through the web-ui anytime not scheduled". Today the only balance tool is "Sesuaikan saldo". It overwrites the gap with an adjustment and does not say where the gap came from, so the owner cannot tell a missed transaction from a duplicate.

## Expected Outcome

From any liquid account or pocket on Rekening & Kantong, the owner opens "Cocokkan saldo" and types the balance the bank app shows. The app shows the recorded balance, the difference, and the likely causes found in the ledger since the last matched point. Each cause links to its rows. When one cause or a small set of causes adds up to the exact difference, the app says so. The owner then:
- opens the rows to fix them;
- or corrects the balance with the existing "Sesuaikan saldo";
- or confirms "Saldo cocok", which becomes the starting point of the next check.

Nothing runs on a schedule, and nothing reminds the owner.

## Approval

**Owner direction, 2026-11-04.** The owner said "6. I think audit should be done by user manually through the web-ui anytime not scheduled" and later "do the complete implementation". That is taken as approval of this change. Every open decision (OD140–OD146 in design.md) takes its recommended default, and the defaults are listed for the owner at acceptance.

## What Changes

- **Check endpoint:** `POST /api/accounts/{id}/balance-check` is read-only. It compares a typed balance with the recorded one and returns deterministic cause candidates and, when found, the set that explains the difference exactly.
- **Matched points:** `POST /api/accounts/{id}/balance-matches` records a matched point only when the typed balance equals the recorded balance. Migration V48 adds `account_balance_matches`.
- **Web dialog:** a "Cocokkan saldo" dialog in the account detail, for standalone accounts, pockets, and parents whose pockets are all liquid.
- **After an adjustment:** "Sesuaikan saldo" keeps its contract. When the dialog opened it, a matched point is recorded after it succeeds.

## Non-goals

- No AI, no schedule, no reminders, and no automatic corrections.
- The check never changes the ledger. Only the owner's choice of "Sesuaikan saldo" does.
- No investment or amount-tracked products. Their values are valuations, not ledger balances.
- No change to the reconcile endpoint, the matching rules or the notification pipeline.

## Capabilities

- Modified: `account-reconciliation` (four added requirements).
- No new project capability. The money, database and personal-data standards apply. The check reads ledger rows and notification facts of one owner only, and logs no amounts.

## Impact

- **Backend:** `backend/app/services/balance_check.py` (new), `backend/app/routers/accounts.py` (two routes), and the tests.
- **Database:** `db/migrations/V48__account_balance_matches.sql`, additive.
- **Merge order:** a parallel change adds V47. This change must merge and deploy after it. If V48 reached production before V47, Flyway would refuse the later V47, because out-of-order migrations are disabled.
- **Frontend:** `frontend/src/components/accounts/BalanceCheckDialog.tsx` (new), `AccountsView.tsx`, `AccountDialogs.tsx`, `lib/queryKeys`, and the tests.
- **Production:** local verification only. Release goes through `deploy.yml` after the owner merges.
