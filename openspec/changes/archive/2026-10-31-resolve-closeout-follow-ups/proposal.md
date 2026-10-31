# Proposal

## Why

The six-change close-out of 2026-10-30 left owner decisions and follow-ups open: name-based API resolution that guesses and creates accounts and categories, two stale main-spec requirements (listener storage and the Outfit font), a reconciliation flag the web never shows, the owner's real name in tracked code and tests of a public repository, and one intermittent backend test.

## Approval

On 2026-10-31 the owner said "ok do all as recommend", approving the recommended resolution of the six follow-ups (C1, C2, C3, C4, C6, C7) as briefed by the coordinator. Recorded 2026-10-31.

## Expected Outcome

- A client that sends `account_name`, `target_account_name`, `source_account_name` or `category_name` gets the one exactly named entity it meant, or an HTTP 422 with up to five candidates. No account, pocket or category is ever created by a name.
- Main specs no longer state backend requirements for the phone's Room storage or name a font the app does not use.
- The owner sees "Perlu rekonsiliasi" on every account card and pocket that needs reconciling, on desktop and mobile, and can open "Sesuaikan Saldo" from it.
- Tracked code, tests, UI copy and current specs contain no real owner name, and owner self-transfer recognition still comes from the profile name and name aliases.
- The backend suite no longer fails intermittently because the app's background notification worker races tests that drive the worker themselves.

## What Changes

- **BREAKING (API, name callers only):** name resolution in `POST /api/transactions` and `POST /api/movements` becomes exact, owner-scoped and non-creating. Unknown or ambiguous names return HTTP 422 (`account_name_unresolved`, `category_name_unresolved`) with candidates instead of HTTP 400, a fuzzy match, a synonym match or an auto-created pocket or category. A name-resolved category must fit the transaction type, as an id already must. No caller in this repository or the Android companion sends names; see design.md.
- Retire the backend `notification-filtering` requirement "Discard Non-Financial Notifications at Listener Entrance" (owned by the companion's `notification-capture` spec).
- Correct the `tactile-pulse-ui` typography clause to Plus Jakarta Sans and Geist Mono, self-hosted.
- Show the reconciliation-required flag on the accounts page, with a warning icon and text, linked to "Sesuaikan Saldo".
- Replace the owner's real name in code comments, tests and UI copy with the synthetic name "Raka Purnama"; make the home greeting's loading fallback show no built-in name.
- Add `BACKGROUND_JOBS_ENABLED` (default true) and switch it off in the test harness, so the shared test app runs no background workers. Make one date-dependent insight test independent of the calendar day.

## Non-goals

- Changing the Android companion: it sends no names (read-only check at main 4a11eb0). No Android follow-up is required.
- New behaviour for typed text capture or notification processing: both resolve entities by id through their own deterministic code.
- Rewriting other stale clauses of `tactile-pulse-ui` or `idempotent-public-transactions` (for example the `transfer` type in "Idempotent Transaction Ingestion via Public API"); they are recorded as follow-ups.
- Archived change folders and dated research evidence: they are history and stay as written.
- Production access or release. Verification is local, on disposable PostgreSQL with synthetic data. Pushing the branch is authorised; opening and merging a pull request stays with the owner, and production release uses the approved pipeline.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `idempotent-public-transactions`: exact, owner-scoped, non-creating name resolution for transactions and movements, with coded 422 answers and candidates.
- `transaction-ledger-management`: the flexible creation contract refers to exact resolution and names the investment-trade target field correctly.
- `notification-filtering`: the listener-storage requirement is removed in favour of the companion spec.
- `tactile-pulse-ui`: the typography clause names the fonts the app self-hosts.
- `account-reconciliation`: the accounts page shows and links the reconciliation-required flag.

Standards: api-consumers (documented breaking change for name callers), money invariants (no entity is created or altered by a name, movement legs and default-pocket routing are unchanged), personal-data (no real names in tracked files), background-jobs (explicit, default-on switch), WCAG 2.2 AA for the changed UI. No project capability is added.

## Impact

- Backend: new `services/entity_names.py`; `routers/transactions.py` and `routers/movements.py` call it and drop the fuzzy resolver; `core/config.py` and `main.py` gain the background-jobs switch; `tests/conftest.py` turns it off; comment and test-data name changes in `notification_parser.py`, `notification_resolution.py` and tests. No migration.
- API: HTTP 400 "could not be resolved" becomes HTTP 422 with a coded detail; names no longer auto-create entities. `MovementCreate` name fields gain the same 100-character limit as transactions.
- Web: accounts page flag, `Icon` warning glyph, `ReconciliationFlag` component, greeting helper, Settings placeholder copy.
- Docs: README API note, `.env.example` switch.
- Android: none.
