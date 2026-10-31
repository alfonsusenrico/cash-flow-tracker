# Design

## Context

The close-out of six changes on 2026-10-30 (archived as `2026-10-30-*`) recorded these owner decisions and follow-ups:

- `enforce-financial-integrity-and-automation/design.md`: "Name-Based Entity Resolution" is contradicted by `_match_account_by_name`, which takes the first substring match and auto-creates a pocket under the parent or a "Jago" root, while `category_name` auto-seeds unknown categories. Main's text is also stale (`type == 'transfer'`).
- `notification-pipeline-reliability/design.md`: main `notification-filtering` still requires the phone not to "insert into Room", which the companion's three-outcome classifier contradicts.
- `modern-pop-ui-and-animations/design.md`: main `tactile-pulse-ui` names the Outfit font.
- `add-ai-notification-processing`: accounts carry `reconciliation_required`, but the web never shows it.
- `enforce-financial-integrity-and-automation/verification.md`: `test_jago_transfer_to_bca_pairs_with_the_bca_credit[False]` failed once in a full run and passed on rerun.
- The repository is public, and tracked code, tests and specs carried the owner's real name.

The owner approved the coordinator's recommendation for each item on 2026-10-31. This change implements them on `fix/closeout-follow-ups` from origin/main 02f4468.

## Goals / Non-Goals

Goals: exact, non-creating name resolution with clear 422 answers; specs that describe this repository's behaviour; a visible, accessible reconciliation flag; no real owner name in current tracked files; a backend suite without the background-worker race.

Non-goals: Android changes, archived folders, other stale spec clauses, production access.

## Decisions

### 1. Exact, owner-scoped, non-creating name resolution (C1)

A new service `app/services/entity_names.py` owns name lookup for `POST /api/transactions` (`account_name`, `target_account_name`, `category_name`) and `POST /api/movements` (`source_account_name`, `target_account_name`). The fuzzy `_match_account_by_name` and `_resolve_category_by_name` are removed from the router.

- Normalisation is trim, collapse whitespace and casefold. Nothing else: no "pocket"/"kantong" stripping, synonyms, substring or "main"/"utama" fallbacks.
- Candidate rows are the ones both endpoints already load: the requesting owner's non-archived accounts and categories. Ownership and archive filtering therefore come from the same query as before, and another owner's rows cannot match or be listed.
- Accounts: if exactly one row matches, it wins. Otherwise, if exactly one matching row is a pocket, the pocket wins (owner rule "prefer an exact pocket match"). Otherwise the name is ambiguous. A matched parent with pockets is still routed to its default pocket by the unchanged `resolve_effective_account`.
- Account names are unique per owner by `uq_account_user_name (user_id, name)`, which is case-sensitive. Normalised collisions therefore only come from case or spacing variants (for example "Cadangan" and "CADANGAN"), or from a name equal to another account's instrument symbol.
- Categories: if a name matches both kinds (for example "Internal Movement"), the transaction type picks the kind. A resolved category of the other kind is rejected with 400 exactly as `category_id` already is, except for investment trades. This closes a gap where a name could attach an income category to an expense.
- Unresolved answer: HTTP 422 with `detail = {code, field, reason, message, candidates}`, where `code` is `account_name_unresolved` or `category_name_unresolved`, `reason` is `not_found` or `ambiguous`, and there are at most 5 candidates (`id`, `name`, plus `parent_name` for accounts or `kind` for categories). For an ambiguous name the candidates are the matches, pockets first. For an unknown name they are the owner's names that contain or are contained in the query, or have a similarity ratio of at least 0.6, best first. Similarity is used only to suggest names, never to resolve one. Messages are Indonesian, like the other coded errors in these routers.
- `MovementCreate` name fields get `max_length=100`, the limit `TransactionCreate` already applies.

Rejected alternatives:
- Keep fuzzy matching and only stop auto-creation. It still records money against a guessed account.
- Case-sensitive exact matching. Clients and people vary case, and the DB constraint already prevents true duplicates.
- Returning 400 for unresolved names. 422 with a code matches the project's coded validation errors and lets a client tell "pick one" from a malformed request.

### 2. Exact instrument symbols stay an identifier (C1)

An account also matches when the name equals its instrument symbol, with or without the `.JK`/`.IDX` suffix (`BBRI` and `BBRI.JK` for "BBRI Stock"). The old resolver did this first. Investment trades address positions by ticker (`test_record_investment_buy_trade_by_account_names` sends `target_account_name = "BBRI"`). A ticker is an exact identifier, not a guess. It goes through the same ambiguity rule, so a symbol that equals another account's name is answered with 422. Trade-off: this is one deliberate widening of "exact name". Rejecting it would break ticker-addressed trades with no safety gain.

### 3. Caller inventory and compatibility (C1)

| Caller | Sends names? | Evidence | Action |
| --- | --- | --- | --- |
| Web frontend | No | `QuickCaptureModal`, `InvestmentTradeModal`, goals payment and `createMovement` post `account_id`, `category_id`, `target_account_id`, `source_account_id`. | None |
| Android companion (main 4a11eb0, read-only) | No | `IngestionApiService.kt` and `PairingApiService.kt` declare no `POST api/transactions` or `POST api/movements`. `categoryName`/`accountName` in `ApiModels.kt` and `CaptureModels.kt` are read-only response fields. `NotificationProcessor.kt` `categoryName` values are local labels. | None, so no Android follow-up |
| Text capture (`text_capture.py`, `capture_parsing.py`) | No | It matches mentions to account ids through its own deterministic parser and owner aliases, and records through ledger services. | None |
| Notification processing and application | No | It resolves ids through mapping, aliases and confirmation questions, then records through ledger services. | None |
| Telegram bot | Did, retired | Deleted in 45a11e3 (2026-10-28). Its tool prompt used `account_name`, `category_name`, `source_account_name` and `target_account_name`. | None, not deployed |
| README API example | Documents names | `account_name: "BCA"`, `category_name: "Gaji"` | Added the exact-match and 422 note |
| Unknown Bearer-key scripts | Unknown | Not observable from the repository | Listed under open decisions |

The default-pocket scenarios ("BCA" goes to the default pocket, "BCA Giro" to that pocket, "GoPay" to itself) still hold under exact matching.

### 4. Retire the listener-storage requirement (C2)

`notification-filtering` "Discard Non-Financial Notifications at Listener Entrance" is REMOVED with a Reason and Migration that point to the companion's `openspec/specs/notification-capture/spec.md`. That spec defines drop, keep and send outcomes and is tested in the companion repository. Every other requirement and scenario of `notification-filtering` stays, including "Bank Jago Pocket Movement Notifications", which the backend parser still implements. The removed requirement also held the example "Kabar baik buat Alfonsus!", which leaves with it.

### 5. Correct the font clause (C3)

`tactile-pulse-ui` "Anti-AI-Slop Visual Design Standard" is MODIFIED as a whole block. Only item 1 changes: Plus Jakarta Sans for interface text and Geist Mono for numeric code text, self-hosted per `web-font-hosting` (`frontend/src/app/layout.tsx`, `next/font/local`). Both scenario names are kept. The other items were not re-audited here.

### 6. Reconciliation flag on the accounts page (C4)

- A `ReconciliationFlag` component renders a warning-triangle icon (new `alert-triangle` in `Icon`) and the text "Perlu rekonsiliasi". Meaning never depends on colour alone. Styling follows the existing amber notice pattern (`border-amber-500/40 bg-amber-500/10`, text `var(--text)`), so text contrast is the normal body contrast in both themes.
- Placement: in the meta line of a flagged top-level card and of a flagged pocket row. `renderAccountGroup` serves both the desktop and the mobile list, so both get it. A parent card with flagged pockets shows "N kantong perlu rekonsiliasi", so the state stays visible when its pocket list is collapsed.
- On an item eligible for "Sesuaikan Saldo" (a leaf, not amount-tracked: the same rule as the options menu, now shared as `canReconcile`), the flag is a button that opens the existing reconciliation modal. Its accessible name is "Perlu rekonsiliasi: sesuaikan saldo <name>", which starts with the visible text (WCAG 2.5.3). On an ineligible item it is a plain label. The parent summary is a label.
- Target height is 32 px, above the 24 px minimum of WCAG 2.2 2.5.8. Focus uses the project's `focus-visible` outline.
- The flag clears through the existing path: a successful reconcile clears `reconciliation_required` server-side and invalidates the accounts query.

Rejected: a coloured dot or badge without text (fails 1.4.1); a page-level banner only (does not identify the account).

### 7. The owner's real name (C6)

Owner self-transfer recognition already comes from data: `notification_resolution.owner_aliases(profile)` reads `users.name` and `users.name_aliases` (Settings "Nama di Notifikasi Bank (alias)"). No code compared text with a hard-coded name, so no migration is needed and behaviour is unchanged.

| Location (at 02f4468) | Occurrence | Code relies on it? | Decision |
| --- | --- | --- | --- |
| `AGENTS.md:5` | Commit identity "Alfonsus Enrico <…>" | Git config | Kept: commit identity (git config), inside the managed profile block |
| `backend/app/services/notification_parser.py:102,109,116,165` | Sample notification wording in comments | No | "RAKA PURNAM" (bank-truncated) or "Raka Purnama" |
| `backend/app/services/notification_resolution.py:70` | Docstring example | No | "Raka Purnama" |
| `frontend/src/app/page.tsx:120` | Greeting fallback `"Enrico"` while the profile loads | Yes, a hard-coded name | Now from data: `greetingName(user)` capitalises the username as before and shows no name until it loads |
| `frontend/src/components/ui/SettingsModal.tsx:211,232` | Placeholder "Contoh: Enrico" and masked example "ALFO**US" | No | "Contoh: Raka Purnama", "RA*A PUR***" |
| `frontend/src/app/auth/login/page.test.tsx:23-35` | Registration test data | No | "Raka Purnama" / `raka01` |
| `backend/tests/test_notification_pairing.py` (17 lines) | Owner identity, masked and truncated forms, single-word alias | No | `OWNER = "Raka Purnama"`. Masked `R**A PU*`, truncated `RAKA PURNAM`, short token `Ra`, whole-word alias `RAKA` vs `RAKAMIN`, and the same dedup and alias semantics |
| `backend/tests/test_notification_parser.py` (7), `test_ingest.py` (2) | Parser examples | No | Synthetic equivalents with the same structure |
| `backend/tests/test_notification_processing_database.py:1029-1032` | Alias-driven recognition | No (test data) | Profile name "Pemilik Contoh" and alias "Raka Purnama", so the alias rather than the profile name still drives recognition |
| `backend/tests/test_clean_core.py:369,380`, `test_recurring_and_payroll.py:134` | Profile name and username examples | No | "Raka Purnama", `raka` |
| `openspec/specs/notification-filtering/spec.md:13` | Example in the requirement removed by C2 | No | Removed with the requirement |
| `research/evidence/2026-10-07-*.md` | `/Users/enrico/...` file paths in recorded commands | No | Kept: dated evidence and machine paths (see open decisions) |
| URLs, `alfonsusenrico` handle, `finance.alfonsusenrico.com` | Domain and handle | Config defaults | Kept, as briefed |
| `openspec/changes/archive/**` | History | No | Kept, as briefed |

### 8. Background jobs off in the test harness (C7)

Root cause, reproduced (see Hypotheses ledger): the session-scoped `TestClient` runs the app lifespan. That lifespan starts the real notification worker, which polls `claim_event` every second, plus the salary-detection, price-sync, movement-repair and plan-link loops. Tests that drive processing by hand (`ingest_with_source_confirmation` re-queues an event through confirm-source-pocket, then calls `claim_event` itself) race that worker. When the worker wins, the test's `claim_event()` returns `None`.

Fix in the owning component, the app's job start-up: a `BACKGROUND_JOBS_ENABLED` setting (default `true`, only the value `false` disables). When disabled, the lifespan starts no loop and logs a warning. `tests/conftest.py` sets it to `false`, next to the existing `PLAN_AUTO_LINK_ENABLED=false` set for the same reason. Accepted events stay durable and queued while jobs are off. `.env.example` documents that deployments keep it `true`.

Rejected:
- Retrying or sleeping in tests. That hides the race and is forbidden by the brief.
- Scoping `claim_event` to a user in tests. The worker would still process the event first.
- Disabling only the notification worker. The other loops also mutate the shared database on wall-clock timers (salary detection after 15 s, price sync after 10 s with an external quote call), which is the same class of hidden dependence.
- Refusing `false` in production. Deployments do not set it, and the startup warning makes a mistaken value visible. This is recorded as an open decision.

### 9. Calendar-dependent insight test (found while establishing the baseline)

On 2026-10-31, `test_observed_insights.py::test_comparison_endpoint_returns_its_windows` failed in 5 of 5 baseline full runs at 02f4468 and in 4 earlier runs. The test inserts spending "5 minutes ago" and expects it in the current month's comparison. On the 31st after a 30-day month (and on 29 to 31 March), the approved `observed-insights` rule shortens both windows to the previous period's length, so today falls outside the compared window. The product behaviour matches the spec, so the test is wrong. It now inserts at the midpoint between the month start and now, which is inside the compared window on every day. No product code changed. This is a test-only correction beyond the six items, made because it blocks the full-suite evidence today.

## Hypotheses ledger (C7)

The original failure's traceback was not kept (`-q` summary only, `close-changes/final-backend-db.txt`).

| # | Hypothesis | Experiment | Result |
| --- | --- | --- | --- |
| H1 | The lifespan notification worker claims the re-queued event between confirm-source-pocket and the test's own `claim_event` | Plugin `widen_window.py` sleeps 1.5 s after the confirmation (inside `count_transactions`) | Before the fix: 2 of 2 parametrisations fail with `TypeError: 'NoneType' object is not subscriptable` at `claimed["id"]`. After the fix: 2 of 2 pass. **Confirmed** |
| H2 | A leftover queued event from another test is claimed first (`claim_event` is global) | Count non-final events after full runs | 0 `queued`, `processing` or `failed` rows after repeated runs. **Rejected** as a current cause |
| H3 | A wall-clock background loop (salary detection at 15 s, price sync at 10 s) changes the pair mid-test | Inspection: same mechanism class as H1 | Not reproduced separately. Removed by the same fix |
| H4 | Parametrisation or order dependence (`[False]` only) | 50 isolated runs (100 executions) and 5 full baseline runs | 100 of 100 and 5 of 5 pass. No order effect. The `[True]` case is equally exposed (H1) |
| H5 | Fixed `post_time` dates against `NOW()` horizons | Inspection: the retry horizon uses `created_at`, not `post_time` | **Rejected** |

The natural window is a few milliseconds per manual claim against a 1 s poll, which explains about one failure in several full runs.

## Risks / Trade-offs

- Name callers outside the repository that relied on fuzzy matching or auto-creation now get 422. This is intended, and the response lists candidates. Rollback is a revert.
- `BACKGROUND_JOBS_ENABLED=false` in a deployment would stop notification processing. Mitigations: default true, warning at startup, `.env.example` note.
- Similarity suggestions might list an unrelated short name. They are only suggestions and never resolve.

## Migration Plan

No schema or data migration. Deploy the backend through the approved pipeline. No Android release is required. Rollback is a revert of the merge commit. Entities that the old resolver auto-created before this change are left as they are.

## Open decisions for owner

1. **External name callers.** Are there Bearer-key scripts outside the repository and the companion that post names? If so, they now receive 422 with candidates. No action was taken.
2. **Research evidence paths.** `research/evidence/2026-10-07-framework-adoption-*.md` contain `/Users/enrico/...` command paths, which reveal the macOS account name. They were kept as dated evidence. Say if they should be generalised.
3. **Production guard for the switch.** Should `APP_ENV=production` refuse `BACKGROUND_JOBS_ENABLED=false`, as it refuses insecure cookies? The current choice is a warning only.

## Follow-ups (not in this change)

- `idempotent-public-transactions` "Idempotent Transaction Ingestion via Public API" still lists a `transfer` type and HTTP 201. The API accepts `expense` and `income` and answers 200.
- `default-pocket` scenarios say "mobile sends `account_name`". The companion does not post transactions. The behaviour holds for any name caller.
- `tactile-pulse-ui` items 2 to 4 (surface shadows, accent hues) were not re-audited against `tokens.css`.
