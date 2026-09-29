# Tasks

## 1. Backend: identity aliases and configuration

- [x] 1.1 Add Flyway `db/migrations/V21__user_name_aliases.sql` (`users.name_aliases TEXT[] NOT NULL DEFAULT '{}'`) and mirror it in `init_db.py`; verify with the isolated PostgreSQL test fixture applying V1–V21 (`backend/tests/conftest.py`).
- [x] 1.2 Extend `PATCH /api/auth/settings` and `GET /api/auth/me` with `name_aliases` (≤10 entries, each 3–150 chars, trimmed, deduplicated); verify with a database-backed round-trip and validation test.
- [x] 1.3 Add `notification_pairing_window_seconds` to `core/config.py` (env `NOTIFICATION_PAIRING_WINDOW_SECONDS`, default 900, bounds 30–86400, invalid → configuration error) and to `.env.example` and `.github/workflows/deploy.yml`; verify with a config unit test.
- [x] 1.4 Implement `owner_aliases(profile)` and `masked_name_matches(alias, candidate)` in `notification_resolution.py`; verify with unit tests for "ALFO**US ***ICO *O", "****LIN VALE**IA P" (no match), uppercase BI-Fast rendering, and short-token rejection.

## 2. Backend: pairing rework

- [x] 2.1 Use the configured window in `discover_candidates` and pass aliases into `load_context` (`self_identity_detected`, `[SELF]` redaction for every alias, masked included); verify with `test_notification_interpretation.py` context tests.
- [x] 2.2 Extend `transfer_signature` with the `irim` verb family, a `root_account_id`, and an `external_counterparty` marker; verify with unit tests on today's four real texts (rows 56, 58, 59, 60) and the GoPay "udah dikirim ke BCA <third party>" text.
- [x] 2.3 Relax `signatures_compatible` per design D2 (strict rule OR relaxed rule) and keep ambiguity flagging; verify with a database-backed test in `test_notification_processing_database.py` that pairs BCA "spent at Shopping" with ShopeePay "Pengisian saldo" 3 s apart, pairs Jago/ShopeePay 4 min apart in either order, refuses a third-party GoPay payment, and flags two candidates as `ambiguous_movement`.
- [x] 2.4 Update `evaluation/notification_cases.py` movement cases (`movement-29`, `movement-30`, boundary now the configured window) so the benchmark reflects the new contract; verify `test_notification_benchmark.py` passes.

## 3. Backend: fallback, failure states, manual resolution, split

- [x] 3.1 In `apply_claim`/`process_claim`, attempt `deterministic_interpretation` when the AI proposal is `needs_review` or the provider error is non-transient; record with `provenance_kind='deterministic_fallback'`; verify with a database-backed test using a stub provider that returns `needs_review` for a proven ShopeePay income.
- [x] 3.2 Rewrite `fail_claim` and `claim_event` per design D4 (`failed` + escalating `next_attempt_at`, auto-claim of `failed` events younger than 48 h with `attempt_count < 8`, `needs_review` only for evidence/mapping/model codes); verify with unit tests for the schedule and a database-backed test that a `provider_unavailable` event is retried and recorded after the provider recovers.
- [x] 3.3 Add `POST /api/ingest/notifications/{event_id}/resolve` per design D5 with owner scoping, kind/liquidity validation, idempotency, snapshot, and pairing pass; verify with database-backed tests for success, repeat idempotency, foreign event 404, and recorded event 409.
- [x] 3.4 Add `POST /api/movements/{movement_id}/split` per design D6 rejecting trades; verify with tests in `test_movements.py` covering category restoration and notification detachment.
- [x] 3.5 Run the full backend suite with the isolated PostgreSQL fixture; verify `pytest backend/tests` passes with zero failures and `git diff --check` is clean.

## 4. Backend: parser cue extensions and real corpus

- [x] 4.1 Extend `SETTLED_PATTERN`, `OUT_PATTERN`, `BALANCE_PATTERN` per design D7 and map myBCA `Account Transfer` outbound to a `Transfer Keluar` hint; verify with new cases in `test_notification_parser.py` and `test_notification_interpretation.py` (hypothetical Jago "You've sent" becomes a candidate; "remaining balance" second amount ignored).
- [x] 4.2 Add `backend/tests/fixtures/real_notifications.py` with the owner's captured texts (names replaced by a fixed alias) and a parametrised test asserting `collect_facts` status/direction/amount for each; verify the test passes.

## 5. Frontend

- [x] 5.1 Add a "Nama alias transfer" comma-separated field to `SettingsModal.tsx` wired to `name_aliases`; verify with a Vitest case that the saved payload contains the trimmed list and with `npm run type-check && npm run lint`.
- [x] 5.2 Add a "Pisahkan" action for non-trade movement rows in `ledger/page.tsx` calling the split endpoint with cache invalidation; verify with a Vitest case and manual check in the local Docker stack. Evidence: Vitest split case; split verified against the rebuilt local Docker API (categories restored, events reverted). Interactive browser check not run (no browser tooling available).

## 6. Mobile (`financial-tracker-mobile-listener`)

- [x] 6.1 Change `NotificationProcessor.processNotification` per design D1 (forward all, local drop only empty/OTP, hints only when matched, `sourceVersion = BuildConfig.VERSION_NAME`, remove hardcoded self-name checks); update DAO queries and inbox filters to include unrecognised rows; verify with `NotificationProcessorTest` cases for an unknown Jago outbound text (persisted, no hints) and an OTP text (dropped), plus `./gradlew testDebugUnitTest`.
- [x] 6.2 Add `ProcessingReasons` (Indonesian messages per `error_code`) and update `StatusBadge` (`failed` → "Diulang otomatis", unrecognised → "Tidak dikenali"); verify with a unit test that every documented backend code has a message.
- [x] 6.3 Add `retryNotification` and `resolveNotification` to `IngestionApiService`/`ApiModels`, a Retry action and manual-record form path in `TransactionEditSheet` for `needs_review`/`failed`, wired through `DebugInboxViewModel` with completion polling and alert delivery on success; verify with `NotificationResultContractTest` additions and a manual device check.
- [ ] 6.4 Build with `JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home ./gradlew assembleDebug`, keep the previous APK for rollback, install with `adb install -r`, confirm the listener reconnects (logcat) and the foreground notification is present; verify by simulating one unknown-format notification through the debug receiver and observing it reach the backend as reviewable. Built 1.2.0 (code 3) and staged; installation is deliberately held until the backend is deployed, because the forward-all app against the old backend would turn promotions into `needs_review`. Rollback APK saved. Non-UI instrumented tests passed on the device (10 passed, 1 skipped by design); the 5 Compose UI tests need the phone unlocked and have not run.
- [x] 6.5 Commit on a topic branch in the mobile repo with a subject-only message.

## 7. Release and monitored allocation run

- [ ] 7.1 Commit backend/frontend on topic branch `feat/notification-pipeline-reliability`, push, and report the branch for the owner's PR; verify CI passes on the pushed revision.
- [ ] 7.2 After the owner merges, confirm the GitHub Actions deployment succeeded and `/api/health` is healthy; owner sets aliases in Settings.
- [ ] 7.3 Owner re-runs allocations while the device DB, logcat, and `GET /api/ingest/notifications` are watched; for each unrecognised or `needs_review` event, add the institution pattern (task 4-style test first) in a follow-up commit; verify every allocation leg is recorded or paired and the unresolved list is empty.
- [ ] 7.4 Update `openspec` artifacts if decisions change, run `openspec validate notification-pipeline-reliability --strict`, and refresh `PROJECT_STATE.md`.
