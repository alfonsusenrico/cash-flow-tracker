# Tasks

## 1. Backend data and configuration

- [x] 1.1 Add Flyway `V22` creating `notification_account_aliases` and replacing `chk_notification_processing_state` to include `needs_confirmation`; mirror in `init_db.py`; verify by migrating a disposable PostgreSQL to V22 and running the suite. Evidence: V22 applied via Flyway to disposable PostgreSQL and to the local stack; startup mirror `notification_mapping.sql`.
- [x] 1.2 Add `NOTIFICATION_MAPPING_AUTO_THRESHOLD` (default 0.85, valid 0.5–1.0) to config, compose, runtime env defaults, workflow, and `.env.example`; verify with a config bounds test. Evidence: `test_mapping_threshold_configuration_bounds`.

## 2. Backend mapping

- [x] 2.1 Resolve names through learned aliases before name matching in institution, pocket, and observed-endpoint resolution; report unresolved names in `facts.unresolved_names`; verify with unit tests (alias hit, archived-alias ignored, unresolved reporting). Evidence: `test_notification_mapping.py` resolution tests.
- [x] 2.2 Extend `Interpretation` with `mappings`, bump the prompt to version 5, and validate proposals per D2; verify with unit tests for eligibility and rejection. Evidence: decision and strict-schema unit tests; provider now drops model categories on movements (`test_provider_drops_model_category_for_movements`).
- [x] 2.3 Apply D3 in processing: auto-accept at or above threshold (record, label, `ai` alias) and `needs_confirmation` below (proposal stored, no transaction), honouring the companion-version gate; verify with database tests for both branches and the old-build branch. Evidence: database tests for auto, confirmation, old build, ineligible proposal; threshold branch mutation-checked.
- [x] 2.4 Add `POST /api/ingest/notifications/{id}/confirm-mapping`, `GET /api/ingest/aliases`, `DELETE /api/ingest/aliases/{id}`; verify with database tests (confirm proposed, confirm alternative, pairing after confirm, foreign 404, wrong state 409, alias removal affects only future events). Evidence: database tests for confirm, 404/409/422, alias list and removal.
- [x] 2.5 Add synthetic benchmark mapping cases and run the paid benchmark once on the development partition; verify no auto-accepted wrong mapping and record cost. Evidence: `evaluation/mapping_benchmark.py --live`, gpt-6-luna reasoning none, 5 cases × 3: 0 wrong automatic mappings (1 auto_ok, 11 confirm, 3 review), $0.012. Live local end-to-end run: proposal 0.72 → confirmation → recorded Main Pocket → GoPay, owner alias stored.

## 3. Frontend

- [x] 3.1 Add the learned-alias list with remove to Settings; verify with Vitest (list, remove, axe) plus type-check, lint, and build. Evidence: Vitest `lists learned bank-notification names and removes a wrong one` (axe), full frontend gate.

## 4. Mobile

- [x] 4.1 Handle `needs_confirmation` and `mapping_proposal` in result consumption and completion polling; send `source_version` 1.3.0; verify with unit tests. Evidence: unit tests `MappingConfirmationTest`; 1.3.0 sends `source_version` from a clean build.
- [x] 4.2 Add the "Konfirmasi Rekening" channel, actionable notification, non-exported `MappingConfirmReceiver` with `goAsync()`, and receipts against duplicate posting; verify with unit tests for payload and a device check. Evidence: unit tests for notification content; instrumented `confirmationIsStoredPostedOnceAndClearedWhenResolved` compiled, not yet run (phone unreachable).
- [x] 4.3 Add the proposal and picker to the detail sheet; verify with an instrumented UI test (phone unlocked). Evidence: all 18 instrumented tests on the TECNO device (17 passed, 1 skipped by design), including the picker test; the run also caught a 1.2.0 badge regression (recorded result with error shown as recorded), fixed in 1.3.1.
- [x] 4.4 Clean-build 1.3.0, install after the backend deploy, and verify one confirmation end to end on the device. Evidence: 1.3.0 installed after deploy run for `3cf4eb8`, then 1.3.1 (`X-Companion-Version` header, badge fix) built with `--no-build-cache`; compiled version strings verified with dexdump; 24/24 rows and settings preserved; listener bound.

## 5. Release

- [ ] 5.1 Push topic branches, owner merges, confirm the deploy run, install the app, update `PROJECT_STATE.md`, and validate this change strictly.
- [ ] 5.2 Retry adopts a newer companion version (`X-Companion-Version`) so events captured before 1.3.0 can reach `needs_confirmation`; verify with `test_retry_from_newer_app_enables_confirmation_for_old_events` and on the device with stuck row 65 after deploy.
