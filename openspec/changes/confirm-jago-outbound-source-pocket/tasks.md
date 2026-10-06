# Tasks

## 1. Rebase and contract setup

- [x] 1.1 After owner approval, refresh the backend target branch and audit the next available Flyway/startup schema version, current deployed source, parser facts, account eligibility queries, and companion version/Room migration state; verify the audit is recorded in the implementation branch and does not alter unrelated working-tree edits.
- [x] 1.2 Create the backend topic branch and adopt this approved mobile contract into a native OpenSpec change in the companion repository; verify both native statuses show the approved artifacts before source edits.
- [x] 1.3 Define fictional fixtures for a Jago parent with main, Dana Darurat, and another child pocket plus named and unnamed outbound notifications; verify fixtures contain no production credentials or personal notification text.

## 2. Backend source-pocket hold

- [x] 2.1 Extend backend notification facts and Jago evidence classification to distinguish explicit source-pocket evidence from an absent source while preserving named movement, card-payment, incoming, and other-institution behavior; verify parser and classification tests cover both supported Jago package spellings.
- [x] 2.2 Add the pre-write source-pocket gate to deterministic acceptance, AI/leased application, fallback, manual resolution, retry, and existing-movement role confirmation; verify unanswered unnamed Jago events create no transaction, movement, reconciliation flag, consumed role, or balance change.
- [x] 2.3 Apply an accepted event-scoped source resolution consistently in context, interpretation validation, transfer signatures, and the shared application path; verify a selected child pocket is used and no alias/default is changed.
- [x] 2.4 Add the next versioned Flyway migration and mirrored startup migration for source resolutions and immutable answer receipts, with owner/event/question/reply uniqueness and safe upgrade behavior; verify upgrade preserves existing events, sender replies, transactions, and scheduled work.
- [x] 2.5 Implement owner-scoped source-pocket options and authenticated `confirm-source-pocket` API contracts, including no-preselection, complete eligible Jago list, stale/ineligible/conflict responses, idempotent replay, and cross-owner isolation; verify API tests exercise 401/403/404/409/422 and accepted queued/processing/recorded outcomes.
- [x] 2.6 Resume normal bilateral pairing only after source confirmation, using the selected source in both arrival orders; verify same-amount ambiguity, counterpart-during-hold, replay, and no-default-main cases with fictional database tests.

## 3. Android companion

- [x] 3.1 Implement typed `source_pocket` result/options/request models and notification routing, preserving sender/account question parsing and alert deduplication; verify unknown or malformed question payloads remain reviewable and never produce a recorded alert.
- [x] 3.2 Extend the existing durable reply outbox and Room migration with answer kind and account ID while retaining sender-row/worker compatibility; verify Room upgrade, duplicate reply identity, process restart, pairing change, and scheduled-work recovery tests.
- [x] 3.3 Add the notification action and inbox detail chooser with all current eligible options, no default selection, refresh/loading/error/offline/stale states, accessible radio semantics, and existing light/dark/enlarged-text behavior; verify Compose/UI tests cover selection, dismissal, stale refresh, and sender/account isolation.
- [x] 3.4 Implement typed worker delivery with current pairing/auth checks, bounded retry, authoritative response identity/hash validation, and result consumption; verify offline, 401/403, 409, 422, 429/5xx, and recorded-after-processing scenarios.
- [ ] 3.5 Adopt the approved companion OpenSpec tasks and build version 1.6.0/code 12 (subject to the version audit), preserving production URL, API-key reference, pairing identity, and rollback artifact; verify value-free installed metadata on the explicitly identified TECNO device only after backend release authorization.

## 4. Verification and release evidence

- [x] 4.1 Run targeted backend parser, application, API, lifecycle, pairing, and migration tests plus the full backend suite; verify `git diff --check` and no regression in named Jago, BCA sender, and existing notification flows.
- [x] 4.2 Run companion unit/instrumentation/lint/build checks and native OpenSpec validation for both coordinated changes; verify the normal app and verification app remain separated and no credential or raw notification content appears in logs/artifacts.
- [ ] 4.3 Run the fictional local-backend USB end-to-end test on TECNO serial `1694125629002315`, covering notification action, full pocket list, offline queued answer, restart, stale option, pairing preservation, and final recorded result; verify the second connected phone is untouched.
- [ ] 4.4 Push only clean topic branches and report their exact revisions for the owner to create and merge the backend PR; verify the designated deployment pipeline succeeds for the merged revision before any normal mobile installation.
- [ ] 4.5 After owner acceptance, record release/installation evidence and synchronize/archive the approved native changes; verify no historical incorrect transaction is rewritten and unresolved rollback behavior remains review/pending rather than defaulting to Jago main.

Executed evidence: 679 backend tests, 46 Android unit tests, 28 physical TECNO checks/phases, normal/verification builds, lint, strict native validation and diff checks passed. Task 4.3 exercised the complete fictional flow over the hardware-verified wireless transport after USB disappeared; USB transport is not claimed. Task 3.5 has a signed normal APK ready; its installation remains pending backend merge/deployment. See docs/jago-source-pocket-verification.md.
