# Tasks

Implementation starts only after owner approval of this coordinated plan. All boxes below are implementation/delivery work, not evidence that planning implements the feature.

## 1. Establish isolated implementation baselines

**Expected Result:** Both repositories have scoped native changes and topic branches without disturbing the existing label edits; relevant code and migration versions are current.

- [x] 1.1 Refresh the backend target and create an isolated topic branch/worktree carrying this approved change; record baseline SHA, next migration version, and preserved unrelated changes. Verify branch/worktree status and the relevant notification paths against refreshed main.
- [x] 1.2 Adopt this plan's mobile contract/design/tasks into the companion's native OpenSpec root using its generated workflow and CLI instructions before source edits; create a topic branch from its current accepted version. Verify native status, strict validation, and explicit linkage to this coordinated plan without changing scope.

## 2. Persist sender questions, answers, and memory

**Expected Result:** A populated database can retain owner-scoped questions and answers separately from model output, with exact sender alias keys and safe duplicate delivery.

- [x] 2.1 Add the next versioned Flyway migration and mirrored startup schema for active questions, event sender resolution, answer receipts, and sender aliases with ownership constraints. Verify empty-database migration, populated upgrade, repeat startup, and unchanged existing ledger/event rows on disposable PostgreSQL.
- [x] 2.2 Add exact mask normalization, alias lookup/conflict handling, and bounded name validation. Verify fixture tests for preserved mask positions, Unicode names, invalid input, cross-owner/account isolation, concurrent conflicting confirmations, and ambiguous-key behavior.

## 3. Integrate backend hold and answer lifecycle

**Expected Result:** Eligible masked transfers cannot reach the ledger before an answer; one accepted answer resumes the original event with its financial evidence intact.

- [x] 3.1 Add typed sender-question results and stable question identity, preserving legacy `mapping_proposal` and companion-version compatibility. Verify repeated lookup, sequential account/sender questions, malformed versions, old-client review, and upgraded-client retry contracts.
- [x] 3.2 Integrate the sender gate before immediate deterministic, leased AI, mapped-account, fallback, and manual write paths. Verify zero transaction/balance changes before answers, high-confidence AI cannot bypass the hold, and expenses/non-transfer income/unmasked senders/proven owned movements retain their existing behavior.
- [x] 3.3 Add `confirm-sender` with owner authorization, stable answer identity, row/advisory locking, atomic memory/answer/queue updates, and lease invalidation. Verify identical replays before/after recording, concurrent duplicate replies, lost-response retries, stale/conflicting replies, wrong-owner 404, invalid-input 422, and no partial persistence on failure.
- [x] 3.4 Preserve sender answers through account confirmation and processing retries; revalidate financial references on completion. Verify archived accounts, subsequent account questions, provider outages and fallback leave truthful results and never fabricate financial facts or duplicate a record.
- [x] 3.5 Add bounded matching sender context and update the central prompt; enforce confirmed/generic transfer wording at final recording. Verify mocked provider contract/evidence tests, prompt-like reply data, deterministic completion, future exact reuse, and separation from owner/internal-transfer identity. No paid general model benchmark is required by this change.

## 4. Provide reversible sender memory in Settings

**Expected Result:** The owner can see, correct, and remove sender memory without changing historical records.

- [x] 4.1 Add owner-scoped list/correct/delete sender-alias endpoints, including ambiguity state and account labels. Verify authorization, input validation, corrected future lookup, deletion prompting again, and unchanged accepted answers/committed transactions.
- [x] 4.2 Extend typed frontend API/query keys and the existing Settings modal with a compact separate sender section and correction/removal states. Verify component tests for loading/empty/error/ambiguous/edit/delete behavior and query invalidation; check rendered desktop/mobile, light/dark, focus, contrast and enlarged-text behavior using fictional names.

## 5. Deliver Android replies durably

**Expected Result:** The app can recognize sender questions and queue replies safely without losing them when the receiver exits or the network disappears.

- [x] 5.1 Extend API models and result validation for sender questions and revision-aware account questions. Verify valid/malformed/unsupported results, source payload/event/pairing correlation, legacy account actions and unchanged recorded-result validation.
- [x] 5.2 Add Room outbox/schema migration and safe retention for pending questions/replies; preserve pairing, event rows and alert receipts. Verify migration from populated current storage, restart recovery, duplicate enqueue identity, backup exclusions and no historical automatic replay.
- [x] 5.3 Add a reply worker using the existing authenticated API and completion path, with bounded retry, permanent-error recovery and pairing suspension. Verify mocked network/offline/5xx/401/403/409/422/lost-response cases, restart recovery, and no raw-event resubmission or premature recorded status.
- [x] 5.4 Add direct-reply and unknown-sender notification actions with explicit restricted receiver and unique event/question intents; persist answers before network work. Verify two-event routing, sequential-question alerts, stale actions, repeat-lookups, lock-screen/unlock behavior on API 31+ and the older supported fallback, and safe failure when notifications are unavailable.
- [x] 5.5 Add equivalent detail-sheet controls and truthful queued/sending/processing/error states; connect question-aware alert receipts and normal result polling. Verify UI tests for name/no-name, invalid/long names, dismissal recovery, disabled notification permission, enlarged text, focus/touch labels and one recorded alert. Do not add a separate TalkBack acceptance gate.

## 6. Verify the coordinated implementation

**Expected Result:** Automated checks and a USB/local-backend scenario prove the approved lifecycle, data preservation, and exactly one ledger record.

- [x] 6.1 Run affected backend tests and then the full suite against disposable migrated PostgreSQL using `TEST_DATABASE_URL`; run the CI-style preflight separately and report skipped DB coverage honestly. Verify existing account mapping, movement pairing, ingestion retries and sender regressions all pass without a stalled background test process.
- [x] 6.2 Run frontend tests, `npm run type-check`, `npm run lint`, and `npm run build`; verify Settings browser evidence from task 4.2 against the same source revision.
- [x] 6.3 Run companion `testDebugUnitTest`, `lintDebug`, and `assembleDebug --no-build-cache` with the repository JDK 17 setup; run relevant instrumentation including Room migration, workers, question UI and notification reply routing. Verify the intended APK version/build constants and manifest restrictions.
- [x] 6.4 Run an end-to-end USB-device check with the verification app ID and fictional local backend account: held masked transfer → inline name → one record → future exact reuse; repeat for explicit no-name, two concurrent transfers, account-then-sender, offline/process restart, stale answer, dismissal and pairing isolation. Verify backend state/ledger assertions and device UI, while keeping the installed normal app's production pairing unchanged.
- [x] 6.5 Review changed scope and privacy boundaries; run `openspec validate add-notification-sender-clarification --strict`, companion change validation and `git diff --check`. Verify no secrets/raw personal notifications in artifacts/logs and document actual passes, skips, unresolved risks and exact tested revisions in handoffs.

## 7. Release and owner acceptance

**Expected Result:** The compatible backend is deployed through the designated pipeline before the normal app upgrade; the owner receives exact artifact/evidence references and preserved configuration.

- [x] 7.1 After required checks pass and within release authorization, commit/push clean topic branches (backend remote; companion remains local if it still has no remote) and report the backend branch for owner-created/merged PR. Verify pushed SHA and CI for that revision; do not create or merge a PR or push directly to main.
- [ ] 7.2 After owner merge, verify the designated deployment workflow succeeds for the exact merged SHA before installing the normal companion upgrade. Record pipeline evidence only; no direct production-server access.
- [ ] 7.3 When install is authorized, retain a rollback APK, build/verify the normal companion version, and upgrade over USB with `adb install -r`. Verify installed version, row/config preservation and listener status without printing credentials or inspecting personal notification contents; remove only temporary verification artifacts within the approved scope.
- [ ] 7.4 Present the verified sender flow and remaining device/OEM limits for owner acceptance; update both handoffs and synchronize/archive accepted native changes using their current CLI instructions. Verify durable specs contain the accepted requirements and report any acceptance/release tasks that remain open.
