Release direction: the owner authorized backend main push and mobile install, with AI benchmarks deferred and operational AI disabled.

# Design

## Context

See `proposal.md` for motivation. The backend's approved active change provides per-event compact states and `GET /api/ingest/notifications/{event_id}/result`; recorded results follow commit, not inference. Its final held-out quality gate and final artifact smoke remain open. The companion at `6a542b4` marks an entire successful HTTP batch synced, prunes to fifteen rows before processing results, lacks result polling, and stores logical keys in a field passed to a transaction UUID route.

Installed companion versions are Kotlin/JVM 17, Room 2.6.1, WorkManager 2.9.1, Retrofit 2.11.0, and Android minimum API 26. Reuse these; this is not a platform upgrade. The current phone retains fifteen BCA/Jago notifications, all transport-synced. Do not erase or automatically replay them.

The sibling companion has no native OpenSpec root or local handoff. This change records coordination in the existing backend planning home. Approval must explicitly include initializing/adopting the mobile plan in the companion root; native apply there and host filesystem approval are required before source edits. No registration of a global store or change to workspace security is proposed.

## Goals / Non-Goals

**Goals:** Recoverable completion tracking with truthful status, stable logical alert identity, safe edits, additive local migration, and local device evidence after the backend is settled.

**Non-Goals:** Redesigning the companion UI, changing selected-package filtering/on-device parsing, enabling production AI, modifying model choice, adding an AI agent, historical replay/cleanup of existing user data, and guaranteeing a financially correct proposal merely because a dry run succeeds.

## Decisions

### 1. Backend first and minimal additive edit identity

Complete the approved backend regression, development tuning/held-out gates, and final isolated artifact ingestion smoke before mobile integration. Keep operational AI disabled while gates fail; do not spend through the $5 budget on repeated broad runs. Bounded synthetic calls retain usage and conservative caps; real owner messages remain local dry-run-only. Mobile/mock test harnesses require no paid model calls.

Extend compact result construction with optional `transaction_id` only after an owner-scoped check confirms that the current anchor is an ordinary expense/income without movement/trade semantics. Existing recorded logical results remain recorded after deletion but expose no editable UUID. Keep `record_key` stable and independent. Do not parse `notification:<event UUID>` into a transaction UUID; the IDs refer to different entities. Do not expose an arbitrary movement leg as editable.

Alternative rejected: removing all mobile editing or continuing to put logical keys into the PATCH path. The former loses a working feature for genuine ordinary UUIDs; the latter violates the API contract.

The existing main notification spec still contains historical parent/primary-account fallback and legacy transfer representation. The approved AI ingestion delta supersedes those requirements; this companion change adds an identity requirement without reintroducing those fallbacks or duplicating the active AI delta.

### 2. Persist transport and completion separately

Add nullable/defaulted Room fields for backend event ID, processing status/error, logical record key, actual editable transaction ID, and a local opaque pairing context identifier. Add a small logical alert delivery table unique on `(pairing_context, record_key)`, with the latest compact result and pending/delivered state. Do not store the API key or raw message in WorkManager input/output or that delivery table.

Keep existing `is_synced` as transport acceptance for compatibility, not recorded success. Correlate a batch result to the matching payload hash; verify accepted IDs and supported states. Apply updates and pending alert claims atomically in Room. Partial response failures remain pending with safe errors even when other events were accepted. Ignored prefilter results may lack event IDs and require no polling. Unknown/malformed responses never fabricate success.

A local pairing generation changes only when URL/credentials change; never print credentials or use their plaintext as an identifier. Accepted events from older pairings are held, not sent to a newly configured server. Legacy synced records without event IDs remain historical/unresolved, not automatically submitted again.

### 3. Bounded durable WorkManager coordination

Retain the existing submission worker and periodic recovery trigger. Add one focused completion worker, sharing the same result-consumption path as immediate ingestion results. Discover at most fifty due accepted queued/processing events per submission/recovery run. Each event has unique completion work and persisted next-check metadata; a process-wide mutex serializes completion HTTP requests with existing timeouts. Use persisted next-check metadata and bounded exponential intervals, initially 10 seconds and capped at 15 minutes. WorkManager scheduling is best-effort, not a real-time guarantee.

Use non-cancelling unique-work coordination rather than replacing an in-flight upload on every captured notification. A worker drains bounded pages and schedules a durable follow-up if more work remains; periodic recovery closes process/scheduling races. Avoid unbounded appended chains or sleeping workers. Transient network/429/5xx failures retain accepted state and retry later; auth failure or foreign/not-found response suspends that pairing/event with a safe error instead of hammering or replaying it. Propagate coroutine cancellation rather than turning it into a generic retry/error log.

Terminal review/failed results do not automatically call backend retry. A manual sync may refresh such stored event results to detect an explicit backend repair/retry; it does not itself enqueue financial reprocessing. A terminal ignored result needs no further work.

Official guidance: [WorkManager conflict policies](https://developer.android.com/develop/background-work/background-tasks/persistent/how-to/manage-work) documents that `REPLACE` cancels existing work. Use the installed APIs and test the selected coordination policy against arrival-during-submit and process interruption.

### 4. Idempotent logical alert delivery, not impossible atomicity

Validate compact recorded data before notification construction: supported type, nonblank key/description, positive whole-IDR amount, supported currency, and required owned endpoint(s). Use receiving `target` for income, source for expense, and source/target for movement. Keep current branding/channel and concise financial content; do not invent category or zero amount.

Use `NotificationManager.notify(record_key_tag, stable_id, notification)` for deterministic logical identity and `setOnlyAlertOnce(true)`. A Room delivery claim prevents concurrent callers from deliberately posting two alerts. Persist completion after successful OS submission; permission/channel blocking retains pending delivery. Restart recovery can idempotently upsert the same active notification. Update delivered content silently only if that notification remains active; do not recreate a dismissed item solely because a second movement leg arrives. Retain delivery receipts independently of the fifteen-row raw-event retention window.

Room and NotificationManager cannot commit atomically. There is a narrow crash window after OS submission but before marking delivery; guarantee stable active identity and recovery, not mathematically exactly-once audible delivery. Tests must cover that interruption rather than claiming it cannot occur.

Alternative rejected: payload-hash notification identity, because two notifications for one movement have different hashes. Another rejected alternative is marking delivery before OS submission, which can lose an alert permanently after a crash.

### 5. Additive migration and truthful existing controls

Export Room schemas and provide explicit supported upgrade paths, including populated v2 and the previously supported v1 schema. Preserve all original notification fields and credential preferences. Classify historical `server_transaction_id` values by complete valid UUID versus opaque logical key; never infer missing backend event IDs. Seed historical logical delivery receipts conservatively to avoid retroactive alerts. Remove destructive fallback; unsupported migrations fail without wiping data.

Prune only resolved rows with no pending alert, after result handling; retain queued/processing/review/failed/legacy-unresolved rows. Existing status badges distinguish submitted/processing from recorded. Existing edit controls require an actual validated ordinary UUID; movement/trade/legacy keys show a concise unavailable or edit-in-web option without passing tokens in URLs. Label changes remain inspection metadata, not proof of a committed ledger edit. Verify affected controls at increased font/display size; the owner has declined TalkBack acceptance for this change. Preserve the established layout.

Official guidance: [Room migration testing](https://developer.android.com/training/data-storage/room/migrating-db-versions) documents migration verification and warns that destructive fallback deletes stored table data. Use populated migration tests and device row/checksum checks before and after installation.

### 6. Close observed privacy/debug boundaries

Remove logs containing notification title/body, alert content, unbounded server bodies, or sensitive exception messages/traces. Expose only bounded status/error codes and counts. Keep production HTTP body/header logging disabled; do not embed tokens in diagnostics or testing artifacts.

Exclude the actual `financial_tracker_prefs.xml` and database directory from cloud backup and device transfer. Preserve authorized on-device runtime credentials; a broad new secret-storage framework is not part of this change. Remove the exported simulation/purge receiver from release/main; any debug harness must be package-local/non-exported or instrumentation-only. Device tests can inject fictional events through the debug instrumentation process, not an unrestricted external broadcast.

Alternative rejected: keeping the receiver publicly exported behind a runtime debug boolean, because the release manifest and another app still present a trust boundary.

## Risks / Trade-offs

- [Backend probabilistic quality still fails] → Backend-first acceptance gate; report failures and keep AI disabled rather than marking mobile end-to-end readiness prematurely.
- [Owner $5 total API budget] → No paid calls for mobile tests; bounded explicitly cost-capped backend comparisons and billing-page authority for total spend.
- [Phone disconnects or Android defers jobs] → Persist work/state; retain listener foreground behavior and periodic recovery; report actual measured completion latency rather than promising instant execution.
- [Legacy records lack event IDs] → Hold and preserve, never auto-replay; ordinary old UUIDs remain distinct from logical keys.
- [Migration regression] → Populated v1/v2 migration tests, retained preferences, no uninstall/data-clear, and device before/after row checks.
- [Pairing changes to a different user/server] → Opaque local pairing context and held accepted events; no cross-pairing automatic replay.
- [OS delivery crash window] → Persistent outbox/receipt and deterministic active identity, with explicit limits on exactly-once audible delivery.
- [Scoped Android changes affect UI controls] → Minimal existing-control updates and increased-font checks; TalkBack acceptance is out of scope by owner decision, and no accessibility conformance claim is made.

## Migration Plan

1. After explicit approval, establish mobile topic branch and local ignored `PROJECT_STATE.md`, initialize its native OpenSpec core integration, and adopt this companion capability/design/tasks using native artifact instructions. Keep backend-specific identity delta in this coordination change. Obtain host approval for writes outside the current workspace; do not copy around the sandbox.
2. Finish backend gates and implement/test the additive ordinary transaction identity. Build the final API artifact and verify it against an isolated synthetic database; keep the current owner local runtime dry-run-only.
3. Implement mobile additive migration and completion/alert/security paths with focused tests, then build debug/release manifests and debug APK with JDK 17. No broad dependency upgrade.
4. Install with `adb install -r` only after populated migration tests pass. Preserve phone application data and configuration; never uninstall or clear it. Verify stored-row preservation and listener permissions/service.
5. Use a separately identified disposable local test pairing for synthetic ingestion-to-completion, movement deduplication, offline recovery, restart, permission denial, and edit tests. Existing owner messages stay dry-run-only. Restore the prior local pairing without exposing its key after the isolated test; do not silently change production settings.
6. Record backend/mobile revision and artifact identities, cost/call counts, pass/fail evidence, device state, and remaining limitations. Commit/push/release require subsequent owner direction.
7. On rollback, stop new completion work while preserving Room data and delivery receipts; prefer a forward corrective APK. Do not downgrade to a destructive-migration build or erase user storage to make rollback succeed.
