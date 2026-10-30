# Local backend and companion review checkpoint

## Final synthetic evaluation and isolated artifact smoke — 2026-09-29

- Backend remains on `feat/ai-notification-processing` at base `8315255`, uncommitted. Prompt v4 was tuned only from the development partition. The three final synthetic OpenAI runs retained raw proposals and trusted decisions under `evaluation/`; `benchmark-report.md` records the same prompt/fixture hashes, accuracy, latency, and bounded usage. The owner accepted five sampled fictional descriptions. Held-out `low` and `none` both failed schema-validity and unambiguous-classification gates, so operational AI remains disabled; no live ingestion was permitted for the artifact smoke.
- Final rebuilt local API image: `sha256:d31cb618be46dc0ae3f4e0f898aae2a335866dd9b9411ccc24d661684610a41a`. Disposable PostgreSQL 16 applied Flyway V1–V20. An authenticated fictional TestClient session executed this image against only that database, with normal background jobs suspended. Mocked OpenAI inference produced a queued-to-recorded ordinary expense. An expired lease was recovered and the stale claim was fenced; redelivery kept one committed ledger effect. A deterministic Jago movement committed exactly two linked legs and redelivery reused its logical key. Ledger lookup showed the ordinary expense, the resulting negative pocket balance carried `reconciliation_required`, and the other pocket gained the expected movement amount. No existing local account/notification/transaction database or production service was used.
- Final route review found that explicit retry switched every non-Stockbit event to AI mode, leaving deterministic retries unclaimable while AI was disabled. Retry now preserves its original mode and rejects an AI retry with a conflict when its worker configuration is unavailable. Focused database regressions cover both cases, including a corrected Jago pocket retry that completes without AI.
- Current-tree full backend suite against disposable PostgreSQL 16: **413 passed, zero skipped**. Focused provider/interpretation/benchmark tests: **123 passed**. Both disposable test containers and their fictional data were removed. The local API was recreated from the final image under normal Compose defaults: `APP_ENV=production`, `NOTIFICATION_AI_ENABLED=false`, `NOTIFICATION_AI_DRY_RUN_ENABLED=false`; local `/api/health` returned HTTP 200.
- Final changed-file review covered owner-scoped result/retry/dry-run routes, provider errors and outbound redaction, disabled-by-default configuration, additive migration, synthetic evaluation output, and unrelated untracked paths. The three fictional JSON reports contain no API-key or Bearer markers; no private runtime records were included. `openspec validate add-ai-notification-processing --strict`, `git diff --check`, and whitespace checks over the untracked implementation/report files all passed. The failed held-out quality gate and ordinary OpenAI retention boundary remain material limitations.

## Tested state

- Backend branch `feat/ai-notification-processing`, base commit `8315255`, uncommitted working tree on 2026-09-28. Final API image: `sha256:b87c015c21c7b03dfbf6abd2f9978b3fb66ceb1f09a5c7cfcfd54efe12300451`; prompt `notification-interpretation-3`.
- Companion branch `fix/mobile-listener-reliability-and-api-alignment`, commit `6a542b4`; no companion source edits or APK installation during this checkpoint.
- Physical TECNO Android 16 device connected through wireless ADB. Configured local API is reachable from the phone; notification access and listener foreground service are enabled. Credentials and stored message contents were consumed privately and never printed.
- Existing local user data was not submitted to normal ingestion. Only authenticated direct dry-run requests were used. Production was not accessed; nothing was committed, pushed, or deployed.

## Reproduced defects and causal corrections

1. Dry-run unconditionally overwrote legitimate `needs_review` / `ignored` outcomes with `would_record`. Focused pre-correction tests reproduced five failures, including empty success and incorrect endpoint validation. Non-record outcomes now remain non-record, and complete proposals use the shared account/default-pocket, Jago hierarchy, category, and Kakeibo resolver.
2. Evidence validation already accepted exact `[SELF]` substrings; fabricated or reconstructed quotes failed correctly. The prompt now explicitly requires exact contiguous sanitized quotes. A repeated real income case then exposed a different error: model selection of the BCA parent instead of its configured default pocket. Context now supplies backend-resolved endpoint IDs without weakening validation.
3. Dry-run mode still started financial background jobs. During the initial phone replay check, three investment valuation rows changed while transaction/event counts stayed unchanged. Investigation also found a pre-existing misplaced daily sleep: price refresh had no daily delay, while recurring execution had the daily delay. Scheduler cadence is corrected, and dry-run-only mode suspends all financial background jobs. No valuation rollback was attempted.
4. Incomplete provider responses previously lost usage metadata. Usage is now captured before response-status rejection so evaluation accounting can include failures when the provider supplies usage.

## Automated evidence

- Full backend suite: **410 passed, zero skipped**, using disposable PostgreSQL 16 with Flyway V1–V20 applied, independent of the existing local database.
- The task-created disposable test database/container was removed after verification; only fictional QA data was discarded and can be recreated. Existing local services/user data were not removed.
- Authenticated session/Bearer direct/stored dry-run integration tests verify owner isolation, foreign-event hiding, production rejection, original captured facts, and unchanged account/transaction/event/goal/obligation/recurring rows.
- Regressions cover non-record outcomes, institution/default-pocket mismatches, exact redaction evidence, malformed provider output, canonical Jago endpoints/Kakeibo, explicit model-context endpoint IDs, incomplete-response usage, and normal scheduler cadence.
- Companion existing unit tests: **18 passed**, using JDK 17. No new mobile behavior is proven by these existing tests.
- Local API Docker build, image identity, health, strict OpenSpec validation, and `git diff --check` passed. Artifact ingestion-to-completion smoke for this final tree and final held-out quality comparison remain open.

## Bounded real-notification dry runs

Seven live model requests were made during this checkpoint; no automatic retry or bulk synthetic rerun was started.

| Prompt | Local stored-phone case | Result |
| --- | --- | --- |
| v2 | myBCA expense | Complete `would_record` |
| v2 | Jago pocket transfer with unresolved mapping | `needs_review: uncertain_mapping` |
| v2 | Jago expense | Complete `would_record` |
| v2 | myBCA income | Complete `would_record` |
| v2 | Same myBCA income repeated | `needs_review: conflicting_observed_account` |
| v3 | Same myBCA income | Complete `would_record` |
| v3 | Same myBCA income repeated | Complete `would_record` |

After background isolation, final before/after checksums of all accounts, notification events, and transactions matched. Counts stayed at 13 transactions and 5 notification events. The initial background valuation changes described above must not be conflated with this final no-write result.

The phone currently retains only BCA/Jago cases; GoPay incoming and missing-ShopeePay-account handoff cases were not re-run from this device. Unit/synthetic coverage is not a replacement for those real cases. Two successful v3 repeats do not establish model quality or production readiness.

The initial five calls had an estimated conservative ceiling of approximately $0.08; two further calls were bounded by the same context/output limits. These are estimates, not account billing or a remaining-credit claim. Preserve the owner's $5 total limit. Previous synthetic quality gates failed; operational AI remains disabled and new prompt/context comparisons are required before activation.

## Companion findings requiring a separate approved change

- `NotificationSyncWorker` marks every batch member synced, prunes before processing results, and has no completion-result polling. Accepted queued/processing events therefore lose durable completion tracking.
- Alerts are keyed by payload hash rather than a persisted logical `record_key`. Redelivery and paired movement notifications can produce duplicate alerts.
- A logical record key is saved as `server_transaction_id` and passed to the UUID transaction-edit route. These identifiers are not interchangeable.
- Room uses destructive migration fallback. New result-state columns require an additive migration preserving stored notifications and credentials.
- Diagnostic logging includes raw notification/alert content and arbitrary backend error bodies; release also exports the debug-injection/purge receiver.
- Backup exclusions name a different preference file from the actual credential store and omit the raw-notification database.
- Income alerts use the external/source side instead of the receiving target. Notification permission denial and process interruption need explicit durable delivery semantics.

Next mobile work should persist per-event acceptance/completion separately, poll only accepted unresolved events with bounded WorkManager retries, deduplicate/upsert alerts by logical key, avoid treating keys as ledger UUIDs, preserve existing data through migration, and close the identified logging/debug/backup boundaries. Existing package filtering and backend financial rules are not to be redesigned as incidental mobile work.
