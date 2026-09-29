# Tasks

## 1. Financial evidence and output contract

- [x] 1.1 Add the strict interpretation contract and versioned tool-less Indonesian prompt; verify unit tests reject extra fields, fabricated references, instructions embedded in data, malformed/truncated JSON, and invalid classifications.
- [x] 1.2 Add exact source-backed amount/currency/settlement validation and timestamp preservation; verify separators, integral decimals, actual fractions, principal/fee ambiguity, conflicting mobile hints, pending/failed events, and nonzero seconds/timezones.
- [x] 1.3 Create at least 40 wholly synthetic notification/context cases with expected outcomes and development/held-out partitions; verify fixture tests cover supported package variants, custom registered data, noise, unsafe inputs, and movement boundaries without real-person/runtime records.

## 2. Registered context and OpenAI provider

- [x] 2.1 Build bounded owner-scoped active account/pocket/category/rule/history context, including effective defaults and savings metadata; verify isolation, archived references, renames, incomplete context, and a candidate outside the latest 20 history records.
- [x] 2.2 Add outbound data minimization/redaction and independent candidate discovery; verify captured HTTP requests exclude secrets, account numbers, OTPs, device identifiers, arbitrary extras, receipts, and raw unrelated history, and that candidate truncation never fabricates uniqueness.
- [x] 2.3 Replace the Zen adapter with official `openai==3.19.2` AsyncOpenAI Responses integration using strict JSON schema, `store=false`, no tools, deadlines, and safe errors; verify mocked SDK transport tests for valid responses, refusal/malformed output, timeout, 429/Retry-After, 5xx, authentication/access denial, and echoed sensitive error bodies.
- [x] 2.4 Replace Zen configuration with disabled-by-default OpenAI configuration for `gpt-5.6-luna` and `low`/`none` reasoning only; verify startup remains available with missing credentials/disabled AI and invalid model/effort rejects requests without network calls.

## 3. Durable processing and migration

- [x] 3.1 Add the next additive Flyway migration and supported startup parity for processing/lease/result/provenance fields and constraints; verify fresh and V19-baseline isolated PostgreSQL migration, repeat validation, unchanged historical balances, and no automatic replay of legacy events.
- [x] 3.2 Implement per-event durable acceptance, duplicate lookup, safe deterministic mode, and recoverable queue states; verify duplicate hashes with changed payload cannot overwrite source facts, definite noise is not persisted, unfamiliar financial candidates are retained, and one batch event's failure does not erase others.
- [x] 3.3 Integrate a bounded lifespan worker with short claim transactions, fencing, lease recovery, bounded retry/backoff, and graceful shutdown; verify deterministic clock/transport tests and isolated database tests for worker restart, lost lease, stale results, exhausted retries, and no database connection held during external inference.

## 4. Trusted ledger application and movements

- [x] 4.1 Extract shared cursor-based pair validation/linkage from the manual merge route without changing its public behavior; verify existing manual merge tests still allow eligible pairs above 30 seconds, preserve amounts/notes/receipts/dates/balances, and reject financial associations or consumed pairs.
- [x] 4.2 Apply validated ordinary observed legs and complete Jago single/dual-pocket movements using current ownership/default/hierarchy checks; verify no unrelated account fallback or unknown liquid pocket creation, two canonical Jago legs, no second notification required, and appropriate canonical Kakeibo.
- [x] 4.3 Add notification-backed cross-account candidate corroboration, mutual uniqueness, short serialized application, and strict original-time matching below 30 seconds; verify both arrival orders, separate batches, a history-omitted candidate, 29/30-second boundaries, masked/insufficient evidence, unrelated equal-value payments, unequal amounts/fees, and concurrent counterpart delivery.
- [x] 4.4 Add observed-versus-complete movement provenance and existing-role confirmation without another ledger effect; verify one logical movement/record key for counterpart notifications, no duplicate pair, one direct role confirmation, and review rather than guessing against ambiguous historical data.
- [x] 4.5 Preserve settled negative-balance reconciliation and isolate deterministic Stockbit processing behind the same claim/application boundary; verify stale-balance ingestion succeeds once with a reconciliation flag, manual overdraft guards remain, and broker buy/sell/funding/instrument-provisioning retries do not duplicate effects or allow AI-selected instruments.

## 5. Compact API results and recovery

- [x] 5.1 Add one compact result per submitted event while preserving batch input and counter keys; verify queued versus recorded semantics, input order, expense/income/movement display fields, numeric exact amounts, null external endpoints, commit-before-success, and stable keys across retry/linkage.
- [x] 5.2 Add owner-scoped result lookup and explicit retry for unresolved events, plus safe status/error inspection metadata; verify Bearer/session access, foreign-event not-found behavior, recorded/ignored retry conflict, concurrent retries, and no implicit ledger writes from label edits.
- [x] 5.3 Support legacy result materialization and deliberate user edit/deletion without notification replay; verify legacy linked movements stay recorded, unlinked historical events are not automatically processed, deletion does not recreate ledger rows, and unchanged older clients can still read counters.
- [x] 5.4 Add authenticated development-only direct/stored-event dry-run routes using current owner context and the normal constrained interpretation/validation boundary, but no persistence, worker, lease, retry, transaction, movement, or balance mutation; verify environment/flag rejection, session/Bearer ownership, foreign event hiding, deterministic no-provider routing, compact redacted output, and database/mutation-spy no-write evidence.

## 6. Synthetic benchmark and prompt tuning

- [x] 6.1 Correct the synthetic-only evaluation runner to apply production-equivalent routing before inference: score ignored/non-candidate cases separately without provider calls, retain Stockbit as a deterministic non-model route, and compute model metrics only from candidate non-Stockbit cases. Verify mocked tests prove routing, failed-request accounting, raw synthetic proposal/trusted-decision/rejection-reason diagnostics, safety-gate override, fixture/prompt/revision identity, latency percentiles, and no runtime data loader imports.
- [x] 6.2 With an authorized OpenAI runtime key available, run the synthetic development benchmark for `gpt-5.6-luna` at `low` and optional `none` reasoning with three repeats per model-eligible case; deliver a report including deterministic-route counts, explicit denial/unavailable results, usage, latency, and conservative estimated/observed cost under a default $0.25 per-run cap within the owner's $5 total evaluation budget, without sending private data. If credentials/access are unavailable, keep this task visibly unverified rather than claiming benchmark completion.
- [x] 6.3 Tune only on the development subset, rerun the held-out `low` versus `none` comparison with identical prompt/schema/fixtures, and retain `low` operationally only if the defined safety/quality gates pass; verify the final report includes abstention/recall and owner-sampled description quality, not just a favorable aggregate score.

## 7. Integrated verification and handoff

- [x] 7.1 Run the full backend suite against a disposable isolated PostgreSQL database, including concurrent delivery/linkage, expired workers, rollback-on-application failure, migration compatibility, and owner isolation; record revision-associated pass/fail/skip counts, and do not treat skipped integration cases as passing evidence.
- [x] 7.2 Rebuild the local API artifact and run authenticated synthetic ingestion-to-completion smoke tests with mocked OpenAI inference first, then the permitted live model if available; verify compact lookup results, durable recovery, paired movement count/balances, negative-balance flagging, ordinary ledger display, and no existing user-data mutation or production action.
- [x] 7.3 Update backend configuration/API/benchmark documentation and `PROJECT_STATE.md`, review the changed-file/security diff, run `openspec validate add-ai-notification-processing --strict` and `git diff --check`, and present implementation/runtime/benchmark evidence separately for owner acceptance; preserve unrelated open changes and leave sync/archive/release to subsequent authorization.
