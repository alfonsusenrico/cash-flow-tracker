# Proposal

## Why

Notification ingestion currently relies on fixed merchant/pocket dictionaries, owner-specific identity checks, and permissive account fallbacks. A constrained AI processor can reduce manual description and classification work while resolving only registered user data and preserving source-backed financial facts.

## What Changes

- Add a backend-only, tool-less notification processor with a versioned prompt and validated structured output. Context contains active owned accounts/pockets/categories, applicable user rules, bounded recent transactions, and separately queried movement candidates.
- Replace the unavailable Zen integration with the official OpenAI Python SDK and the Responses API. Use `gpt-5.6-luna` as the sole operational model, with explicit `low` reasoning by default and an opt-in `none` latency setting.
- Use strict structured output and `store=false`; retain the existing trusted validation boundary. OpenAI API inputs are not used for training by default, but the standard service may retain abuse-monitoring logs for up to 30 days. Do not represent this provider as zero-retention unless that control is separately approved for the configured project.
- Preserve exact source-supported amounts and original event timestamps, including seconds. Reject uncertain facts and references instead of inventing accounts, liquid pockets, categories, or transactions.
- Add durable PostgreSQL processing state, bounded retries, recoverable worker leases, and explicit review/failure outcomes. Keep external inference outside database locks and the batch HTTP request.
- Automatically reconcile uniquely supported cross-account movement legs strictly less than 30 seconds apart, without creating duplicate financial effects. Resolve explicit Jago pocket movements within the same registered parent immediately through the existing bilateral movement service.
- Return the approved compact per-event result alongside existing batch counters, with an owner-scoped result lookup and explicit retry for unresolved events. Only a committed ledger operation produces `recorded`.
- **BREAKING behavior correction:** remove unrelated primary/first-account fallback and automatic creation of unknown liquid Jago pockets. Unknown but plausibly financial notifications become inspectable review items rather than disappearing solely because a regex did not recognize them. Existing mobile filtering and request fields remain unchanged.
- Preserve deterministic broker trade processing, including its existing instrument-provisioning contract; the AI cannot create instruments, choose trade quantities, or apply goal/debt/recurring effects.
- Evaluate only notifications that the normal backend route would send to the AI. Report deterministic entrance-filter and Stockbit routes separately without external inference, retain synthetic raw proposals and trusted rejection reasons for tuning, and enforce a conservative per-run benchmark cost cap within the owner's $5 evaluation budget.
- Add an owner-authenticated, development-only dry-run interface for either an explicitly submitted mobile payload or one selected locally stored event. It uses current owner context and the same constrained provider/validation path but has no persistence or ledger authority, so real local examples can be reviewed before production activation.

## Capabilities

### New Capabilities

- `ai-notification-processing`: Constrained model processing, bounded dynamic context, financial evidence validation, privacy/cost controls, and synthetic model evaluation.

### Modified Capabilities

- `notification-ingestion`: Dynamic owned-reference resolution, durable processing, compact committed results, Jago bilateral movements, and conservative cross-notification movement reconciliation.
- `notification-filtering`: Backend settlement/evidence gating that distinguishes definite noise from unfamiliar financial candidates without modifying companion-app behavior.

## Impact

- Backend: `backend/app/routers/ingest.py`, notification parsing/classification services, shared movement mutation ownership, configuration, and application lifespan. Existing manual movement APIs and ledger UI behavior remain unchanged.
- Persistence: an additive versioned Flyway migration after V19 and equivalent supported startup initialization for processing state, leases, safe error codes, result identity, and movement provenance. No historical ledger rewrite or automatic replay.
- API: preserve `POST /api/ingest/notifications` input and existing counter keys; add `results`, `GET /api/ingest/notifications/{event_id}/result`, an owner-authenticated explicit retry endpoint, and development-only authenticated dry-run routes for direct or selected stored local events. Queued acceptance is not transaction success.
- Runtime: environment-configured OpenAI credential/model/reasoning effort and opt-in activation; bounded worker concurrency using PostgreSQL as the durable queue. Development dry runs require separate explicit opt-in and do not start that worker. No Redis durability assumption, separate agent server, new deployment service, or automatic model fallback.
- Dependencies: pin the official OpenAI Python SDK to the latest release verified during implementation; do not reuse the Telegram bot's agent/tool loop.
- Verification: deterministic unit/contract tests, isolated PostgreSQL concurrency/migration/recovery tests, local API/Docker smoke checks, and synthetic-only live model reports. Direct API eligibility and model quality remain unverified until those evaluations run.
- Non-goals: mobile changes, frontend redesign/review UI, production access/deployment, expanding supported banks, autonomous agent tools, broad investment/accounting redesign, and repairing historical movements automatically.
