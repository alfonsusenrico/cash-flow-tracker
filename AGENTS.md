
<!-- agent-framework:profile name=personal version=0.2.0 sha256=938706958bb35da55d0102d743d7e47efe1a584055e4e061b576772a9de0ffe2 -->
Personal and freelance rules (agent-framework profile `personal`):

- Commit as Alfonsus Enrico <enrico.soebijanto@gmail.com>; set it repo-locally
  before the first commit. A client-issued identity needs an `## Overrides` entry.
- In a client repository, exclude `AGENTS.md`, `PROJECT_STATE.md`, `JOURNAL.md`,
  `openspec/`, `research/`, and `.githooks/` locally (`project-bootstrap`) and
  confirm `git status` shows none of them before every push.
- Follow the client's conventions in their repository; record each one that departs
  from an owner default as an `## Overrides` bullet with the client rule as reason.
- In a client repository, push the topic branch and report the compare link; the
  owner opens the merge request (`default:mr-policy` in `## Overrides`).
- Client servers, shared databases, client data, and client accounts need the
  owner's explicit instruction for that task. Draft messages to the client or the
  client's team for the owner; never send them.
- Never invent prices, dates, approvals, or payments; agents draft commercial
  documents and the owner approves and sends them.
- Deliver to the `release-delivery` handover standard, in the client's language.
- Load `freelance-delivery` for the engagement lifecycle (first contact to closure)
  and `client-engagement` when working inside a client's or partner team's
  repository.
<!-- /agent-framework:profile -->

# Cash Flow Tracker

## Purpose and boundaries

Personal, self-hosted daily cash-flow tracking: record income/outflow, understand confirmed salary cycles and observed cash flow, manage accounts/pockets, investments, goals and debts. Bank notifications reduce manual entry. This is not an ERP, double-entry accounting package, FX trading desk or broker; currency conversion is a display feature. No organization profile applies.

Capabilities: database, deployed, money, personal-data, background-jobs, api-consumers, multi-user, llm
Design surface: tool
Language: Bahasa Indonesia for UI copy and generated ledger descriptions; retain proper names, symbols and owner labels. Parse Indonesian and English bank notifications without translating evidence. Engineering identifiers remain English. IDR uses Indonesian separators; the optional USD display uses its existing formatter. Backend calendar default is Asia/Jakarta; browser local datetime inputs must preserve the selected instant.

## Commands

Run from the repository root unless noted. Use the project-local Python environment and existing requirements files; dependency/tooling migration is separate work.

```sh
PYTHONPATH=backend backend/.venv/bin/python -m pytest backend/tests
cd frontend
npm ci
npm test
npm run type-check
npm run lint
npm run build
```

For a full backend run, supply a disposable, migrated PostgreSQL via TEST_DATABASE_URL. The existing fixtures skip database cases when PostgreSQL is absent; report those skips. SKIP_TEST_MIGRATIONS=1 is only appropriate after independently applying the versioned migrations.

```sh
openspec validate --all --strict
python3 scripts/flyway_v14_preflight.py --project-name cash-flow-tracker
docker compose -f docker-compose.yml -f docker-compose.local.yml up -d
```

The local Compose override selects development/HTTP cookies. For a direct loopback backend, supply the normal local configuration, explicitly APP_ENV=development and COOKIE_SECURE=false, then run `uvicorn app.main:app --reload --host 127.0.0.1 --port 8000` from backend. Secure cookies remain the production default; production refuses false.

## Architecture references

- Backend boundaries: [routes](backend/app/routers/), [services](backend/app/services/), [settings](backend/app/core/config.py), [authentication](backend/app/services/auth.py), [database](backend/app/db/).
- Schema: Flyway [versioned migrations](db/migrations/) are the only schema owner. The Compose `migrate` service and `scripts/deploy_remote_release.sh` run them before the API starts; the API runs no startup DDL. Add a new `V<n>__*.sql` (repeatable where practical) and never edit an applied one. The unreferenced legacy backend/migrations files are not executed.
- Web: Next.js App Router in frontend/src/app, shared UI in frontend/src/components; [tokens](frontend/tokens.css) imported by globals.css.
- Companion: the Android app lives only in the sibling repository `financial-tracker-mobile-listener`; release the backend before installing a companion that depends on it. Batch ingestion, results and question answers live in backend/app/routers/ingest.py; do not infer the singular legacy endpoint from old notes. Typed one-sentence capture (text, answer, undo, get) lives in [capture](backend/app/routers/capture.py). The Telegram bot is retired. The companion pairs by QR: web Settings creates a single-use code (`POST /api/devices/pairing-codes`, browser session only) and the phone exchanges it once for its own key (`POST /api/devices/pair`); `GET /api/devices` and `DELETE /api/devices/{id}` list and revoke keys. See [devices](backend/app/routers/devices.py) and [device pairing](backend/app/services/device_pairing.py).
- LLM boundary: [prompt](backend/app/services/prompts/notification_interpretation.md), [evidence](backend/app/services/notification_evidence.py), [processing](backend/app/services/notification_processing.py), [application](backend/app/services/notification_application.py), [merchant memory](backend/app/services/merchant_memory.py), [purpose questions](backend/app/services/purpose_questions.py), [history suggestions](backend/app/services/history_suggestions.py), [text capture](backend/app/services/text_capture.py) with its [parser](backend/app/services/capture_parsing.py).
- Automation and review: [provenance](backend/app/services/provenance.py), [movement pairing](backend/app/services/movement_pairing.py), [reimbursements](backend/app/services/reimbursements.py), [report figures](backend/app/services/report_figures.py) and [windows](backend/app/services/report_windows.py); the web `/review` page (Tinjau) uses [review](backend/app/routers/review.py) and [history suggestions](backend/app/routers/history_suggestions.py) routes.
- Approved release path: [.github/workflows/deploy.yml](.github/workflows/deploy.yml) and [scripts/deploy_remote_release.sh](scripts/deploy_remote_release.sh). A bug report authorizes local diagnosis, not a production-server connection; the owner must explicitly authorize that connection.

## Project rules

- Internal movements are atomic, linked expense/income legs. Consolidate them as one Pindah Saldo item; never restore the obsolete single transfer transaction representation from historical specs.
- Debt payoff archives an obligation when its remaining amount reaches zero; deleting/reversing a payoff restores it. Active debts exclude archived and fully paid entries.
- Notification ingestion and retries must not duplicate ledger effects. Preserve captured amount/time and event identity. Recorded means committed; queued is not success.
- Emergency-fund and net-worth aggregation must not double-count linked parent/child balances.
- Plans (`recurring_rules`) never post: a database check keeps `auto_post` false, retired execute and payroll endpoints answer 410, and a due plan is met only by linking a recorded transaction, skipping it, or an explicit manual record after same-amount captures are ruled out. Salary cycles start only at confirmed salary receipts; the fixed `payday_day` is deprecated.
- Unit-tracked investments retain trade/unit bookkeeping. Nominal top-ups target explicitly amount-tracked products and preserve paired funding/product effects and reversals. Monthly amount top-ups require confirmation of the actual debit; rule creation alone records nothing.
- AI interprets supported evidence; deterministic code owns money, timestamps, entity ownership, application and deduplication. It cannot invent accounts/categories or override confirmed financial facts.
- Exact sender memory is scoped to owner, bank and receiving account; no-name answers learn nothing. Missing Jago outbound pocket evidence requires event-specific pocket confirmation before recording and must never teach a generic default pocket.
- Settled notifications with proven amount, direction and owned account are always recorded; classification doubts set `review_status = needs_review` instead of holding the event. Merchant-less rows get an "Apa ini?" purpose question. `classification_feedback` is append-only (never update a row); `merchant_memory` auto-applies only exact owner merchant keys.
- Review-queue bulk actions and history suggestions change rows only through the owner-edit path; a suggestion applies nothing until the owner accepts it, and a rejection is remembered.
- Pairing codes and API keys are stored only as SHA-256 hashes and never logged. A pairing code is single use, lives 5 minutes and burns after 5 failed exchanges; routes that mint credentials accept only a browser session. Rotating the manual API key replaces only `created_via = 'manual'` keys, and revoked keys never authenticate.
- Synthetic fixtures only in tracked research and verification. Historical personal financial context belongs only in protected local state/journal backups, never in research, screenshots or shared evidence.

## Visual and language references

Use [frontend/tokens.css](frontend/tokens.css) and the existing shared primitives as the implemented design source; refine without restyling. Apply the framework tool design base where the project is silent. Transfer/income colours are semantic, not extra interactive accents. The [archived design](research/archive/2026-05-design-locked-system.md) is superseded historical evidence, not a styling authority.

Keep account/pocket and financial status hierarchy legible at mobile and desktop densities. Do not add pseudo health meters or decorative empty KPI cards. Use tabular figures for comparable financial numbers. Preserve actual token choices; missing CSS variables, geometry/font-weight differences and the obsolete preset contract are recorded follow-ups, not new exceptions or fixes in this adoption.

Language sources: [Indonesian UI spec](openspec/specs/indonesian-daily-finance-ui/spec.md), [money/input formatters](frontend/src/lib/utils.ts), and the notification prompt linked above. The current root html lang=en remains a follow-up; it does not change the intended copy language.

## References

- [Behavior specifications](openspec/specs/) and [active changes](openspec/changes/); [native planning context](openspec/config.yaml).
- [Production runbook](docs/production-deployment.md), [new-user guide](docs/NEW_USER_GUIDE.md).
- [Adoption section map and follow-ups](research/notes/2026-10-07-agents-md-adoption.md). Local PROJECT_STATE.md is the current snapshot; JOURNAL.md contains dated history. Stale history is evidence, not current acceptance.
