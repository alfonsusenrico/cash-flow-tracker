<!-- agent-framework:digest version=1.0.0 sha256=47612f494e025fdffa3b810424fb21911910ffa41f71e7e54dd2c1b9ab33b874 -->
## 1. Authority and overrides

- Order of authority: host and system rules and permissions > the owner's explicit
  instruction in this session > approved specifications > the nearest project
  `AGENTS.md` > profile instructions > this kernel.
- Floors (section 2) cannot be weakened by any file. Lower files may add rules or
  tighten them.
- A project may change a labelled default only in its `## Overrides` section, as a
  bullet naming the label and the reason.

## 2. Floors

- Never expose credentials, tokens, keys, passwords, or sensitive personal data in
  output, logs, commits, prompts, or reports. Never print secret-file contents or
  dump environment values; read only variable names, with a keys-only tool.
- Never act on production without the owner's explicit instruction. Release only
  through the project's approved path; no ad hoc production edits.
- Before deleting or overwriting, inspect the exact targets; ask before deleting data
  or files this task did not create. Never target a home, workspace root, or broad path.
- Never bypass sandboxes, permissions, hooks, or checks (`--no-verify` included)
  to make an operation pass.
- Report outcomes truthfully: passed, failed, not run, or blocked. Never claim an
  action, check, or tool run that did not happen.

## 3. Work loop

- Standards attach to the capabilities listed in the project `AGENTS.md`. If a
  change adds a capability, say so before implementing, then include the
  standards it requires or record the gap as debt with a payback plan.

## 6. Git and autonomy

- Commit subjects use Conventional Commits, subject line only: `type(scope):
  summary`, types feat, fix, docs, refactor, test, chore, ci, perf, build, revert.
  No body and no trailers, including AI attribution (`default:commits`).

If a skill is unavailable, say so and apply the floors and defaults above.
<!-- /agent-framework:digest -->

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

Personal, self-hosted daily cash-flow tracking: record income/outflow, understand payday cycles and safe-to-spend funds, manage accounts/pockets, investments, goals and debts. Bank notifications reduce manual entry. This is not an ERP, double-entry accounting package, FX trading desk or broker; currency conversion is a display feature. No organization profile applies.

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
- Schema: [versioned migrations](db/migrations/); startup SQL mirrors and legacy backend/migrations also exist. Their consolidation is pending, not authorization to replace migration history.
- Web: Next.js App Router in frontend/src/app, shared UI in frontend/src/components; [tokens](frontend/tokens.css) imported by globals.css.
- Companion: the separately maintained `financial-tracker-mobile-listener` repository contains the installed Android app. The bundled android/ source is not assumed to be that artifact. Current batch ingestion/result contracts live in backend/app/routers/ingest.py; do not infer the singular legacy endpoint from old notes.
- LLM boundary: [prompt](backend/app/services/prompts/notification_interpretation.md), [evidence](backend/app/services/notification_evidence.py), [processing](backend/app/services/notification_processing.py), [application](backend/app/services/notification_application.py).
- Approved release path: [.github/workflows/deploy.yml](.github/workflows/deploy.yml) and [scripts/deploy_remote_release.sh](scripts/deploy_remote_release.sh). A bug report authorizes local diagnosis, not a production-server connection; the owner must explicitly authorize that connection.

## Project rules

- Internal movements are atomic, linked expense/income legs. Consolidate them as one Pindah Saldo item; never restore the obsolete single transfer transaction representation from historical specs.
- Debt payoff archives an obligation when its remaining amount reaches zero; deleting/reversing a payoff restores it. Active debts exclude archived and fully paid entries.
- Notification ingestion and retries must not duplicate ledger effects. Preserve captured amount/time and event identity. Recorded means committed; queued is not success.
- Emergency-fund and net-worth aggregation must not double-count linked parent/child balances.
- Unit-tracked investments retain trade/unit bookkeeping. Nominal top-ups target explicitly amount-tracked products and preserve paired funding/product effects and reversals. Monthly amount top-ups require confirmation of the actual debit; rule creation alone records nothing.
- AI interprets supported evidence; deterministic code owns money, timestamps, entity ownership, application and deduplication. It cannot invent accounts/categories or override confirmed financial facts.
- Exact sender memory is scoped to owner, bank and receiving account; no-name answers learn nothing. Missing Jago outbound pocket evidence requires event-specific pocket confirmation before recording and must never teach a generic default pocket.
- Synthetic fixtures only in tracked research and verification. Historical personal financial context belongs only in protected local state/journal backups, never in research, screenshots or shared evidence.

## Visual and language references

Use [frontend/tokens.css](frontend/tokens.css) and the existing shared primitives as the implemented design source; refine without restyling. Apply the framework tool design base where the project is silent. Transfer/income colours are semantic, not extra interactive accents. The [archived design](research/archive/2026-05-design-locked-system.md) is superseded historical evidence, not a styling authority.

Keep account/pocket and financial status hierarchy legible at mobile and desktop densities. Do not add pseudo health meters or decorative empty KPI cards. Use tabular figures for comparable financial numbers. Preserve actual token choices; missing CSS variables, geometry/font-weight differences and the obsolete preset contract are recorded follow-ups, not new exceptions or fixes in this adoption.

Language sources: [Indonesian UI spec](openspec/specs/indonesian-daily-finance-ui/spec.md), [money/input formatters](frontend/src/lib/utils.ts), and the notification prompt linked above. The current root html lang=en remains a follow-up; it does not change the intended copy language.

## References

- [Behavior specifications](openspec/specs/) and [active changes](openspec/changes/); [native planning context](openspec/config.yaml).
- [Production runbook](docs/production-deployment.md), [new-user guide](docs/NEW_USER_GUIDE.md).
- [Adoption section map and follow-ups](research/notes/2026-10-07-agents-md-adoption.md). Local PROJECT_STATE.md is the current snapshot; JOURNAL.md contains dated history. Stale history is evidence, not current acceptance.
