# Agent framework adoption — 2026-10-07

## Approved outcome and boundaries

Owner-approved documentation adoption from main `4db742f`. This is a personal project with no organization profile. Preserve the dirty `fix/investment-topup-label` checkout; do not change application code, CI, secrets, production or GitHub settings. Install no framework hooks. Adoption uses the installed CLI rather than a hand-authored digest.

Expected result: a current pinned digest, project-specific instructions below 8,000 characters, explicit existing capabilities/language, protected compact local state plus journal, real native OpenSpec context, synthetic research scaffolding, passing checks and a pushed review branch. Commits separately cover AGENTS/digest, OpenSpec config and research. Legacy spec summaries and strict-format repairs are isolated in another documentation commit.

## Before/after sizes

| File | Before | After |
| --- | --- | --- |
| AGENTS.md | 18,602 bytes / 18,594 characters | 8,760 bytes / 8,760 characters; 6,775 project characters outside the managed digest |
| PROJECT_STATE.md | 100,939 bytes / 100,724 characters | 5,085 bytes at compaction; final receipt remains below 8 KB |

The original state is unchanged and privately backed up outside the repository. Dated checkpoints are copied to local JOURNAL.md in their original newest-first order under a new adoption checkpoint. Neither journal nor original context is committed. The journal/backup are mode 0600; the backup directory is 0700. The original checkout's large state is deliberately preserved; compaction lives in the adoption worktree.

## Original AGENTS section map

| Original section | Disposition and retained project meaning |
| --- | --- |
| Opening mandatory reads of organization/home documents | Removed absolute paths and dead links. Global kernel/skills govern procedure; this project has no organization profile. |
| owner-baseline and Conditional guides | Replaced with the CLI-managed pinned kernel digest, no hand-copied procedures. |
| 1. Purpose and Core Mission | Retained personal finance, daily awareness, bank notification automation and non-goals. Clarified that existing currency display conversion is not an FX trading desk. |
| 2. Mandatory Native OpenSpec Workflow and stages | Removed kernel/skill restatements. Native OpenSpec remains in use; project context and artifact rules now live in openspec/config.yaml. |
| 3. Architecture and backend router catalog | Replaced discoverable catalogs with source pointers. Payday, ledger, account hierarchy, portfolio, goal, debt, recurring and notification behavior remains in source/specs. |
| 3. Frontend page/catalog styling | Retained mobile/desktop financial hierarchy; dropped old palette/layout prescriptions that contradict current tokens. Live tokens/components are the reference. |
| 3. Android Companion | Corrected provenance: bundled android/ is legacy; the separately maintained companion is not assumed to match it. Batch ingestion/result contracts are linked in source. |
| 4. Persistence Entities | Kept sources of truth via database/migration pointers rather than restating evolving schemas. No entity, table or API changed. |
| 4. Bilateral movements | Retained atomic linked expense/income legs and one consolidated Pindah Saldo row; do not restore obsolete transfer types from historical documents. |
| 4. Debt Lifecycle | Retained payoff archive/reversal behavior and active-debt filtering. |
| 4. Idempotent Ingestion | Retained captured facts, idempotent retries and committed/queued distinction. |
| 4. Emergency fund deduplication | Retained parent/child balance deduplication for linked balances. |
| 4. Visual standard / dead UI standard reference | Replaced restated colours with a tokens link and framework tool-base gap rule. Retained meaningful financial density, hierarchy and prohibition of pseudo health meters/decorative empty KPI cards. No visual implementation changed. |
| 5. Local Commands | Retained current package commands and V14 preflight. Added explicit local HTTP configuration and honest database skip guidance; destructive compose-down/rebuild recipes are not always-loaded instructions. |
| 6. Secrets sanitization | Delegated to pinned floors and sensitive-operations skill; retained the project-specific rule that tracked research/evidence contains fictional data only. |
| 7. Production Isolation | Retained approved pipeline/script and explicit server-connection authority. Generic secret/deploy/migration floors are not copied. Existing Flyway/startup/legacy mechanisms are documented as consolidation debt. |
| 8. Branches, Commits, Merge Policies | Removed universal restatements; installed kernel governs subject-only Conventional commits, topic branches and owner merging. |

New project-specific continuity rules preserve amount-tracked top-up confirmation, sender-memory scope and event-specific Jago source-pocket confirmation from approved, implemented changes. These are domain facts, not a replacement implementation plan.

## Existing capabilities and source evidence

| Capability | Why it applies | Source |
| --- | --- | --- |
| database | Ledger and notification state persist in PostgreSQL with connection pooling and versioned migrations. | backend/app/db/pool.py; db/migrations/; docker-compose.yml |
| deployed | The project uses a production pipeline and an immutable source-release layout. | .github/workflows/deploy.yml; scripts/deploy_remote_release.sh; docs/production-deployment.md |
| money | Transaction balances, atomic movements, obligations and investment top-ups have consequential effects. | backend/app/services/ledger_mutations.py; backend/app/services/investment_topups.py; backend/app/routers/obligations.py |
| personal-data | Accounts, user identities and captured financial notification text are processed/stored. | backend/app/services/auth.py; backend/app/routers/ingest.py; db/migrations/V25__notification_sender_confirmation.sql |
| background-jobs | Lifespan starts price synchronization, recurring execution and durable notification work. | backend/app/main.py:79; backend/app/main.py:96; backend/app/services/notification_processing.py |
| api-consumers | Web, Android companion and Telegram integration consume authenticated APIs. | frontend/src/lib/api.ts; backend/app/routers/ingest.py; telegram-bot/ |
| multi-user | Unified session/Bearer authentication resolves users and routes restrict rows by user_id. | backend/app/services/auth.py:115; backend/app/routers/ingest.py:146; backend/app/routers/accounts.py |
| llm | The provider interprets notification context using strict schemas and validated outcomes. | backend/app/services/openai_notification_provider.py; backend/app/services/notification_interpretation.py; backend/app/services/prompts/notification_interpretation.md |

These declarations enable the framework standards for already-existing capabilities. They do not certify every standard as satisfied. Separate payback work must audit request IDs/structured logging/backups, privacy retention, append-only audit coverage, worker separation, generated API clients/contract drift, session revocation and AI cost/quality gates. Known gaps have proposed follow-ups below; unrelated standards are not retrofitted here.

## Language and design decisions

- **Language:** Indonesian UI and generated descriptions; preserve proper names, account labels and stock symbols. Indonesian/English bank evidence stays verbatim. Sources: openspec/specs/indonesian-daily-finance-ui/spec.md; backend/app/services/prompts/notification_interpretation.md:13,17; frontend/src/components/layout/Sidebar.tsx:21; frontend/src/lib/utils.ts:34. USD display retains its en-US formatter; IDR retains id-ID formatting.
- **Surface:** tool, based on the financial workbench, daily home, ledger and account-management flows. This is not a marketing redesign.
- **Visual authority:** frontend/src/app/globals.css imports frontend/tokens.css. Its active coral action/focus, semantic transfer/income states and Plus Jakarta Sans/Geist Mono definitions take precedence over obsolete lime/charcoal prose. Comments mentioning Outfit do not override actual font declarations/imports.
- **No Overrides section:** no labelled kernel default deviation is required. Pip requirements are inherited existing-project tooling, not a new-Python-project uv decision. Token/base differences are visible follow-up debt, not permission to invent undocumented exceptions.
- **design.md recommendation:** separately approve moving it to research/archive/design-pre-token-system.md. It prescribes Inter, green CTAs and <=8px controls while the live implementation uses different fonts, action colour and geometry. Preserve it as historical evidence rather than silently treating it as a locked current contract. This adoption does not move or edit it.

## Hooks report

The shared Git metadata contains executable pre-commit, post-commit, post-checkout and post-merge Bob Shell hooks. pre-commit/post-checkout/post-merge filter pending JSONL notes against files still modified. post-commit validates matching diffs, attaches attribution to refs/notes/bob, updates pending notes and may fetch/merge/push the note ref remotely; it also cleans failed note-merge state. These are attribution hooks, not application tests or Conventional Commit enforcement.

core.hooksPath is unset. The original pending-note file exists but is empty; the adoption worktree has none, so its commits do not activate note publication. An existing refs/notes/bob ref remains (last metadata timestamp 2026-06-02). No tracked runtime/CI reference to Bob notes was found. This is a bounded repository audit, not proof that the owner's external Bob tooling no longer uses them.

Framework hook installation would set core.hooksPath=.githooks in shared repository configuration and stop Git running the current .git/hooks hooks for every worktree. The owner must decide whether to retain/chain them, how to preserve note behavior and how to provide framework hooks in each worktree. No hooks were installed, modified, disabled or bypassed; core.hooksPath is unchanged. This af version does not report missing hooks in project check, so the expected not-installed state is recorded explicitly.

## Native OpenSpec verification repair

Strict baseline from an isolated copy of main: **7 passed / 37 failed**, exit 1. The previously reported 17/27 baseline and 44/0 summary-only result came from non-strict validation; they did not prove the requested strict gate. Final evidence uses the exact strict command and exit status.

Replace the 27 inherited Purpose placeholders with summaries of their existing contracts. OpenSpec 1.13.1 also rejects requirement text over 500 characters in strict mode. Keep concise normative introductions and move the original detailed clauses into explicit WHEN/THEN scenarios. Reformat 108 long bodies across 53 spec files, including MODIFIED blocks so their scenario names remain consistent with main. Requirement names, every original contract clause (ignoring indentation) and all pre-existing scenarios are preserved. No historical behavior contract is reconciled or newly approved by this formatting repair.

Final `openspec validate --all --strict`: **44 passed / 0 failed**, exit 0. No validator option or check is weakened; no specs or changes are archived. Obsolete preset and notification contracts remain separate reconciliation work.

## Proposed follow-ups — not implemented

| Follow-up | Evidence and proposed acceptance |
| --- | --- |
| CI coverage and deployment security | Workflow triggers main/manual only; pytest has no PostgreSQL service; conftest skips DB cases when unreachable. Workflow interpolates secrets directly and uses accept-new host keys. Add PR CI with migrated disposable PostgreSQL, name-safe credential handling and pinned host verification. |
| Artifact rollback and runtime health | Compose build services lack commit image tags; runbook rollback uses --no-build; web binds `${WEB_PORT:-8090}:80`; nginx /healthz is static; workflow retrieves production runtime.env onto the runner. Tag/retrieve exact images and prove rollback and revision-backed readiness; review ingress and runtime-file transfer. |
| Migration ownership | docker-compose.yml:99 uses Flyway/db/migrations; backend/app/db/init_db.py:27 mirrors startup DDL; backend/migrations has legacy SQL. Agree one migration authority and compatibility/retirement plan with reapply/drift tests. |
| Token and obsolete preset reconciliation | --accent-lime is referenced with old lime fallbacks in BottomNav and other components; --primary-light in Badge.tsx; --space-md in globals.css. Those variables are undefined. AppLayout.tsx:106 removes data-preset and theme_preset while theme-presets-switcher still requires them. Plan token cleanup and spec reconciliation without silently reinstating historical themes. |
| Tooling | Python uses requirements files/pip; no ruff or Prettier configuration found. Propose project-local uv migration and formatter/linter adoption with a dedicated formatting-only change and CI checks. |
| Repeatable UI evidence | Older verification docs refer to out-of-repository browser harnesses/temporary artifacts; root html lang=en conflicts with the intended Indonesian UI. Inventory and version synthetic browser checks under tests/e2e or scripts/verify, then cover language, responsive, theme, keyboard and zoom cases. No new browser acceptance is claimed here. |
| Capability-standard audit | Check structured observability, privacy retention, audit events, separate workers, generated clients/drift checks, auth revocation and bounded AI quality/cost controls against declared standards. Split confirmed gaps into approved changes before modifying behavior. |

## Verification and delivery

See ../evidence/2026-10-07-framework-adoption-verification.md for exact commands, counts, source revision, scope checks and commit receipts. Local PROJECT_STATE.md records owner confirmation and remaining decisions. No personal context from the journal/backup is copied into this tracked note.
