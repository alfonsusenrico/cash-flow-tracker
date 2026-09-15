# Cash Flow Tracker — Agent Instructions & Workflow Baseline

This document records approved project standards, architecture decisions, and the mandatory execution workflow for **Cash Flow Tracker**. Future agents and pair-programming sessions MUST read `/Users/enrico/project/AGENTS.md`, `/Users/enrico/project/AI_PROJECT_DELIVERY_STANDARD.md`, and this file before performing meaningful work.

---

## 1. Purpose and Core Mission

**Cash Flow Tracker** is a self-hosted personal finance companion designed for **daily mindfulness and friction-free money tracking**.
- **Primary Goal:** Make it effortless to record daily cash in/out, monitor accounts, track payday cycles, and know how much is safe to spend today without boredom or forgetfulness.
- **Non-Goals:** It is NOT an enterprise resource planning (ERP) system, a complex multi-state budgeting machine, an investment portfolio manager, or a debt-collection tracking engine.

---

## 2. Mandatory Spec-Driven Workflow (OpenSpec Standard)

No non-trivial implementation shall occur without an approved OpenSpec change. All work follows the 7-stage OpenSpec Core lifecycle:

```mermaid
flowchart TD
    A["1. Review Current Situation<br/>(Codebase & Working-Tree Audit)"] --> B["2. Scaffold Change<br/>(openspec new change &lt;name&gt;)"]
    B --> C["3. Prepare Strategy & Artifacts<br/>(proposal.md, design.md, specs/, tasks.md)"]
    C --> D["4. User Review & Approval Gate<br/>(Explicit user confirmation required)"]
    D --> E["5. Work Controller & Implementation<br/>(Execute tasks sequentially, update tasks.md)"]
    E --> F["6. Verify Approved Contract<br/>(Tests, lints, build, openspec validate)"]
    F --> G["7. User Acceptance & Archive<br/>(openspec archive)"]
```

### Stage 1: Review Current Codebase Situation
- Audit active files, migrations, models, endpoints, and working-tree status before planning.
- Identify technical debt, redundant abstractions, and constraints.
- Document facts and authoritative requirements; never guess user intent.

### Stage 2: Scaffold OpenSpec Change
- Initialize/maintain change under `openspec/changes/<change-name>/`.
- Use schema `spec-driven` with `.openspec.yaml`.

### Stage 3: Prepare Strategy, Specs, and Implementation Plan
Every change requires 4 foundational artifacts:
1. **`proposal.md`**:
   - `## Why`: Clear problem statement and motivation.
   - `## What Changes`: Concrete scope of functional and technical modifications.
   - `## Capabilities`: New or modified capabilities (`<capability-name>`).
   - `## Impact`: Affected files, database impact, API changes, dependencies.
   - `## Expected Outcome`: User-visible and system-visible end state in plain language.
2. **`specs/<capability>/spec.md`**:
   - Formal, testable requirements using `SHALL` language.
   - Concrete Gherkin scenarios: `#### Scenario: ...`, `- **WHEN** ...`, `- **THEN** ...`.
3. **`design.md`**:
   - Technical strategy, data structures, endpoint contracts, error handling, and trade-offs.
   - Guardrails against over-engineering and scope creep.
4. **`tasks.md`**:
   - Phased, numbered, checkable tasks (`- [ ] 1.1 ...`, `- [ ] 2.1 ...`).
   - Each phase states its **Expected Result** and verification evidence.
   - Tasks describe concrete deliverables, not open-ended activities.

### Stage 4: User Review & Approval Gate
- Present artifacts to the user for explicit review and feedback.
- **STOP and wait for user approval** before writing implementation code. Do not proceed until approved.

### Stage 5: Work Controller & Execution
- Implement sequentially from `tasks.md`.
- Mark completed tasks immediately: `- [ ]` $\rightarrow$ `- [x]`.
- Keep code changes minimal, focused, and scoped strictly to the active task.
- **Pause rule:** If ambiguity, an unforeseen blocker, or a design flaw is uncovered, pause immediately, report to the user, and suggest artifact adjustments before continuing.

### Stage 6: Verification & Contract Validation
- Run targeted automated tests, linters, type checks, build commands, and `git diff --check`.
- Execute `openspec validate <change-name>` to verify specification integrity.
- Perform end-to-end smoke testing for all newly introduced/modified behavior.

### Stage 7: Acceptance & Archive
- Present verification proof to the user for final acceptance.
- On user approval, archive the change with `openspec archive <change-name>`.

---

## 3. Current State & Codebase Situation

- **Legacy Bloat (Phases 2–5):** The legacy codebase accumulated 34 database migrations, 13 frontend pages, duplicate routers (`routers/web.py` 2.2k lines and `routers/public.py` 1.8k lines), and 5 overlapping abstraction layers (Buckets, Allocation Plans, Strategy Rules, Financial Goals, Obligations, Assets, Net Worth).
- **Approved Decision:** Clean-slate refactor approved by project owner. We do **NOT** need to write backward-compatibility data migrations or retain legacy migration tables.
- **Target Source Layout:** Consolidate into a clean, role-based structure following `/Users/enrico/project/AI_PROJECT_DELIVERY_STANDARD.md`.

---

## 4. Architecture Guardrails & Decisions

### 1. Minimal 5-Table Data Model
Only 5 core persistence entities are permitted:
1. `users`: Authentication, session secret, invite code, payday cycle day, currency.
2. `accounts`: Liquid cash holders (Cash, Bank, E-wallet), current balance, archived status.
3. `categories`: Spending/income labels, icon, color, optional monthly budget ceiling.
4. `transactions`: Daily cash movements (Expense, Income, Internal Transfer), timestamp, notes, receipt.
5. `api_keys`: Hashed Bearer tokens for external automation (Telegram bot).

*Strictly Prohibited:* Do not reintroduce buckets, allocation state machines, strategy rules, goal projections, debt/loan obligations, investment assets, or background auto-funding schedulers.

### 2. Unified Backend (FastAPI)
- Single auth dependency resolving either session cookies (browser) or Bearer API keys (Telegram bot).
- No duplicate endpoint trees.
- Direct, high-performance endpoints:
  - `GET /api/pulse`: Today's spending, remaining daily allowance, monthly pace, recent feed.
  - `POST /api/transactions`: 1-call expense/income/transfer entry.
  - `GET /api/insights`: Visual category breakdown and cycle spending histogram.
  - `GET/POST /api/accounts`: Balances and quick movement.

### 3. Anti-AI-Slop Visual & Interaction Standard
- **No AI-Slop:** No rainbow gradients, no blurry purple glow dropshadows, no meaningless 0–100 health meters, no 24px-padded empty bubbly cards, and no slow bouncy animations.
- **Craft & Density (Linear / Wise / Apple Card aesthetic):**
  - Crisp typography (Inter, tabular figures for currency and dates).
  - High-contrast neutral palette (`#FAFAF9` light / `#0F1012` dark), subtle 1px structural borders.
  - Emerald green for Cash In, coral/rose for Cash Out, cobalt for Transfers.
  - Tactile, rapid inputs: 1-screen quick capture, keyboard shortcuts (`N` for new), thumb-friendly numeric pad, 1-tap category chips.
- **The 3-Screen Layout:**
  1. **Pulse (Today):** Allowance, quick capture bar, recent activity timeline.
  2. **Insights (Month):** Category budget progress bars, daily spending cadence.
  3. **Accounts (Vault):** Physical bank/wallet cards, 1-tap transfer.

---

## 5. Local Commands

```bash
# OpenSpec
openspec status
openspec status --change <change-name>
openspec validate <change-name>
openspec archive <change-name>

# Backend
cd backend
source .venv/bin/activate
uvicorn app.main:app --reload --port 8000
python -m pytest tests/

# Frontend
cd frontend
npm run dev
npm run type-check
npm run lint
npm run build

# Docker
docker compose up -d
docker compose down
```
