# Design

## Context

See `proposal.md` for motivation. The ledger currently computes `pageInflow` and `pageOutflow` in frontend state from the paginated `transactions` array (maximum 50 items). When paginating through transaction history or filtering, these numbers only reflect the active slice rather than actual cash flow.

## Goals / Non-Goals

**Goals:**
- Provide reliable, server-computed running payday cycle cash flow (inbound, outbound, net) and cumulative filtered cash flow totals.
- Deliver a responsive UI complying with Scandinavian Tactile Minimalist standards that displays both cycle and cumulative figures cleanly on mobile (< sm) and desktop (>= sm).
- Keep performance optimal with zero extra database round-trips by computing aggregates during the existing `GET /transactions` query.

**Non-Goals:**
- Altering the transaction list pagination mechanism or row data schema.
- Changing budget calculation rules or Kakeibo analytics on the `/insights` page.

## Decisions

### 1. Unified Aggregate Query in `GET /transactions`
- **Choice**: Replace the standalone `SELECT COUNT(*) AS total` query with a combined aggregation query that calculates `COUNT(*)`, cumulative inflow/outflow, and running cycle inflow/outflow within the same `WHERE` clause.
- **Rationale**: The database executes one scan over the filtered rows. The response retains backward compatibility (`ok`, `total`, `limit`, `offset`, `transactions`) while adding `summary: { cycle: {...}, cumulative: {...} }`.
- **Alternatives Considered**:
  - *Separate `/transactions/summary` endpoint*: Requires an extra network request on every filter or pagination change, adding latency and requiring duplicated filter parameter parsing.
  - *Client-side fetching of all transactions*: Unscalable and slow on large ledgers; defeats pagination.

### 2. Running Payday Cycle Window
- **Choice**: Reuse `get_cycle_window(payday_day, now_local.date())` from `app.routers.pulse`.
- **Rationale**: Reuses established, tested logic that takes into account the user's configured `payday_day` (default 25) with month-end date clamping.

### 3. Cash Flow Business Rules
- **Income (Inflow)**:
  `t.type = 'income' AND COALESCE(c.name, '') != 'Internal Movement' AND COALESCE(c.is_excluded_from_budget, false) = false`
- **Expense (Outflow)**:
  `t.type = 'expense' AND COALESCE(c.name, '') != 'Internal Movement' AND COALESCE(c.is_excluded_from_budget, false) = false`
- **Net Flow**:
  `inflow - outflow`
- **Cycle Filter**:
  The cycle aggregates apply `AND t.date >= cycle_start AND t.date <= cycle_end` on top of the active filter conditions.

### 4. UI/UX & Responsive Hierarchy
- **Desktop (>= sm)**:
  - Header with title "Arus Kas", cycle date badge (e.g., `25 Sep – 24 Okt`), and an accessible segmented pill toggle: `[ Siklus Berjalan ]` | `[ Total Kumulatif ]`.
  - 3 structured cards (`Uang Masuk`, `Uang Keluar`, `Selisih Bersih`).
  - Primary metric rendered in large bold `tabular-nums` tracking tight typography.
  - Secondary comparison row with subtle divider displaying the alternate scope (e.g., `Kumulatif: +Rp 54.200.000` when viewing Cycle, or `Siklus ini: +Rp 7.436.000` when viewing Cumulative).
- **Mobile (< sm)**:
  - Compact segmented pill switcher at the top (`Siklus (25 Sep – 24 Okt)` vs `Kumulatif`).
  - 1-row tactile ribbon with 3 columns (`Masuk`, `Keluar`, `Net`), tabular numerals, and appropriate semantic color tokens (Emerald for positive/income, Rose for negative/expense).

## Risks / Trade-offs

- **[Risk] Multiple active filters (e.g. date range) overlapping with cycle window** → **Mitigation**: Cumulative aggregates always reflect all matching rows under the active filters. When a user explicitly filters by a custom date range outside the current cycle, cycle aggregates accurately reflect transactions within both the cycle window and the custom date filter.
- **[Risk] Timezone handling** → **Mitigation**: Cycle window calculation uses `settings.tz` (Asia/Jakarta) and converts to UTC timestamps matching transaction date boundaries in PostgreSQL.
