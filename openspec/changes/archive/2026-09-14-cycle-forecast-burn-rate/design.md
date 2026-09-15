## Context

In personal cash flow tracking, users operate around recurring income milestones (paydays). The current financial overview (`/api/dashboard/overview`) provides 7-day and 30-day daily burn averages, but attempts to forecast a 30-day outflow by simply multiplying the rounded 30-day daily rate by 30. This creates rounding distortions (e.g. Rp 50.000 spent turning into Rp 50.010) and fails to provide actionable insight for the active payday cycle (e.g., Aug 29 – Sep 28).

## Goals / Non-Goals

**Goals:**
- Provide a statistically and mathematically accurate **Active Payday Cycle Forecast** (`projected_cycle_outflow`) that projects total outflow through the end of the active payroll cycle.
- Eliminate premature integer rounding artifacts so single or sparse transactions accurately reflect actual figures.
- Provide cycle context in the API response (`cycle_elapsed_days`, `cycle_total_days`, `cycle_end_date`) to power dynamic UI descriptions.
- Update the frontend Insights dashboard tile from a static "Proyeksi Pengeluaran 30 Hari" to a cycle-aware "Proyeksi Akhir Siklus".

**Non-Goals:**
- Replacing the 7-day and 30-day historical daily burn rate averages (they remain useful historical velocity indicators).
- Predictive machine learning or multi-variable regression forecasting (linear pace extrapolation is transparent, deterministic, and sufficient for daily mindfulness).

## Decisions

### Decision 1: Extrapolate Active Cycle Pace Linearly
- **Chosen:**
  $$\text{projected\_cycle\_outflow} = \text{round}\left(\left(\frac{\text{total\_outflow\_in\_cycle}}{\text{days\_elapsed}}\right) \times \text{total\_days\_in\_cycle}\right)$$
- **Rationale:** The entire dashboard is framed by the active cycle (e.g., `29 Aug - 28 Sep`). Users need to know whether their current run-rate will fit within their salary and budget before their next payday.
- **Alternatives Considered:**
  - *7-day extrapolation ($\times 30$):* Highly erratic for users with lumpy expenses (rent, groceries).
  - *Rolling 30-day sum:* Backward-looking, not a forecast.

### Decision 2: Guardrails for Cycle Edge Cases
- When `total_outflow == 0`: forecast is 0.
- When `days_elapsed == 0` (first second of cycle): clamp `days_elapsed` to 1 to prevent division by zero.
- For non-cycle views (`timeframe == "30d"` or `"90d"`): extrapolate the timeframe's elapsed pace or use exact unrounded actuals, ensuring no `burn_30d * 30` integer rounding drift occurs.

### Decision 3: Backward-Compatible API Contract
- Maintain `daily_burn_7d`, `daily_burn_30d`, and `projected_30d_outflow` in `burn_rate` dictionary.
- Add `projected_cycle_outflow`, `cycle_elapsed_days`, `cycle_total_days`, and `cycle_end_date`.

## Risks / Trade-offs

- **[Risk] Early Cycle Volatility:** On Day 1 or Day 2 of a cycle, a large single expense (e.g., paying monthly rent of Rp 3.000.000 on Day 1) would project an unrealistically large cycle total ($3.000.000 \times 30 = 90.000.000$).
  → **Mitigation:** In the UI, display the progress fraction (e.g. `Hari 16 dari 31`) so the user immediately understands that confidence increases as the cycle matures.
