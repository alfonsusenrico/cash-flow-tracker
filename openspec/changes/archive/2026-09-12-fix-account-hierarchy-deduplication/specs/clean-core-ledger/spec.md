## MODIFIED Requirements

### Requirement: Payday Cycle Window & Safe-to-Spend Allowance
The system SHALL calculate the active spending cycle based on the user's configured payday day across the entire calendar range of 1 to 31. For months with fewer days than the configured payday day (e.g. February, April, June, September, November), the cycle calculation SHALL dynamically clamp the cycle start date to the maximum days in that month (`calendar.monthrange(year, month)[1]`). It SHALL expose `GET /api/pulse` returning:
1. `today_spent`: Total expenses recorded today.
2. `safe_to_spend_today`: `(total_cycle_budget - cycle_spent_so_far) / max(1, remaining_cycle_days)`.
3. `cycle_remaining`: Total remaining spending budget for the current cycle.
4. `cycle_pace`: Progress percentage of the cycle days elapsed versus budget percentage consumed.
5. `total_liquid_balance`: The sum of all active top-level account balances (`parent_id IS NULL`), accurately reflecting market valuations and avoiding cartesian join multiplication.

#### Scenario: Checking daily pulse with late-month payday
- **WHEN** a user with a configured payday of 29 requests `GET /api/pulse` in September
- **THEN** the system calculates the cycle starting from August 29 through September 28, adjusting safe daily spend accordingly

#### Scenario: Pulse total liquid balance ignores child duplicates
- **WHEN** a user has a parent account with 10,000,000 IDR and child pockets totaling 10,000,000 IDR
- **THEN** `GET /api/pulse` returns `total_liquid_balance` of 10,000,000 IDR rather than 20,000,000 IDR
