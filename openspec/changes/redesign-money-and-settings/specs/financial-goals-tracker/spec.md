## MODIFIED Requirements

### Requirement: Goal Milestone Progress & Monthly Pacing
The system SHALL compute the funding progress percentage and required monthly contribution pace for individual goals.

#### Scenario: Required behavior and constraints
- **WHEN** the system calculates a goal milestone and contribution pace
- **THEN** the following required behavior and constraints hold:

  The system SHALL compute the funding progress percentage and required monthly contribution pace for individual goals:
  1. `percentage_completed`: `min(100, round((current_amount / target_amount) * 100))`.
  2. `monthly_target_pace`: `max(0, round((target_amount - current_amount) / max(1, remaining_months)))`.
  3. The Target & Tagihan page SHALL show each goal as its own row in "Target tabungan" with its name, a labelled progress bar (the bar hidden from assistive technology, the percentage and amounts in text), the saved and target amounts, its linked accounts or "Dicatat manual", its monthly pace when it has a target date, and a status pill in words. It SHALL NOT display an aggregated global target summary that sums unrelated goals together. Activating a row SHALL open the goal's drawer with its existing actions.

#### Scenario: Viewing goals view without misleading aggregate banner
- **WHEN** the user navigates to the Goals page with an Emergency Fund goal and a Long-term Savings goal
- **THEN** both goals render as individual rows with their own progress, without an overarching combined target sum

#### Scenario: Goal without a target date
- **WHEN** a goal has no `target_date`
- **THEN** its row SHALL show the saved and target amounts and the percentage, and SHALL show no monthly pace instead of an invented one
