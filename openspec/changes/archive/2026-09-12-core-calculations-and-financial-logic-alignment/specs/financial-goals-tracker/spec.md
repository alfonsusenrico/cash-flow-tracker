## MODIFIED Requirements

### Requirement: Goal Milestone Progress & Monthly Pacing
The system SHALL compute the funding progress percentage and required monthly contribution pace for individual goals:
1. `percentage_completed`: `min(100, round((current_amount / target_amount) * 100))`.
2. `monthly_target_pace`: `max(0, round((target_amount - current_amount) / max(1, remaining_months)))`.
3. The Goals view SHALL display individual target cards with their linked accounts and pacing progress, and SHALL NOT display an aggregated global target summary card that sums unrelated goals together.

#### Scenario: Viewing goals view without misleading aggregate banner
- **WHEN** the user navigates to the Goals page with an Emergency Fund goal and a Long-term Savings goal
- **THEN** both goals render as individual progress cards without an overarching combined target sum banner
