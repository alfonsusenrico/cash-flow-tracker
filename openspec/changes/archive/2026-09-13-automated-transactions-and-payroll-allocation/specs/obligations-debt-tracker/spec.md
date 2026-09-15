## ADDED Requirements

### Requirement: Automated Recurring Debt Amortization
The system SHALL support linking recurring payment rules directly to active debt obligations (`obligation_id`):
1. When a recurring rule linked to an obligation executes (whether automatically or via 1-tap confirmation), it SHALL record the expense and decrement `obligations.remaining_amount` by the payment amount.
2. If `remaining_amount` reaches 0, the obligation status SHALL indicate that it is fully paid off.

#### Scenario: Recurring debt amortization execution
- **WHEN** a recurring rule of Rp 1.500.000 linked to an obligation with Rp 3.000.000 remaining executes
- **THEN** the system logs the transaction and updates the obligation's `remaining_amount` to Rp 1.500.000
