# Spec Delta

## MODIFIED Requirements

### Requirement: Debt & Obligations Management
The system SHALL support creating, editing, querying, and deleting recurring obligations and debts:
1. Each obligation SHALL contain `name`, positive `total_amount`, `remaining_amount` from zero through `total_amount`, optional `due_date`, optional non-negative `minimum_payment`, optional `notes`, and `is_archived`.
2. Create and update SHALL validate the final combined state, including when total and remaining amounts change together.
3. The system SHALL expose `GET /api/obligations`, `POST /api/obligations`, `PATCH /api/obligations/{id}`, and `DELETE /api/obligations/{id}`.
4. Active queries SHALL return only obligations where `is_archived = false` and `remaining_amount > 0`.
5. Setting remaining amount to zero SHALL archive the obligation; raising it above zero through an authorized reversal or edit SHALL reactivate it.

#### Scenario: Rejecting debt above its original total
- **WHEN** a user submits a final obligation state whose remaining amount exceeds its total amount
- **THEN** the system rejects the request and preserves the previous obligation

#### Scenario: Validating a combined update
- **WHEN** a user updates total and remaining amounts in one request and the resulting values satisfy the obligation constraints
- **THEN** the system persists both values and recalculates payoff metrics from the final state

#### Scenario: Creating an obligation
- **WHEN** a user submits an obligation with a 12,000,000 IDR total and 10,000,000 IDR remaining
- **THEN** the system persists the coherent outstanding liability

#### Scenario: Querying active obligations excludes zero-balance debts
- **WHEN** a user queries active obligations
- **THEN** obligations with zero remaining balance or archived status are excluded

#### Scenario: Direct update to zero remaining balance auto-archives
- **WHEN** a user updates an obligation's remaining amount to zero
- **THEN** the system persists zero and archives the obligation in the same operation
