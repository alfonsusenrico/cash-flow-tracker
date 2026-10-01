# Spec Delta

## MODIFIED Requirements

### Requirement: Recurring Transaction Rules Definition

The system SHALL support creating, querying, updating, and deactivating recurring transaction rules (`recurring_rules`):
1. Each rule SHALL specify `name`, `type` (`expense`, `income`, `transfer`, `investment_topup`), `amount` (> 0), `source_account_id`, optional `target_account_id` (required for transfers and investment top-ups), optional `category_id`, optional `obligation_id`, schedule type (`monthly_day`, `payday`, `weekly`), `schedule_day`, `auto_post` (boolean), `is_payroll_allocation` (boolean), and `notes`.
2. The system SHALL compute `next_due_date` automatically based on the schedule type and schedule day.
3. The system SHALL expose `GET /api/recurring`, `POST /api/recurring`, `PUT /api/recurring/{id}`, `DELETE /api/recurring/{id}`, and `POST /api/recurring/toggle/{id}`.
4. Investment-top-up rules SHALL validate the liquid funding account and amount-tracked mutual-fund leaf target, use `auto_post = false`, and reject category, obligation, and payroll-allocation linkage. Their creation SHALL NOT itself record a contribution.

#### Scenario: Creating a monthly debt payment rule
- **WHEN** user creates an automated expense rule for "Cicilan Mobil" of Rp 2.500.000 scheduled monthly on day 25 with `auto_post = true` linked to an obligation
- **THEN** system saves the rule with `next_due_date` calculated and `auto_post` enabled

#### Scenario: Creating a weekly pocket transfer rule
- **WHEN** user creates a recurring transfer from "BCA" to "Jago - Jajan" of Rp 300.000 scheduled weekly
- **THEN** system saves the rule and advances `next_due_date` to the upcoming scheduled day

#### Scenario: Define monthly Bibit contribution
- **WHEN** the owner schedules Rp112.590 monthly from BCA RDN to a specific eligible Bibit product
- **THEN** the system saves a manually confirmed investment-top-up rule with the selected due day and makes no ledger or valuation changes

#### Scenario: Reject automatic investment logging
- **WHEN** a create or update request enables automatic posting or payroll allocation for an investment-top-up rule
- **THEN** the request fails and the saved rule remains unchanged

### Requirement: Execution Engine (Auto-Post & 1-Tap Confirmation)

The system SHALL support executing due recurring rules:
1. Rules with `auto_post = true` that reach or pass `next_due_date` SHALL be automatically executed in an atomic transaction when triggered by the system, creating a ledger transaction and advancing `next_due_date` to the next interval.
2. Rules with `auto_post = false` that reach or pass `next_due_date` SHALL appear in `GET /api/recurring/pending` and in a frontend notification banner for 1-tap user confirmation.
3. The endpoint `POST /api/recurring/execute` SHALL accept a list of rule IDs to execute immediately on demand.
4. Each executed transaction SHALL reference the originating `recurring_rule_id` and update `last_executed_at` on the rule.
5. Investment-top-up rules SHALL never execute through automatic processing. Confirmation SHALL use the same contribution semantics as a manual top-up and commit the occurrence, both ledger legs, capital/value adjustment, and schedule advancement atomically. Repeated or concurrent confirmation of one occurrence SHALL record it once. Failure SHALL leave that occurrence pending and present its error to the owner.

#### Scenario: Auto-posting a due scheduled bill
- **WHEN** system processes pending recurring rules on day 25 and an active rule with `auto_post = true` is due
- **THEN** system records the transaction in the ledger, updates account balances, and advances the rule's `next_due_date` by 1 month

#### Scenario: Confirming a pending scheduled payment
- **WHEN** user views pending recurring rules on the dashboard and clicks "Konfirmasi Catat"
- **THEN** system records the transaction, decrements any linked obligation balance, and marks the rule as fulfilled for the current cycle

#### Scenario: Confirm the actual monthly debit
- **WHEN** an investment-top-up rule is due and the owner confirms after the successful Rp112.590 debit
- **THEN** one contribution is recorded from the specified RDN to the specified product, the occurrence is fulfilled, and the next due date advances one month

#### Scenario: Date alone does not confirm a debit
- **WHEN** a monthly investment-top-up rule becomes due and automatic processing runs
- **THEN** it remains pending without any ledger or valuation changes until the owner confirms

#### Scenario: Funding validation fails during confirmation
- **WHEN** the owner confirms a due investment top-up but funds are insufficient or its product is no longer eligible
- **THEN** the rule stays pending, its next due date is unchanged, no contribution is recorded, and the interface reports the failure

#### Scenario: Concurrent confirmation
- **WHEN** two confirmation requests refer to the same investment rule occurrence
- **THEN** only one contribution is recorded and the schedule advances once
