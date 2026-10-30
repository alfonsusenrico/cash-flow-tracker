# Spec Delta

## ADDED Requirements

### Requirement: Plan References Are Owned and Validated
Creating or updating a recurring plan SHALL accept only active accounts, categories and obligations owned by the same user. An update SHALL validate the final combined state of the plan, and an optional relationship SHALL be cleared only when the request sets it to null explicitly.

#### Scenario: Updating a plan without crossing tenant boundaries
- **WHEN** a user creates or updates a plan with another user's account
- **THEN** the request is rejected and the stored plan is unchanged

#### Scenario: Clearing an optional relationship
- **WHEN** a user updates a plan and sets its obligation to null
- **THEN** the link is removed, while omitted fields keep their stored values

### Requirement: Plan Endpoints Match the Plan Type
A transfer plan SHALL use two distinct liquid accounts and SHALL reject an investment position as either endpoint. An investment top-up plan SHALL target an eligible amount-tracked product and SHALL reject a category or an obligation.

#### Scenario: Rejecting a transfer plan to an investment position
- **WHEN** a user creates a transfer plan whose source or target is an investment position
- **THEN** the request is rejected with 422 and no plan is stored

#### Scenario: Rejecting forbidden options on an investment top-up plan
- **WHEN** a user creates or updates an investment top-up plan with a category or an obligation
- **THEN** the request is rejected with 422 and the stored plan is unchanged

### Requirement: Explicit Occurrence Recording Is Atomic and Idempotent
Recording a plan occurrence through `POST /api/recurring/{id}/occurrences/record` SHALL commit the ledger effect, the occurrence record and the advanced due date in one database transaction. A repeated or concurrent record of the same occurrence SHALL return the existing result, and a failed record SHALL leave the occurrence pending with the ledger and schedule unchanged.

#### Scenario: Concurrent recording of one occurrence
- **WHEN** two requests record the same occurrence at the same time
- **THEN** exactly one occurrence and one ledger effect are committed

#### Scenario: Recording fails
- **WHEN** recording an occurrence fails validation, for example for insufficient funds
- **THEN** no ledger row is created, the occurrence stays pending and the due date is unchanged

### Requirement: Monthly Investment Top-up Waits for an Explicit Record
A monthly investment top-up plan SHALL move money only when the owner records its occurrence; its due date alone SHALL NOT create a contribution.

#### Scenario: Recording a monthly investment top-up once
- **WHEN** the top-up plan becomes due and the owner then records its occurrence twice
- **THEN** nothing moves before the record, exactly one contribution is created, the second request reports the existing result, and the next due date advances
