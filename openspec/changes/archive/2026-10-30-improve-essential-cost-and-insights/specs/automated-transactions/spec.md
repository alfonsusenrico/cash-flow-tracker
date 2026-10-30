## ADDED Requirements

### Requirement: Automatic linking of one certain payment
A due occurrence of an active expense or income plan SHALL be linked automatically when exactly one unlinked recorded transaction has the same owner and type, the plan's account and the identical amount, is dated from three days before to seven days after the due date and not in the future, is not an internal movement leg or fee, was not rejected for this plan, and is not a candidate for any other due plan. Transfer and top-up plans SHALL NOT be linked automatically.

#### Scenario: Exactly one candidate
- **WHEN** one matching payment was recorded the day after a due date
- **THEN** the occurrence SHALL be linked once with link method `auto`, no transaction SHALL be created, and an audit event with identifiers and actions only SHALL be written

#### Scenario: Concurrent passes
- **WHEN** several automatic-linking passes run at the same time for the same owner
- **THEN** the occurrence SHALL be linked once and debt progress SHALL change once

#### Scenario: Two candidates
- **WHEN** two matching payments were recorded in the window
- **THEN** nothing SHALL be linked and the occurrence SHALL stay due with both candidates for the owner

#### Scenario: Wrong account, amount or window
- **WHEN** the only payments differ in account or amount, or fall four days before or eight days after the due date
- **THEN** nothing SHALL be linked

#### Scenario: A payment two plans could claim
- **WHEN** one payment matches due occurrences of two plans
- **THEN** neither occurrence SHALL be linked automatically

#### Scenario: When linking runs
- **WHEN** a transaction is created or updated, including from a notification or text capture, or a plan is created or updated
- **THEN** the owner's due plans SHALL be checked shortly afterwards without delaying the recording, and every due plan SHALL also be checked by the periodic background pass

### Requirement: Undoing a plan link
The owner SHALL be able to undo a link from a due-plan view where automatic links are labelled "Ditautkan otomatis". Undoing SHALL return the occurrence to due, reverse debt progress that the link applied exactly once, and record the pair so it is never linked automatically again.

#### Scenario: Undo an automatic debt payment link
- **WHEN** the owner selects "Batalkan tautan" on an automatic link that paid off a debt
- **THEN** the debt SHALL be restored and reactivated once, the occurrence SHALL be due again, and a repeated undo SHALL change nothing

#### Scenario: No automatic relink
- **WHEN** automatic linking runs again after an undo
- **THEN** the same transaction SHALL NOT be linked to that plan automatically, while the owner MAY still link it explicitly

#### Scenario: Resolving after an undo
- **WHEN** the owner resolves an undone occurrence whose following occurrence was already resolved
- **THEN** the plan's next due date SHALL move to the first later occurrence that is not resolved
