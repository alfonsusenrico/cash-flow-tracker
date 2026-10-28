# Spec Delta

## Purpose

Let the owner clarify an unfamiliar masked transfer sender before ledger recording and reuse confirmed names without weakening financial evidence or account ownership checks.

## ADDED Requirements

### Requirement: Sender clarification before recording

The backend SHALL hold an otherwise recordable incoming BCA account transfer when its notification contains a masked sender that has no unique, owner-confirmed alias for the receiving account.

#### Scenario: Required behavior and constraints
- **WHEN** an otherwise recordable incoming BCA transfer has a masked sender
- **THEN** the following required behavior and constraints hold:

  The backend SHALL hold an otherwise recordable incoming BCA account transfer when its notification contains a masked sender that has no unique, owner-confirmed alias for the receiving account. The hold SHALL apply independently of model confidence and across AI, deterministic, and fallback recording paths. It SHALL create no ledger entry or balance change until answered. Notifications without a sender mask, non-transfer income, expenses, and independently proven own-account movements SHALL retain their existing behavior. Uncertain financial facts SHALL use existing review/account-confirmation behavior rather than being accepted through a sender answer.

#### Scenario: High financial confidence with an unfamiliar sender
- **WHEN** BCA reports an incoming account transfer from the fictional sender `AN**RA**`, the amount and receiving account are proven, and no confirmed alias exists
- **THEN** the event is `needs_confirmation` for sender identity and no transaction or balance mutation exists even if AI financial confidence is high

#### Scenario: AI unavailable or deterministic mode
- **WHEN** the same eligible transfer reaches deterministic processing or deterministic fallback
- **THEN** sender clarification still occurs before any ledger write

#### Scenario: Independent evidence proves an own-account movement
- **WHEN** existing validated evidence proves a transfer between the owner's accounts
- **THEN** sender clarification does not block that movement and sender aliases are not used as proof of ownership

#### Scenario: Other financial notification
- **WHEN** the event is an expense, non-transfer income, or an incoming transfer with an unmasked sender
- **THEN** this sender clarification policy does not add a new hold

### Requirement: Typed and stable questions

The result SHALL expose a sender question containing its type, unique question identifier, observed masked sender, institution, receiving-account identity and label, proven amount, and currency.

#### Scenario: Required behavior and constraints
- **WHEN** the backend returns a sender clarification question
- **THEN** the following required behavior and constraints hold:

  The result SHALL expose a sender question containing its type, unique question identifier, observed masked sender, institution, receiving-account identity and label, proven amount, and currency. An unanswered question SHALL retain its identifier across result lookup and retries that do not change its meaning. Changing the question or moving between account and sender clarification SHALL produce a different question identifier. Account selection SHALL be resolved before sender clarification when the receiving account is uncertain, and accepted sender answers SHALL survive subsequent account confirmation or processing retries.

#### Scenario: Repeated result lookup
- **WHEN** the companion looks up the same unanswered sender question repeatedly
- **THEN** the question identifier and its financial context remain stable

#### Scenario: Account confirmation precedes sender confirmation
- **WHEN** both the receiving account and masked sender are unresolved
- **THEN** the account question appears first, and its answer leads to a separately identified sender question without recording prematurely

### Requirement: Validated and idempotent owner answers

Only the owner of an event SHALL be able to answer its current sender question.

#### Scenario: Required behavior and constraints
- **WHEN** an owner submits or retries an answer to a sender question
- **THEN** the following required behavior and constraints hold:

  Only the owner of an event SHALL be able to answer its current sender question. An answer SHALL choose either a bounded, nonblank sender name or explicit recording without a name. The answer SHALL NOT alter notification text, payload hash, amount, direction, currency, or account selection. Acceptance SHALL persist the answer and resume the same event atomically. Repeating the same accepted answer SHALL return the current result without another ledger record or memory mutation; a different answer to an already answered or superseded question SHALL return a conflict. Financial references SHALL be revalidated before recording, and success SHALL be reported only after recording commits.

#### Scenario: Known sender reply
- **WHEN** the owner replies `Andra` to the current question
- **THEN** the answer is persisted and the same event resumes toward a single `Transfer masuk dari Andra` record with its original financial facts

#### Scenario: Sender genuinely unknown
- **WHEN** the owner chooses `Catat tanpa nama`
- **THEN** the event resumes with a generic `Transfer masuk` description and no sender alias is learned

#### Scenario: Repeated delivery after a lost response
- **WHEN** the accepted answer is resent because the response was lost
- **THEN** the current result is returned and neither a second record nor another alias update is created

#### Scenario: Conflicting or stale reply
- **WHEN** a different answer targets a completed question or a reply targets a superseded question
- **THEN** the backend returns a conflict without changing the accepted answer, alias memory, or ledger

#### Scenario: Unauthorized or invalid answer
- **WHEN** another owner answers the event, or a reply contains only whitespace, exceeds the name limit, or contains prohibited control characters
- **THEN** an inaccessible event returns not found and invalid content returns a validation error without changing state

#### Scenario: Receiving account becomes unavailable
- **WHEN** the receiving account is archived before the accepted answer can be recorded
- **THEN** the event remains unresolved through the existing reference-validation behavior and no invalid-account record is committed

### Requirement: Scoped sender memory and descriptions

The backend SHALL store sender confirmations separately from owner-name and owned-account aliases.

#### Scenario: Required behavior and constraints
- **WHEN** a sender confirmation is saved or reused to describe a transfer
- **THEN** the following required behavior and constraints hold:

  The backend SHALL store sender confirmations separately from owner-name and owned-account aliases. Automatic reuse SHALL require the same owner, institution, receiving account, and normalized exact sender mask with mask characters preserved. The system SHALL NOT use fuzzy name similarity or a sender alias to infer an internal movement. Only uniquely confirmed names SHALL be reused; conflicting confirmations SHALL stop automatic reuse. The matching name SHALL be supplied as bounded owner-confirmed data to AI interpretation and SHALL control the recorded sender wording in deterministic paths too. Previously recorded transactions SHALL remain unchanged.

#### Scenario: Subsequent matching notification
- **WHEN** another transfer arrives with the exact mask for the same owner, institution, and receiving account
- **THEN** a unique confirmed alias is reused and the record description includes that name without asking again

#### Scenario: Different scope or mask
- **WHEN** another owner, receiving account, institution, or different mask has no matching confirmed alias
- **THEN** the prior confirmation is not reused and an eligible masked transfer asks again

#### Scenario: Conflicting pending confirmations
- **WHEN** two still-current questions for the same alias key receive different owner-confirmed names
- **THEN** each answer applies only to its own event, the memory is marked ambiguous, and future transfers ask again until the owner corrects or removes that memory

#### Scenario: Reply resembles a prompt instruction
- **WHEN** a bounded reply contains text that resembles an instruction
- **THEN** it is handled solely as name data and cannot change financial facts, model policy, or trigger another action

### Requirement: Owner management and privacy

The owner SHALL be able to list, correct, and remove confirmed sender memory in web Settings, including its institution, receiving account, mask, name, and ambiguity state. Corrections/removal SHALL affect future decisions only and SHALL NOT modify committed descriptions or already accepted event answers. Sender data SHALL be owner-scoped, excluded from operational logs, and sent to the configured AI provider only as the bounded matching context needed for notification processing.

#### Scenario: Correcting a remembered name
- **WHEN** the owner corrects a sender alias in Settings
- **THEN** future exact matches use the correction while committed transactions and previously accepted answers remain unchanged

#### Scenario: Removing memory
- **WHEN** the owner removes a sender alias
- **THEN** the next eligible matching notification asks again and historical ledger records are unchanged

#### Scenario: Another owner's memory
- **WHEN** an owner requests or changes another owner's alias identifier
- **THEN** the alias is not disclosed or changed

### Requirement: Compatibility and durable pending state

The backend SHALL emit sender questions only to companion versions declaring sender-reply support.

#### Scenario: Required behavior and constraints
- **WHEN** the backend negotiates sender-reply support or restores a pending question
- **THEN** the following required behavior and constraints hold:

  The backend SHALL emit sender questions only to companion versions declaring sender-reply support. Older or unrecognized versions SHALL receive `needs_review` with a sender-confirmation reason and no automatic ledger write. The existing account-mapping contract SHALL remain supported. An unanswered sender event SHALL remain pending without automatic expiry or recording; dismissal, provider failures, retries, and restarts SHALL NOT bypass the hold. An upgraded companion SHALL be able to explicitly retry an older event under its newer version and obtain a supported question.

#### Scenario: Old companion
- **WHEN** an eligible masked transfer was submitted by a companion without sender-reply support
- **THEN** its result is actionable `needs_review` and no unsupported sender question or automatic record is returned

#### Scenario: Upgraded companion retries an old event
- **WHEN** an upgraded companion explicitly retries that event
- **THEN** its newer capability is adopted and the event can reach a sender question

#### Scenario: Owner does not answer
- **WHEN** a sender question is dismissed or left unanswered for an extended period
- **THEN** the event remains unresolved and no automatic generic record is created
