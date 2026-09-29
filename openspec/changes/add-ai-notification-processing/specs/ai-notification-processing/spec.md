# Spec Delta

## Purpose

Reduce manual notification classification through a constrained AI processor while keeping financial facts, registered references, privacy, and ledger writes under backend control.

## ADDED Requirements

### Requirement: Tool-Less Structured Notification Processing
The system SHALL use a versioned prompt to request one structured interpretation of a notification. The processor SHALL propose a concise Indonesian description, observed direction, existing account/pocket/category references, transaction-level Kakeibo classification, and optional existing movement candidate. It SHALL have no tools, credentials in context, database access, or authority to perform writes. Notification text and history SHALL be treated as untrusted data rather than instructions.

#### Scenario: Describing a settled merchant payment
- **WHEN** a supported notification confirms a merchant payment and suitable registered references are available
- **THEN** the processor returns a structured proposed record with a concise description and the backend validates it before creating a transaction

#### Scenario: Ignoring instructions embedded in a notification
- **WHEN** notification text asks the processor to ignore its rules, disclose secrets, or invent an account
- **THEN** those instructions do not grant additional authority and any invalid proposed result creates no financial effect

### Requirement: Bounded User-Owned Processing Context
Processing context SHALL contain only the authenticated user's active accounts/pockets, eligible categories, applicable user rules, and a configurable bounded selection of recent relevant transactions. Account hierarchy, explicit default pockets, category kind, and savings designation SHALL be represented. Movement candidates SHALL be identified separately from the recent-history limit. An incomplete context SHALL NOT authorize invented references or arbitrary fallback selection.

#### Scenario: Handling renamed accounts and custom categories
- **WHEN** the user renames an account or creates a custom expense category before processing
- **THEN** the context reflects current registered data rather than a fixed list of category or pocket names

#### Scenario: Finding a counterpart outside the recent-history sample
- **WHEN** a valid opposite movement leg falls within the automatic time window but outside the bounded recent-history sample
- **THEN** the system still considers it through movement candidate discovery

#### Scenario: Isolating another user's records
- **WHEN** another user has similarly named accounts or recent transactions
- **THEN** none of those references or transaction contents appear in the processing context

### Requirement: Source-Backed Financial Facts
The backend SHALL preserve the original event timestamp including seconds and SHALL verify amounts and currency against notification evidence independently of model confidence. Amount parsing SHALL be exact, not floating-point or silent rounding. Model output and mobile expected fields SHALL NOT override conflicting source evidence. Multiple plausible amounts, missing settlement evidence, unsupported currency, or a non-integral amount unsupported by the current whole-IDR ledger SHALL produce review rather than an invented or rounded transaction.

#### Scenario: Preserving amount and seconds
- **WHEN** the notification states `Rp125.000` and its event timestamp is `2026-09-28T10:15:27+07:00`
- **THEN** the committed amount is 125000 IDR and the timestamp retains the original instant and seconds regardless of the model's proposed values

#### Scenario: Rejecting an ambiguous principal and fee
- **WHEN** notification evidence contains a principal, fee, and total without an unambiguous supported settlement interpretation
- **THEN** no guessed amount is committed and the event is retained for review

#### Scenario: Rejecting silent fractional truncation
- **WHEN** the only supported amount is `IDR 50,000.50`
- **THEN** the backend does not truncate it to 50000 and creates no ledger effect

### Requirement: Backend Reference and Classification Validation
Before a financial write, the system SHALL revalidate proposed references against current active ownership, hierarchy, category kind, and account eligibility. Ordinary liquid operations SHALL NOT use investment positions or mutate positions, goal progress, debt balances, allocations, or recurring rules. Kakeibo SHALL be a valid transaction-level value independent of category naming; movements SHALL use the canonical economic classification rule. Invalid or uncertain required references SHALL produce review without creating generic accounts, liquid pockets, or categories. Existing deterministic instrument-provisioning rules remain separate from the AI processor.

#### Scenario: Detecting an account archived during inference
- **WHEN** a proposed source account becomes archived after context construction but before application
- **THEN** the backend rejects that reference and creates no transaction against the archived account

#### Scenario: Rejecting a fabricated pocket or foreign category
- **WHEN** a model proposes a nonexistent pocket or a category owned by another user
- **THEN** the backend creates neither that reference nor a ledger transaction and records a review outcome

#### Scenario: Classifying two expenses in the same category independently
- **WHEN** two purchases share an expense category but have different supported spending purposes
- **THEN** their validated Kakeibo classifications can differ without changing the category definition

### Requirement: OpenAI Operational Provider With Explicit Retention Policy
The initial processor SHALL use the official OpenAI API with `gpt-5.6-luna` as its only operational model. It SHALL use a tool-less structured-output request with response storage disabled. It SHALL permit only `none` or `low` reasoning effort and SHALL default to `low`. The system SHALL require `OPENAI_API_KEY`, SHALL remain disabled without valid opt-in configuration, and SHALL NOT silently select a different model, provider, reasoning level, or paid fallback. An owner-authorized development dry run MAY invoke the same provider only when separately enabled; it SHALL not enable the normal worker or ledger writes. Documentation SHALL state that OpenAI API inputs are not used for training by default but standard abuse-monitoring retention may be up to 30 days unless the configured project has separately approved retention controls.

#### Scenario: Configuring a permitted low-reasoning model
- **WHEN** enabled operational configuration names `gpt-5.6-luna` with `low` reasoning and an available OpenAI credential
- **THEN** the processor issues a tool-less structured-output request with response storage disabled

#### Scenario: Configuring an unsupported model or reasoning effort
- **WHEN** enabled operational configuration names a different model or a reasoning effort other than `none` or `low`
- **THEN** external processing is rejected before real notification content is sent

#### Scenario: Losing OpenAI API access
- **WHEN** the configured OpenAI credential or model access is denied
- **THEN** accepted events remain recoverable with an explicit provider failure and no automatic fallback

#### Scenario: Keeping inference opt-in
- **WHEN** the feature is disabled or its required credential is missing
- **THEN** the application remains available and sends no notification data to an external model

#### Scenario: Isolating a local dry run from operational AI
- **WHEN** an owner invokes an explicitly enabled development dry run while operational AI remains disabled
- **THEN** the system may return one validated proposal without starting the worker or creating a ledger effect

### Requirement: Minimized and Sanitized External Data
The system SHALL send only the notification fields and owned context needed for interpretation. Authentication secrets, account numbers, OTPs, device identifiers, receipt contents, arbitrary raw extras, and unrelated personal metadata SHALL be excluded or redacted. Diagnostics and evaluation reports SHALL NOT contain real notification text, personal transaction history, credentials, or unsanitized provider error bodies.

#### Scenario: Sanitizing a banking notification
- **WHEN** captured notification fields contain an account number, OTP, or device metadata
- **THEN** those values do not appear in the outbound model request or diagnostics

#### Scenario: Handling an error body that echoes the prompt
- **WHEN** a provider error includes request content or authorization details
- **THEN** only a safe error classification is exposed or logged

### Requirement: Synthetic OpenAI Configuration Benchmark
Model evaluation SHALL use a versioned wholly synthetic corpus mimicking supported Indonesian notification formats and synthetic registered context, not exported or merely anonymized real records. The runner SHALL apply the same entrance routing as production: definite ignored/non-candidate events SHALL be scored separately without external inference; deterministic Stockbit events SHALL be reported as a non-model route; only candidate non-Stockbit events SHALL be sent to `gpt-5.6-luna` and counted in model-quality metrics. Evaluation SHALL measure `gpt-5.6-luna` with `low` reasoning and, when explicitly requested, the permitted `none` baseline using the same prompt, schema, model-eligible cases, and parameters. Reports SHALL identify provider/model/reasoning effort, prompt and fixture versions, routing counts, model sample counts, repeats, schema validity, fact accuracy, reference accuracy, category/Kakeibo accuracy, movement precision/recall, safe abstention, token usage when returned, and latency percentiles. Provider failures SHALL count as unsuccessful model-eligible cases. Aggregate scores SHALL NOT hide unsafe amounts, references, or false movement merges.

#### Scenario: Comparing permitted reasoning configurations fairly
- **WHEN** `gpt-5.6-luna` is evaluated with `low` and `none` reasoning
- **THEN** each configuration is scored against the same synthetic expected outcomes and its unsuccessful requests remain visible in the report

#### Scenario: Excluding deterministic routes from model-quality scoring
- **WHEN** a synthetic fixture is ignored by the entrance filter or routed to deterministic Stockbit processing
- **THEN** the report records that route without a model request and excludes it from model-quality metrics while preserving a separate deterministic-route result

#### Scenario: Refusing evaluation against private runtime data
- **WHEN** a benchmark run attempts to obtain real notifications or ledger records
- **THEN** the run is refused without sending that data

#### Scenario: Preventing a misleading passing score
- **WHEN** a model has a high overall score but its validated decision would write a wrong amount, foreign account, or false movement match
- **THEN** the report marks the safety gate failed rather than recommending operational activation

#### Scenario: Exposing rejected model proposals
- **WHEN** trusted validation rejects an unsafe model proposal without a ledger write
- **THEN** the synthetic-only report retains the raw proposal, trusted decision, and safe rejection reason, and counts the proposal as a model error rather than correct

#### Scenario: Enforcing the evaluation budget
- **WHEN** a requested live synthetic run's conservative estimate exceeds its configured per-run dollar cap
- **THEN** the runner refuses before sending an OpenAI request and reports the estimate and cap without exposing credentials
