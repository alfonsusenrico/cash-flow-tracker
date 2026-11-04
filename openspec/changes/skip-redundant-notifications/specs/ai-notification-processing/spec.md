## MODIFIED Requirements

### Requirement: Synthetic OpenAI Configuration Benchmark
Model evaluation SHALL use a versioned wholly synthetic corpus mimicking supported Indonesian notification formats and synthetic registered context, not exported or merely anonymized real records.

#### Scenario: Required behavior and constraints
- **WHEN** an authorized synthetic notification benchmark is run
- **THEN** the following required behavior and constraints hold:

  Model evaluation SHALL use a versioned wholly synthetic corpus mimicking supported Indonesian notification formats and synthetic registered context, not exported or merely anonymized real records. The runner SHALL apply the same entrance routing as production: definite ignored/non-candidate events SHALL be scored separately without external inference; deterministic Stockbit events SHALL be reported as a non-model route; only candidate non-Stockbit events SHALL be sent to the selected allowlisted model and counted in model-quality metrics. Configurations compared in one evaluation SHALL use the same prompt, schema, model-eligible cases, and parameters apart from the model or reasoning effort under test. Reports SHALL identify provider/model/reasoning effort, prompt and fixture versions, routing counts, model sample counts, repeats, schema validity, fact accuracy, reference accuracy, category/Kakeibo accuracy, movement precision/recall, safe abstention, token usage when returned, and latency percentiles. Provider failures SHALL count as unsuccessful model-eligible cases. Aggregate scores SHALL NOT hide unsafe amounts, references, or false movement merges.

#### Scenario: Comparing permitted reasoning configurations fairly
- **WHEN** one allowlisted reasoning model is evaluated with `low` and `none` reasoning
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

#### Scenario: Scoring a notification sequence
- **WHEN** the corpus contains the fictional sequence "myBCA debit Rp 500.000, GoPay credit 'Tabungan by Jago' Rp 500.000, Jago 'received Rp500.000 from GoPay', then a Jago pocket move two minutes later", each event replayed on its own without neighbours
- **THEN** the report SHALL score each event as an ordinary observed leg or complete movement against its expected outcome, SHALL leave the duplicate decision to the backend tests, and SHALL name the fixture version `synthetic-notifications-5` and prompt version `notification-interpretation-12`

#### Scenario: Enforcing the evaluation budget
- **WHEN** a requested live synthetic run's conservative estimate exceeds its configured per-run dollar cap
- **THEN** the runner refuses before sending an OpenAI request and reports the estimate and cap without exposing credentials

## REMOVED Requirements

### Requirement: Neighbouring Notifications in Context
**Reason**: The owner decided on 2026-11-04 that redundant notifications are a notification-control problem, not a model judgement ("I don't think we need AI to see this"). The live benchmark showed the model never used the neighbours for the same-money decision (0% recall), and they added about 9% input tokens per notification.
**Migration**: The context no longer carries `neighbours`, `neighbours_incomplete` or their ids, and `NOTIFICATION_NEIGHBOUR_WINDOW_SECONDS` is no longer read. Redundant reports are handled by the deterministic "One Arrival Seen by Two Apps" guard and by "Redundant Notification Patterns" in `movement-pairing`.

### Requirement: Same-Money Proposal
**Reason**: Removed together with the neighbour context. The model never proposed `same_money_as` in the benchmark, and a model proposal is no longer part of the duplicate decision.
**Migration**: The output schema drops `same_money_as` and the validation code `invalid_neighbour_reference`. Events still held with `possible_duplicate_observation` are requeued by migration V46 and decided by the deterministic path.
