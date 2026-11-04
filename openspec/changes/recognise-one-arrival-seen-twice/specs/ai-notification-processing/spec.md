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
- **WHEN** the corpus contains the fictional sequence "myBCA debit Rp 500.000, GoPay credit 'Tabungan by Jago' Rp 500.000, Jago 'received Rp500.000 from GoPay', then a Jago pocket move two minutes later", replayed with each event's neighbours
- **THEN** the report SHALL score `same_money_as` precision and recall per event, SHALL count a `same_money_as` between two different real transfers as a safety failure, and SHALL name the fixture version `synthetic-notifications-4` and prompt version

#### Scenario: Enforcing the evaluation budget
- **WHEN** a requested live synthetic run's conservative estimate exceeds its configured per-run dollar cap
- **THEN** the runner refuses before sending an OpenAI request and reports the estimate and cap without exposing credentials

## ADDED Requirements

### Requirement: Neighbouring Notifications in Context
Processing context SHALL carry the owner's notification events and recorded legs whose post times fall within a configured window of the event, each with a context-local id, institution, direction, amount, signed seconds from the event, processing state, account when recorded, and whether it is part of a movement. Neighbours SHALL carry no notification text of other events.

#### Scenario: Neighbours in time
- **GIVEN** a GoPay credit of Rp 500.000 recorded 0.2 seconds earlier and a myBCA debit of Rp 500.000 still queued 3 seconds later
- **WHEN** Jago's "You've received Rp500.000 from GoPay." is interpreted
- **THEN** the context SHALL list both as neighbours with a context-local id, institution, direction, amount, signed seconds from this event, state, account when recorded, and whether each is part of a movement, ordered by time
- **AND** neighbours SHALL be limited to `NOTIFICATION_NEIGHBOUR_WINDOW_SECONDS` (default 300, accepted from 30 to 1800) and 20 entries, nearest first, and SHALL carry no notification text of other events

#### Scenario: Neighbours unavailable
- **WHEN** the neighbour query fails or the neighbours do not fit the context budget
- **THEN** the context SHALL carry `neighbours_incomplete: true` with the neighbours that fit, and processing SHALL continue

### Requirement: Same-Money Proposal
The model output SHALL include `same_money_as`, either null or the context-local id of one neighbour that reports the same money movement as this notification. The backend SHALL treat it only as a proposal. When the deterministic "One Arrival Seen by Two Apps" rule confirms the same neighbour, the event SHALL become a duplicate observation. When it does not, the event SHALL be held `needs_review` with `possible_duplicate_observation` and SHALL book nothing.

#### Scenario: Confirmed proposal
- **WHEN** the model proposes the recorded GoPay credit as `same_money_as` for Jago's "You've received Rp500.000 from GoPay." and the deterministic rule matches the same leg
- **THEN** the event SHALL be recorded as `duplicate_observation` of that leg

#### Scenario: Unconfirmed proposal
- **WHEN** the model proposes a neighbour that differs in amount, direction or account naming from this notification
- **THEN** the event SHALL be held `needs_review` with `possible_duplicate_observation`, no transaction SHALL be created, and the owner SHALL be able to record it from Tinjau

#### Scenario: Unknown or forbidden id
- **WHEN** `same_money_as` is not a supplied neighbour id, names a neighbour that is not recorded yet, or neighbours were incomplete
- **THEN** the proposal SHALL be rejected as `invalid_neighbour_reference`, like `invalid_candidate_reference`, and the event SHALL follow the existing path for a rejected model answer

#### Scenario: Model unavailable
- **WHEN** the model is disabled, fails or returns an unusable answer
- **THEN** the deterministic path SHALL run and the deterministic rule SHALL still prevent the duplicate booking
