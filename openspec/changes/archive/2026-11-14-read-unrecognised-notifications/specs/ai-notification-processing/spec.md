## ADDED Requirements

### Requirement: Model Reading of Unrecognised Wording
When the model is available, a notification from a supported app that passes the noise, promotion, headline, currency and amount checks but whose wording the fixed rules do not recognise SHALL be read by the model instead of being held as `settlement_unproven` or ignored. The backend SHALL accept the reading only with verbatim amount and activity quotes, a direction that agrees with any direction cue the rules found, and confidence at or above the review threshold.

#### Scenario: Unrecognised settled wording is read
- **WHEN** Jago reports "You've added Rp47.619 into Bayar Kredit Locked Pocket." and the model is available
- **THEN** the event SHALL be queued for the model rather than held as `settlement_unproven`
- **AND** an accepted reading of a move into "Bayar Kredit Locked" SHALL continue through the normal mapping, source and record checks

#### Scenario: An unsure reading goes to review
- **WHEN** the model reads unrecognised wording with confidence below the review threshold, or quotes words absent from the notification
- **THEN** the event SHALL be `needs_review` with `wording_unconfirmed`
- **AND** no ledger row SHALL be written

#### Scenario: The model judges unrecognised wording as noise
- **WHEN** the model answers `ignored` for an unrecognised reminder such as "Tagihan Rp150.000 jatuh tempo besok"
- **THEN** the event SHALL be `ignored` and no ledger row SHALL be written

#### Scenario: Model unavailable
- **WHEN** unrecognised wording arrives while the model is disabled or misconfigured
- **THEN** the event SHALL be `needs_review` with `settlement_unproven` as before

### Requirement: Stored Model Reading
An accepted reading SHALL be stored on the event with its direction, quoted pocket names, whether a source was named, any suggested source, confidence and prompt version. Later steps of that event SHALL use the stored reading without asking the model again. Retrying the event SHALL discard it.

#### Scenario: Source answer reuses the reading
- **WHEN** the owner answers the source question of a model-read move
- **THEN** the move SHALL be recorded from the stored reading without another model call

#### Scenario: Retry reads afresh
- **WHEN** the owner retries an event with a stored reading
- **THEN** the reading SHALL be cleared and the model SHALL read the notification again

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
- **THEN** the report SHALL score each event as an ordinary observed leg or complete movement against its expected outcome, SHALL leave the duplicate decision to the backend tests, and SHALL name the fixture version `synthetic-notifications-6` and prompt version `notification-interpretation-13`

#### Scenario: Enforcing the evaluation budget
- **WHEN** a requested live synthetic run's conservative estimate exceeds its configured per-run dollar cap
- **THEN** the runner refuses before sending an OpenAI request and reports the estimate and cap without exposing credentials

#### Scenario: Recording the shape of an invalid synthetic reply
- **WHEN** a synthetic benchmark request ends in `provider_invalid_output`
- **THEN** the report row SHALL keep the failure class, field paths with error types, the incomplete reason, output tokens, the reply's outcome and direction when decodable, which reference fields are null, string field lengths, the mappings count and the raw synthetic reply text up to 4,000 characters
- **AND** production processing SHALL NOT capture reply text, because only the benchmark enables that capture

#### Scenario: Scoring an incomplete model record
- **WHEN** a synthetic reply is valid for the sent schema but proposes a `record` without a required reference
- **THEN** the report SHALL count it as schema-valid, as a validation rejection with the code `model_uncertain`, and in a separate incomplete-record count that names the missing reference
- **AND** it SHALL NOT count as correct for classification, references or the validated outcome

#### Scenario: Scoring unrecognised wording
- **WHEN** the corpus contains synthetic wording the fixed rules do not recognise, such as a Jago "added into … Locked Pocket" top-up, a top-up naming its source, an unrecognised income, and reminders or future credits that must stay ignored
- **THEN** each SHALL be sent to the model with `needs_reading` and scored against the expected reading, and a move without a named source SHALL be scored as an owner confirmation whose suggested source is reported, never as a recorded movement
