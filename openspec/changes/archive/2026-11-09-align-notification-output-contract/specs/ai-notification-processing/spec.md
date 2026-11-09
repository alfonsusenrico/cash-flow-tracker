## ADDED Requirements

### Requirement: Structured Output Validation Matches the Sent Schema
The provider boundary SHALL accept as a well-formed proposal every completed reply that satisfies the JSON schema sent with the request, and SHALL report `provider_invalid_output` only for a reply that violates that schema, is empty, is not JSON, is a refusal, is incomplete, or is not a readable response. Rules the sent schema cannot express SHALL be applied by backend validation after parsing, never as an invalid provider output.

#### Scenario: Schema-valid record without a category
- **GIVEN** the model replies `outcome` `record`, direction `expense`, an owned `account_id`, `category_id` null and every other field valid for the sent schema
- **WHEN** the reply is parsed
- **THEN** the provider SHALL return the proposal without `provider_invalid_output`
- **AND** backend validation SHALL reject it with the existing code `model_uncertain` before any financial write

#### Scenario: Reply longer than the sent bound
- **WHEN** a completed reply has a 161-character `description` while the sent schema allows at most 160
- **THEN** the provider SHALL raise `provider_invalid_output` with the failure class `schema_mismatch`, the field `description` and the error type `string_too_long`
- **AND** no ledger row SHALL be created from that reply

#### Scenario: Refusal
- **WHEN** the reply contains a refusal item and no output text
- **THEN** the provider SHALL raise `provider_invalid_output` with the failure class `refusal`

#### Scenario: Incomplete response
- **WHEN** the response status is `incomplete` with the reason `max_output_tokens` or `content_filter`
- **THEN** the provider SHALL raise `provider_invalid_output` with the failure class `incomplete` and that reason
- **AND** any partial text SHALL NOT be parsed

#### Scenario: Reply that is not JSON
- **WHEN** a completed reply's text cannot be decoded as JSON
- **THEN** the provider SHALL raise `provider_invalid_output` with the failure class `json_decode`

#### Scenario: Unreadable response body
- **WHEN** the response body has no status or cannot be read as a Responses API object
- **THEN** the provider SHALL raise `provider_invalid_output` with the failure class `malformed_response`

### Requirement: Content-Free Invalid Output Diagnostics
For every `provider_invalid_output` the system SHALL record only a content-free diagnosis: the failure class, the incomplete reason when present, schema field paths with their validation error types, output token count, model, reasoning effort and prompt version. It SHALL NOT log or store reply text, notification text, field values or provider error bodies. No new stored or API-visible error code SHALL be introduced.

#### Scenario: Diagnosing a schema mismatch in production
- **GIVEN** a real notification whose reply fails the sent schema at `description`
- **WHEN** the worker handles the failure
- **THEN** the log line SHALL contain the event identifier, `schema_mismatch`, `description:string_too_long`, the output token count, model, effort and prompt version
- **AND** it SHALL NOT contain the description, the merchant, the notification text or any other reply value
- **AND** the event SHALL continue through the existing retry and fallback with the existing error codes

#### Scenario: Field paths never carry values
- **WHEN** a reply fails inside `mappings`
- **THEN** the recorded field path SHALL use schema property names and list positions only, such as `mappings.0.alternatives`
- **AND** a key the schema does not define SHALL be recorded as `?`, never by its name

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

#### Scenario: Recording the shape of an invalid synthetic reply
- **WHEN** a synthetic benchmark request ends in `provider_invalid_output`
- **THEN** the report row SHALL keep the failure class, field paths with error types, the incomplete reason, output tokens, the reply's outcome and direction when decodable, which reference fields are null, string field lengths, the mappings count and the raw synthetic reply text up to 4,000 characters
- **AND** production processing SHALL NOT capture reply text, because only the benchmark enables that capture

#### Scenario: Scoring an incomplete model record
- **WHEN** a synthetic reply is valid for the sent schema but proposes a `record` without a required reference
- **THEN** the report SHALL count it as schema-valid, as a validation rejection with the code `model_uncertain`, and in a separate incomplete-record count that names the missing reference
- **AND** it SHALL NOT count as correct for classification, references or the validated outcome
