## ADDED Requirements

### Requirement: Server Text Language
Text the server writes SHALL be in the account's language for every authenticated request, Bearer request, background job, notification ingestion and report, and in the request's preferred supported language (`Accept-Language`, default English) only on unauthenticated endpoints. Responses carrying server text SHALL state their language in `Content-Language`.

#### Scenario: Account language beats the header
- **GIVEN** an English account
- **WHEN** its companion, which sends no `Accept-Language`, or a browser sending `Accept-Language: id-ID` requests the review queue
- **THEN** every server-written question and label SHALL be English and the response SHALL carry `Content-Language: en`

#### Scenario: Background job
- **WHEN** the recurring job records a payment for an English account and an Indonesian account
- **THEN** each row's generated note SHALL be in its own account's language

#### Scenario: Unauthenticated endpoint
- **WHEN** a sign-in with a wrong password sends `Accept-Language: id-ID,id;q=0.9`
- **THEN** `detail.message` SHALL be Indonesian and `Content-Language` SHALL be `id`

### Requirement: Error Contract
Every error response SHALL carry `detail` as `{code, params, message}`: a stable snake_case code, the values the message needs, and a message in the resolved server language. Schema-validation errors SHALL keep the framework's field list. Every code SHALL exist in both server and both web catalogues, and the web SHALL render errors from `code`.

#### Scenario: Former plain-string error
- **WHEN** an Indonesian account deletes a category that does not exist
- **THEN** the response SHALL be 404 with `detail.code` `category_not_found`, `detail.params` `{}` and an Indonesian `detail.message`, and the web SHALL show its `errors.category_not_found` text in the interface language

#### Scenario: Missing code in a catalogue
- **WHEN** a new error code exists on the server but not in the English web catalogue
- **THEN** the code parity test SHALL fail and name the code and the catalogue

#### Scenario: Error with values
- **WHEN** a payment exceeds the available balance
- **THEN** `detail` SHALL carry `code` `insufficient_funds`, `params` with `available_amount` and `required_amount`, and the existing top-level fields SHALL remain

### Requirement: Server-Composed Text Carries Its Code
Every question, suggestion reason and fact the server composes SHALL be rendered in the server text language and SHALL also carry the message code and parameters it was rendered from, so that a client can render it itself. Period labels and system labels SHALL be rendered in the server text language next to the structured values a client renders them from: the period's start and end dates, and a system line's `key` and `label_source`. User data inside composed text SHALL be inserted as stored.

#### Scenario: Purpose question for an English account
- **WHEN** a Rp187.500 payment from BCA Harian without a merchant is recorded for an English account
- **THEN** the question's prompt SHALL read "What was this for? Rp187.500 from BCA Harian" and SHALL carry its code and the parameters `amount` 187500 and `account` "BCA Harian"

#### Scenario: Period label for an English account
- **GIVEN** an English account whose pay cycle started on 25 October and is still open
- **WHEN** it reads the report window of that cycle
- **THEN** the label SHALL read "Pay cycle since 25 Oct · ongoing" and the response SHALL carry the window's start and end dates

#### Scenario: Indonesian text unchanged
- **WHEN** the same payment is recorded for an Indonesian account
- **THEN** the prompt SHALL be byte-identical to the text produced before this change, "Apa ini? Rp187.500 dari BCA Harian"

### Requirement: Quoted Server Text Applies per Language
Where a requirement quotes server-composed text in Indonesian, an Indonesian account SHALL receive exactly that text, and an English account its English counterpart from the server catalogue.

#### Scenario: Learned top-up question
- **WHEN** `topup-learning` would ask "Sepertinya top up ke Reksa Dana A, kayak bulan lalu?"
- **THEN** an Indonesian account SHALL receive exactly that text and an English account "Looks like a top-up to Reksa Dana A, like last month?"

### Requirement: Stored Generated Text Is Written Once
Notes, descriptions and questions that the server or the model generates and stores SHALL be written in the account's language at creation and SHALL NOT be rewritten when the language changes. Rows created before this change SHALL keep their text.

#### Scenario: Language switch after a transfer
- **GIVEN** an Indonesian account whose movement note reads "Pindah saldo ke BCA Harian"
- **WHEN** the owner switches to English and then records another movement to BCA Harian
- **THEN** the first note SHALL still read "Pindah saldo ke BCA Harian" and the new note SHALL read "Transfer to BCA Harian"

### Requirement: AI Output Language
Model-written descriptions and reasons SHALL be requested in the account's language. English output SHALL be used only when enabled by configuration, and that SHALL require English evaluation cases passing offline and an owner-approved held-out benchmark passing. Until then, English accounts SHALL receive the Indonesian output. The Indonesian prompt and its version SHALL be unchanged.

#### Scenario: English output disabled
- **GIVEN** English AI output is not enabled
- **WHEN** a notification for an English account is interpreted
- **THEN** the prompt SHALL be the Indonesian one with its current version, and the description SHALL be in Indonesian

#### Scenario: English output enabled
- **GIVEN** English AI output is enabled after a passing benchmark
- **WHEN** a notification for an English account is interpreted
- **THEN** the prompt version SHALL end in `-en`, the description SHALL be English, and a generic English placeholder such as "Payment" SHALL be rejected like "Pembayaran"

#### Scenario: Indonesian prompt unchanged
- **WHEN** the Indonesian rendering of each prompt is produced
- **THEN** it SHALL be byte-identical to the prompt file before this change

### Requirement: Rendered Text Stays Out of Logs
Logs and error reports SHALL record message keys, error codes, prompt versions, the output language and counts, and SHALL NOT record rendered messages, questions, labels or model output.

#### Scenario: Purpose question log
- **WHEN** a purpose question naming "Kedai Awan" and Rp 125.000 is created and answered for an English account
- **THEN** the captured logs SHALL contain its code and no "Kedai Awan", no "125.000" and no rendered prompt
