# topup-learning Specification

## Purpose
Learn, per owner, which investment product a recurring debit (same account, same amount) is a top-up of, from the owner's own conversions and answers, so that Tinjau suggests it, assigns equal debits one per product, and records it automatically once the owner has confirmed it often enough.

## Requirements

### Requirement: Top-up Evidence
The system SHALL learn, per owner, which amount-tracked mutual-fund products a debit pattern (source account and exact amount) is converted into. Evidence SHALL be each transaction's latest state: a live top-up the owner converted from it, or the owner's "Bukan top up" while it is unconverted. Automatic top-ups SHALL NOT be evidence. Existing conversions SHALL count without any data being rewritten. Undoing a conversion or a "Bukan top up" in Tinjau SHALL remove the answer that action stored.

#### Scenario: Existing conversion is the first confirmation
- **GIVEN** a Rp 112.590 debit from "RDN Contoh" converted into "Reksa Dana A" before this change
- **WHEN** another unlinked Rp 112.590 debit from "RDN Contoh" awaits its counterpart
- **THEN** Tinjau SHALL suggest "Reksa Dana A" for it with a basis of 1 confirmation

#### Scenario: Undone conversion
- **GIVEN** the only conversion of a pattern was deleted by the owner
- **WHEN** a matching debit awaits its counterpart
- **THEN** no suggestion SHALL be offered

#### Scenario: Another owner's conversions
- **GIVEN** another owner converted debits with the same amount
- **WHEN** the owner's matching debit awaits its counterpart
- **THEN** no suggestion SHALL be offered

#### Scenario: Different notification text
- **GIVEN** a conversion whose notification match key differs from the current debit's match key
- **WHEN** the queue is requested
- **THEN** that conversion SHALL NOT count for the current debit

#### Scenario: Accepted suggestion undone in Tinjau
- **GIVEN** a debit that the owner first answered "Bukan top up" and then converted by accepting a suggestion for "Reksa Dana A"
- **WHEN** the owner undoes the conversion in Tinjau
- **THEN** the "Bukan top up" SHALL again be that debit's latest state, and the pattern's agreement SHALL equal its value before the conversion

#### Scenario: Bukan top up undone
- **GIVEN** an awaiting debit answered "Bukan top up"
- **WHEN** the owner undoes that answer
- **THEN** the debit SHALL carry no top-up answer and its suggestion SHALL be offered again as before

### Requirement: Per-product Agreement
For each product of a pattern the system SHALL compute its confirmations, their weight and the weight against it (rejections of the pattern and corrections that chose another product). A product SHALL be suggested only when its weight exceeds the weight against it, and SHALL qualify for automatic recording only with at least 3 confirmations and at least 90% of the combined weight. Confirming one product SHALL NOT count against another.

#### Scenario: Rejection outweighs one confirmation
- **GIVEN** one confirmation of "Reksa Dana A" and one "Bukan top up" for the pattern
- **WHEN** a matching debit awaits its counterpart
- **THEN** no suggestion SHALL be offered

#### Scenario: Three confirmations qualify
- **GIVEN** three confirmations of "Reksa Dana A" and nothing against it
- **WHEN** the agreement is computed
- **THEN** the product SHALL qualify for automatic recording, whereas with two confirmations it SHALL only be suggested

#### Scenario: Correction weighs twice
- **GIVEN** three confirmations of "Reksa Dana A" and one debit where the owner chose "Reksa Dana B" instead of a suggested "Reksa Dana A"
- **WHEN** the agreement is computed
- **THEN** "Reksa Dana A" SHALL have 3 of 5 weight and SHALL NOT qualify for automatic recording

### Requirement: Top-up Suggestion in Tinjau
For an unlinked outgoing debit from a liquid account awaiting its counterpart, `GET /api/review/queue` SHALL include `topup_suggestion` when the pattern has a learned product: the assigned product, a prompt such as "Sepertinya top up ke <produk>, kayak bulan lalu?", the basis and an explanation, or, when no single product can be assigned, the products to choose from without a default. `POST /api/review/topup-suggestions/reject` SHALL store the owner's "Bukan top up" for an unlinked expense.

#### Scenario: Reject a suggestion
- **WHEN** the owner rejects the suggestion for a debit and requests the queue
- **THEN** the debit SHALL still be awaiting without `topup_suggestion`, and repeating the rejection SHALL store nothing

#### Scenario: Foreign or linked transaction
- **WHEN** the rejection names another owner's transaction or a linked movement leg
- **THEN** the response SHALL be 404 or 409 respectively and nothing SHALL be stored

### Requirement: One-to-one Assignment
Within a window of ±3 days around a debit, the system SHALL assign pending matching debits, ordered by date and id, to the learned products not yet filled in that window, ordered by agreement weight, support and name, one debit per product, when the two lists have the same size. With a single learned product every pending debit SHALL be assigned to it. Otherwise the owner SHALL be asked to choose and nothing SHALL be assigned.

#### Scenario: Same-amount pair with two learned products
- **GIVEN** "Reksa Dana A" and "Reksa Dana B" each confirmed once for Rp 112.590 from "RDN Contoh"
- **WHEN** two Rp 112.590 debits from "RDN Contoh" in the same second await their counterpart
- **THEN** the earlier-ordered debit SHALL be suggested "Reksa Dana A" and the other "Reksa Dana B", each with an explanation of the rule

#### Scenario: Lone debit with two learned products
- **GIVEN** the same two learned products and a single matching debit
- **WHEN** the queue is requested
- **THEN** the suggestion SHALL offer both products as choices without a default

#### Scenario: Partner already filled
- **GIVEN** the same two learned products and a top-up to "Reksa Dana A" from the pair's first debit
- **WHEN** the second debit awaits its counterpart
- **THEN** it SHALL be suggested "Reksa Dana B"

### Requirement: Automatic Top-up Recording
When a notification records a debit that stays unlinked awaiting its counterpart, the system SHALL convert each pending debit of its window that is assigned to a product qualifying for automatic recording into a top-up to that product, while `TOPUP_AUTO_RECORD_ENABLED` is not `false`. A conversion the system cannot make SHALL leave the debit awaiting.

#### Scenario: Recorded after three confirmations
- **GIVEN** three confirmations of "Reksa Dana A" for Rp 112.590 from "RDN Contoh"
- **WHEN** the companion sends a new "RDN spending of IDR 112,590.00" notification
- **THEN** the debit SHALL be the outgoing leg of a top-up to "Reksa Dana A", classified `habit`, reviewed, absent from Tinjau and its badge
- **AND** replaying the same notification SHALL record nothing more

#### Scenario: Switch off
- **GIVEN** the same evidence and `TOPUP_AUTO_RECORD_ENABLED=false`
- **WHEN** the notification is recorded
- **THEN** the debit SHALL await its counterpart with the suggestion

#### Scenario: Pair recorded one per product
- **GIVEN** "Reksa Dana A" and "Reksa Dana B" each with three confirmations for the pattern
- **WHEN** two matching debits arrive in the same minute
- **THEN** the first SHALL wait until the second arrives, and then one SHALL become a top-up to each product

#### Scenario: Below the threshold
- **GIVEN** two confirmations of "Reksa Dana A"
- **WHEN** a matching notification is recorded
- **THEN** the debit SHALL await its counterpart with the suggestion and no top-up SHALL be created

### Requirement: Automatic Top-up Provenance and Money
An automatically recorded top-up's outgoing leg SHALL be classified `habit` and reviewed and SHALL keep its amount, date, notification link and idempotency key. The funding balance SHALL NOT change and the product's value and invested cost SHALL grow by the amount. No owner feedback or merchant memory SHALL be written, and the basis SHALL be stored on the notification event.

#### Scenario: Money of an automatic top-up
- **GIVEN** an RDN balance and a product value before a matching notification
- **WHEN** the debit is recorded automatically as a top-up
- **THEN** the RDN balance SHALL be lower by the debit only once, the product's value and invested cost SHALL be higher by the amount, and the ledger SHALL report the basis of the record

### Requirement: Top-up Evidence Weights and Window
A confirmation SHALL weigh 1, or 2 when the owner chose it instead of another suggested product; a rejection SHALL weigh 2. Only the latest 10 labels of a pattern SHALL count. When both the evidence's and the current debit's notifications carry a match key, the keys SHALL be equal. Archived, foreign, unit-tracked and parent products SHALL never be suggested.

#### Scenario: Only the latest ten labels count
- **GIVEN** five older rejections and ten newer confirmations of "Reksa Dana A" for a pattern
- **WHEN** the agreement is computed
- **THEN** only the ten confirmations SHALL count and the product SHALL qualify for automatic recording

#### Scenario: Archived product
- **GIVEN** the only learned product of a pattern is archived
- **WHEN** a matching debit awaits its counterpart
- **THEN** no suggestion SHALL be offered
