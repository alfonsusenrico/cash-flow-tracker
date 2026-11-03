# Spec Delta

## MODIFIED Requirements

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
