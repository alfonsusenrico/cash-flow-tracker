## ADDED Requirements

### Requirement: Jago Moves Into a Pocket Without a Named Source
A model-read Jago move whose text names no source pocket SHALL ask the source-pocket question before any record. The options SHALL be every owned liquid account that holds its own balance, at any institution, except the target, and SHALL carry the model's suggested source when it is an option. The source SHALL never be assumed to be the main pocket.

#### Scenario: Added into a locked pocket asks for the source
- **WHEN** the model reads "You've added Rp47.619 into Bayar Kredit Locked Pocket" as a move into "Bayar Kredit" with no named source and suggests "Tabungan", and the owner has no matching hand-entered move
- **THEN** the event SHALL be `needs_confirmation` with a `source_pocket` question for 47619 IDR whose suggested account is "Tabungan"
- **AND** the options SHALL list the owner's pockets and accounts at other banks and wallets, without "Bayar Kredit" and without an account that only groups pockets
- **AND** no transaction or movement SHALL be written

#### Scenario: Owner answers the source of the move
- **WHEN** the owner answers that question with the pocket "Makan-Jalan"
- **THEN** exactly one linked movement of 47619 IDR from "Makan-Jalan" to "Bayar Kredit" SHALL be recorded with the notification's timestamp
- **AND** the event SHALL be `recorded`

#### Scenario: Owner names an account at another bank
- **WHEN** the owner answers that the money came from "BCA Harian"
- **THEN** the arrival SHALL be recorded in "Bayar Kredit" and completed on "BCA Harian" as one movement, linking a single unlinked matching BCA leg already recorded or creating it
- **AND** when BCA's own notification for that debit arrives later it SHALL attach to the movement without a second debit
- **AND** when several BCA legs could match, the arrival SHALL wait in review for the owner to choose and no movement SHALL be guessed

#### Scenario: Unanswered or stale answer never debits the main pocket
- **WHEN** the question is unanswered, or the answer is stale or names the target pocket
- **THEN** no movement SHALL be recorded and the main pocket SHALL NOT be debited

#### Scenario: Other "into" wording is not a main-pocket move
- **WHEN** a Jago notification says money went into a pocket with a verb other than a move verb
- **THEN** the fixed rules SHALL NOT record it as a move from the main pocket

### Requirement: Hand-Entered Moves Confirmed by an Unnamed-Source Notification
For a move with an unnamed source, the system SHALL confirm a unique hand-entered move into the target pocket of the same amount, dated within 48 hours and not referenced by another notification, before asking for the source and again after the answer. It SHALL write nothing new.

#### Scenario: A hand-entered move is confirmed, not duplicated
- **WHEN** the owner entered a move of Rp130.900 from "Makan-Jalan" to "Bayar Kredit" 57 minutes before Jago reports "You've added Rp130.900 into Bayar Kredit Locked Pocket"
- **THEN** the event SHALL be `recorded` as confirming that movement's inbound role
- **AND** no question SHALL be asked and no transaction SHALL be written

#### Scenario: Several hand-entered moves match
- **WHEN** two unconfirmed hand-entered moves of the same amount into the target pocket fall within 48 hours
- **THEN** the source-pocket question SHALL be asked
- **AND** after the answer the move whose source is the answered pocket SHALL be confirmed if it is the only one, a new movement SHALL be recorded if none is, and the event SHALL be `needs_review` with `ambiguous_existing_movement` if several are
