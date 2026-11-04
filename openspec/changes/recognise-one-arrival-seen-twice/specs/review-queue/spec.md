## ADDED Requirements

### Requirement: Seen-Twice Section
`GET /api/review/queue` SHALL return `seen_twice`: duplicate observations and events held with `possible_duplicate_observation` from the last 30 days that the owner has not acknowledged, each with its app, amount, post time and the leg it duplicates. These items SHALL count in `unreviewed` and not in `attention`. Acknowledging SHALL remove an item; recording it separately SHALL create its own leg with an undo token.

#### Scenario: Seen twice
- **GIVEN** Jago's "You've received Rp500.000 from GoPay." recorded as a duplicate observation of a GoPay credit
- **WHEN** the queue is requested
- **THEN** `seen_twice` SHALL list it with the GoPay leg as the kept leg, `counts.unreviewed` SHALL include it and `counts.attention` SHALL NOT
- **AND** "Benar, satu transaksi" SHALL acknowledge it and remove it from the queue, and "Catat terpisah" SHALL record the event as its own leg with an undo token

#### Scenario: Held report without a match
- **GIVEN** an event held with `possible_duplicate_observation` whose proposed neighbour has no recorded leg
- **WHEN** the owner opens it in "Terlihat dua kali"
- **THEN** the first choice SHALL record it as its own transaction, and the other SHALL dismiss it without recording
