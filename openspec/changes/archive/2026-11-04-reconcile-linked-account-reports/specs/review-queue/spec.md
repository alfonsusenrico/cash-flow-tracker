## ADDED Requirements

### Requirement: Skipped by a Linked Account
`GET /api/review/queue` SHALL return `skipped`: the owner's events skipped by an account link in the last 30 days, newest first, each with its app, direction, amount, post time, the kept leg and the link. They SHALL count only in `counts.skipped`, never in `unreviewed` or `attention`. Tinjau SHALL show them in a folded "Dilewati otomatis" list with "Catat terpisah".

#### Scenario: A skipped Jago report
- **GIVEN** the link "Gopay juga dilaporkan Bank Jago" and a Jago report skipped as the second report of a GoPay credit
- **WHEN** the owner opens Tinjau
- **THEN** "Dilewati otomatis" SHALL list it with the GoPay leg and the link, `counts.skipped` SHALL be 1 and `counts.unreviewed` SHALL NOT include it

#### Scenario: Recording a skipped report separately
- **WHEN** the owner chooses "Catat terpisah" on that item
- **THEN** the event SHALL be recorded as its own leg with an undo token, the link SHALL stay active, and "Urungkan" SHALL make it a skipped report of the same link again

#### Scenario: Another owner's skipped report
- **WHEN** another owner requests `POST /api/review/seen-twice/{event_id}/record-separately` for that event
- **THEN** the response SHALL be 404 and nothing SHALL change

## MODIFIED Requirements

### Requirement: Seen-Twice Section
`GET /api/review/queue` SHALL return `seen_twice`: duplicate observations from the last 30 days that the owner has not acknowledged and that no account link skipped, each with its app, amount, post time and the leg it duplicates. These items SHALL count in `unreviewed` and not in `attention`. Acknowledging SHALL remove an item; recording it separately SHALL create its own leg with an undo token.

#### Scenario: Seen twice
- **GIVEN** Jago's "You've received Rp500.000 from GoPay." recorded as a duplicate observation of a GoPay credit
- **WHEN** the queue is requested
- **THEN** `seen_twice` SHALL list it with the GoPay leg as the kept leg, `counts.unreviewed` SHALL include it and `counts.attention` SHALL NOT
- **AND** "Benar, satu transaksi" SHALL acknowledge it and remove it from the queue, and "Catat terpisah" SHALL record the event as its own leg with an undo token

#### Scenario: Held report without a match
- **GIVEN** an event held with `possible_duplicate_observation` by the earlier prompt `notification-interpretation-11`
- **WHEN** migration V46 runs
- **THEN** the event SHALL be queued again and decided by the deterministic path, and `seen_twice` SHALL never list a held report

## REMOVED Requirements

### Requirement: Skipped by a Pattern
**Reason**: Skip patterns are retired. Skipped reports now come from account links (Skipped by a Linked Account).
**Migration**: V47 points events skipped by a converted pattern at its link.
