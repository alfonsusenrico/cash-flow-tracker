# Spec Delta

## MODIFIED Requirements

### Requirement: Saran Perbaikan Riwayat Section
The `/review` page SHALL offer, in Indonesian and regardless of the rest of the queue, history suggestions as a folded section "Saran dari riwayat" marked optional, shown while suggestions are pending. A "Cari saran dari riwayat" control SHALL always open the period, the estimate before any run, progress while it runs and the last run. Each pending group SHALL open as a Tinjau item with its accept and reject actions.

#### Scenario: Estimate then confirm by keyboard
- **WHEN** the owner presses Enter on "Hitung perkiraan", tabs to "Mulai buat saran" and presses Enter
- **THEN** the first request SHALL carry no `confirm`, the second SHALL carry `confirm: true`, and progress SHALL show the processed and selected counts.

#### Scenario: Reject one row
- **WHEN** the owner opens a group's rows and activates "Tolak" on one row
- **THEN** one reject request SHALL carry only that suggestion's id and the status SHALL name the group and the count.

#### Scenario: Failure
- **WHEN** a decision request fails
- **THEN** an alert SHALL show the server's message and no success message SHALL appear.

#### Scenario: Not counted as waiting
- **GIVEN** three pending suggestions and nothing else in Tinjau
- **WHEN** Tinjau loads
- **THEN** "Saran dari riwayat" SHALL show 3 marked optional, the header and badge SHALL show 0 needing the owner, and Tinjau SHALL show "Semua beres"

## ADDED Requirements

### Requirement: Suggestion Decisions Can Be Undone
Accepting or rejecting suggestions SHALL return an undo token when at least one suggestion was decided. Undoing an accept SHALL restore the rows' earlier values and review state and return the suggestions to pending. Undoing a reject SHALL return them to pending. A suggestion decided again in between SHALL keep its current status.

#### Scenario: Undo an accept
- **GIVEN** the owner accepted both "Kedai Awan" suggestions
- **WHEN** the owner undoes the accept
- **THEN** both rows SHALL carry their values before the accept, both suggestions SHALL be pending, and the two `accepted_suggestion` feedback rows SHALL no longer count for any reader

#### Scenario: Undo a reject
- **GIVEN** the owner rejected one suggestion
- **WHEN** the owner undoes the reject
- **THEN** the suggestion SHALL be pending and the row SHALL be unchanged

#### Scenario: Only skipped ids
- **WHEN** an accept skips every id because none is pending
- **THEN** `undo_token` SHALL be `null`
