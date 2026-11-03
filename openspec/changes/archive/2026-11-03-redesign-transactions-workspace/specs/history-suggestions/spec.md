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
- **THEN** "Saran dari riwayat" SHALL show 3 marked optional, the header and badge SHALL show 0 needing the owner, and Tinjau SHALL show "Semua sudah ditinjau"
