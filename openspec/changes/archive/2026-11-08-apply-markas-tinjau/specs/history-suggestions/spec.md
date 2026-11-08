## MODIFIED Requirements

### Requirement: Saran Perbaikan Riwayat Section
The `/review` page SHALL offer, in the interface language and regardless of the rest of the queue, history suggestions in the optional folder "Saran" below the rail's rule, shown while suggestions are pending. "Cari saran dari riwayat" in the page's "…" menu SHALL always open a dialog with the period, the estimate before any run, progress while it runs and the last run. Each pending group SHALL open as a Tinjau item with its accept and reject actions.

#### Scenario: Estimate then confirm by keyboard
- **WHEN** the owner opens "Cari saran dari riwayat", presses Enter on "Hitung perkiraan", tabs to "Mulai buat saran" and presses Enter
- **THEN** the first request SHALL carry no `confirm`, the second SHALL carry `confirm: true`, and the dialog SHALL show progress with the processed and selected counts.

#### Scenario: Reject one row
- **WHEN** the owner opens a group's rows and activates "Tolak" on one row
- **THEN** one reject request SHALL carry only that suggestion's id and the status SHALL name the group and the count.

#### Scenario: Failure
- **WHEN** a decision request fails
- **THEN** an alert SHALL show the server's message and no success message SHALL appear.

#### Scenario: Not counted as waiting
- **GIVEN** three pending suggestions and nothing else in Tinjau
- **WHEN** Tinjau loads
- **THEN** "Saran" SHALL show 3 below the rule, the hero and badge SHALL show 0 needing the owner, and the item pane SHALL show "Semua sudah ditinjau"

#### Scenario: Start disabled before an estimate
- **WHEN** the dialog opens and no estimate has been shown for the current period
- **THEN** "Mulai buat saran" SHALL be disabled, and changing a date after an estimate SHALL disable it again

### Requirement: Suggestion Period and Estimate Controls
The dialog SHALL offer a period (default the last three months) checked in the browser like the backend, and an estimate shown before any run with a "Mulai buat saran" confirmation and a "Batal" that returns focus.

#### Scenario: Cancel the estimate
- **WHEN** the owner activates "Batal" on a shown estimate
- **THEN** no run request SHALL be sent and focus SHALL return to "Hitung perkiraan".

#### Scenario: Invalid period
- **WHEN** the owner sets "Sampai tanggal" before "Dari tanggal" and presses "Hitung perkiraan"
- **THEN** no request SHALL be sent, the period field SHALL be marked invalid and the reason SHALL be shown under it

### Requirement: Suggestion Progress and Last Run
The dialog SHALL show progress from the run's real counts while it runs, and the last run's window, counts and the reason when suggestions were limited. A run SHALL continue when the dialog is closed, and reopening the dialog SHALL show its progress or its result.

#### Scenario: Model unavailable
- **GIVEN** the last run completed without the model
- **WHEN** the dialog is shown
- **THEN** it SHALL show that run's window, its counts and why model suggestions were skipped.

#### Scenario: Closed while running
- **GIVEN** a run in progress
- **WHEN** the owner closes the dialog and opens it again
- **THEN** no new run SHALL be requested, and the dialog SHALL show the same run's progress, or its result once it has finished
