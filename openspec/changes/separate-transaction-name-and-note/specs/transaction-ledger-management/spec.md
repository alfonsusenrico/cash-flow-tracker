## ADDED Requirements

### Requirement: Transaction Name and Owner Note
Every transaction SHALL have a name, the text shown as its title, and an optional note that only the owner writes. Automatic writers (notification interpretation, text capture, purpose answers, merchant memory, habits) SHALL write the name and SHALL NOT read or write the note. The title SHALL be the name, else the merchant, else the category.

#### Scenario: A remark on an event's spending
- **GIVEN** a recorded expense named "Bayar makan di Kafe Contoh" in Budaya
- **WHEN** the owner adds the note "fellowship coach"
- **THEN** the ledger row SHALL show "Bayar makan di Kafe Contoh" as its title with "fellowship coach" as a second line, and the name SHALL be unchanged

#### Scenario: The AI never writes the note
- **WHEN** a notification or a text capture is recorded
- **THEN** its name SHALL hold the description and its note SHALL be empty
- **AND** the model input SHALL NOT contain any note

#### Scenario: Search finds a note
- **WHEN** the owner searches the ledger for "fellowship"
- **THEN** every transaction whose name, merchant or note contains it SHALL be listed

### Requirement: Existing Notes Become Names
The migration SHALL keep every existing transaction text as its name and SHALL start every note empty, without rewriting rows.

#### Scenario: Migrated rows
- **GIVEN** a row whose notes were "Isi bensin di SPBU Contoh"
- **WHEN** the migration has run
- **THEN** its name SHALL be "Isi bensin di SPBU Contoh" and its note SHALL be empty

### Requirement: Notes Alias for Older Clients
During the compatibility window the API SHALL return `notes` as a copy of `name` and SHALL accept `notes` in requests as the name; when both are sent, `name` SHALL win.

#### Scenario: An older companion resolves an event
- **WHEN** a companion sends `notes: "Parkir Contoh"` when resolving an event
- **THEN** the recorded transaction's name SHALL be "Parkir Contoh" and its note SHALL be empty
