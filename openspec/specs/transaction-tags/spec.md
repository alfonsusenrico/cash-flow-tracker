# transaction-tags Specification

## Purpose
Owner-created tags group transactions across categories, such as everything spent for one event, so the ledger can show what that event cost. Tags are attached by hand in Catat and the edit dialog, or by the AI from the owner's own wording in a typed capture, and are managed in Pengaturan.

## Requirements

### Requirement: Owner-Created Tags
The owner SHALL be able to create tags while recording or editing a transaction and attach up to 10 tags to it. Tag names SHALL be unique per owner after lower-casing and collapsing spaces, and a new name equal to an existing one SHALL reuse that tag. Another owner's tags SHALL never be visible or usable.

#### Scenario: Creating a tag while recording
- **WHEN** the owner records "Bayar makan di Kafe Contoh" and types the new tag "fellowship coach"
- **THEN** the tag SHALL be created when the transaction is saved and attached to it

#### Scenario: A differently spelled duplicate reuses the tag
- **GIVEN** the tag "fellowship coach"
- **WHEN** the owner adds the new tag "Fellowship  Coach" to another transaction
- **THEN** the existing tag SHALL be attached and no second tag SHALL be created

#### Scenario: Another owner's tag
- **WHEN** a request attaches a tag ID owned by someone else
- **THEN** it SHALL be refused with `TAG_NOT_FOUND` and nothing SHALL be written

### Requirement: Filtering the Ledger by Tag
The ledger SHALL filter by one or more tags, listing rows that carry any of them, and its summary totals SHALL follow the filter. Rows and the transaction detail SHALL show their tags.

#### Scenario: What an event cost
- **GIVEN** five expenses across Makanan & Minuman and Transportasi tagged "fellowship coach"
- **WHEN** the owner filters the ledger by that tag
- **THEN** exactly those five rows SHALL be listed and the expense total SHALL be their sum

### Requirement: Managing Tags
Pengaturan SHALL list the owner's tags with their row counts and let the owner rename, merge and archive them. A merge SHALL move every link to the kept tag. An archived tag SHALL stay on its rows and in the filter, but SHALL NOT be offered in pickers or to the model.

#### Scenario: Merging a typo
- **GIVEN** tags "fellowship coach" (4 rows) and "felowship coach" (1 row)
- **WHEN** the owner merges the second into the first
- **THEN** "fellowship coach" SHALL have 5 rows and "felowship coach" SHALL no longer exist

### Requirement: Tags in AI Text Capture
Text capture SHALL give the model the owner's active tags and accept from it existing tag IDs or new tag names, each with a span quoted from the text. The backend SHALL drop any reference that is not the owner's active tag, any span that is not in the text, and any invalid name. It SHALL turn a new name that matches an existing tag into that tag.

#### Scenario: A prefix maps to an existing tag
- **GIVEN** the tag "fellowship coach"
- **WHEN** the owner types "fellow coach: bayar makan di Kafe Contoh cash jam 19"
- **THEN** the recorded transaction SHALL carry "fellowship coach", be named from the rest of the text, and use the account Cash and the time 19:00

### Requirement: Capture Tags on Send and Undo
A text capture SHALL be recorded with its tags when the owner sends it, creating any new tag the owner named, and the recorded result SHALL list them. Undoing the capture SHALL also delete a tag it created that no other transaction carries.

#### Scenario: A new tag on instruction
- **WHEN** the owner types "isi bensin 150rb tag-nya Malang trip" and has no such tag
- **THEN** the tag "Malang trip" SHALL be created and attached, and undoing the capture SHALL delete it again when no other transaction carries it

#### Scenario: No tag wording
- **WHEN** the owner types "es teh manis 5rb tunai"
- **THEN** no tag SHALL be proposed
