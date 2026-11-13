## ADDED Requirements

### Requirement: Transaction Titles
A transaction that is not a movement or top-up SHALL be titled by its note, else its merchant, else its category, else "Tanpa keterangan". A generic note a notification writes SHALL count as no note when the row has a merchant. When the note is the title and the row has a merchant that the note does not contain (ignoring case), the merchant SHALL lead the row's meta line. The edit dialog's subtitle SHALL follow the same order.

#### Scenario: Note and merchant
- **WHEN** a row has the note "Top up saldo untuk development" and the merchant "fal.ai"
- **THEN** its title SHALL be "Top up saldo untuk development" and its meta SHALL start with "fal.ai"

#### Scenario: Note naming the merchant
- **WHEN** the note is "Top up saldo fal.ai untuk development" and the merchant "fal.ai"
- **THEN** the title SHALL be the note and the meta SHALL NOT repeat "fal.ai"

#### Scenario: Merchant only
- **WHEN** a row has the merchant "Kedai Awan" and no note
- **THEN** its title SHALL be "Kedai Awan"

#### Scenario: Generic notification note
- **WHEN** a notification row has the note "Pengeluaran" and the merchant "Toko Contoh"
- **THEN** its title SHALL be "Toko Contoh"
