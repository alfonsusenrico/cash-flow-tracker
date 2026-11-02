## MODIFIED Requirements

### Requirement: The owner can send a kept notification
The Aktivitas feed SHALL list kept notifications under "Di ponsel", and each kept notification's detail page SHALL offer "Kirim ke Tracker". That action SHALL change the row to the send outcome with an owner reason, and submit it with the next sync. Kept rows SHALL be deleted 7 days after they were posted.

#### Scenario: Owner sends a kept notification
- **WHEN** the owner taps "Kirim ke Tracker" on a kept row
- **THEN** the row SHALL leave the "Di ponsel" list and be submitted to the tracker.

#### Scenario: Kept rows expire
- **WHEN** a kept row was posted more than 7 days ago and has not been sent
- **THEN** the listener SHALL delete it.

### Requirement: Sending an unsent notification offers only what the send carries
The detail page's form for a notification that has not been sent SHALL offer the type, amount and note, and SHALL NOT offer account or category pickers, because the ingest request carries no account or category. It SHALL say that the tracker picks the account and category when it records the notification. Sending SHALL store the type, amount and note as the row's labels and SHALL join a kept row to the send pipeline.

#### Scenario: Unsent row opened
- **WHEN** the owner opens the form of an unsent notification
- **THEN** the form shows no "Rekening" or "Kategori" picker and explains that the tracker picks them

#### Scenario: Unsent row sent
- **WHEN** the owner taps "Kirim Notifikasi ke Tracker" with amount 45000, type expense and note "Kedai Contoh"
- **THEN** exactly those three values are stored and the row is queued for sending

#### Scenario: Recorded or reviewable row opened
- **WHEN** the owner opens the form of a recorded ordinary transaction or of a row that needs review
- **THEN** the account and category pickers are offered, because the update or manual record carries them

#### Scenario: Already sent row
- **WHEN** the send is requested for a row that has been sent meanwhile
- **THEN** nothing is relabelled and the owner is told the notification was already sent

### Requirement: Owner edits keep the captured labels they do not change
When the owner sends an unsent notification or the server accepts a ledger edit from the detail page, the companion SHALL store the type, amount and note, SHALL keep the captured counterparty, and SHALL set the direction from the type only for expense ("out") and income ("in"). Any other type, such as a pocket move, SHALL keep its captured direction.

#### Scenario: Pocket move sent unchanged
- **WHEN** an unsent Jago pocket move labelled transfer, direction internal and counterparty "Contoh Pocket → Kantong Utama" is sent from the detail page
- **THEN** the row keeps the counterparty and the internal direction, so it is submitted as a transfer

#### Scenario: Type changed to income
- **WHEN** an unsent expense with counterparty "Kedai Contoh" is sent as income
- **THEN** the direction becomes "in" and the counterparty stays "Kedai Contoh"

#### Scenario: Ledger edit accepted
- **WHEN** the server accepts an edit of a recorded expense to Rp47.500
- **THEN** the row's amount label becomes 47500 and its counterparty is unchanged
