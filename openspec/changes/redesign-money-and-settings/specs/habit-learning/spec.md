## ADDED Requirements

### Requirement: Habit List API
`GET /api/habits` SHALL return, for the authenticated owner only and without changing any state, the learned habits, the learned top-up patterns, the auto-record switch states (`auto_record.habits`, `auto_record.topups`) and the thresholds in use, at most 100 entries per list, newest first, with `truncated` when more exist. It SHALL never return a notification text signature, SHALL use a constant number of statements, and SHALL log no merchant name, amount or label.

#### Scenario: Owner scoping
- **WHEN** another owner calls `GET /api/habits`
- **THEN** none of the first owner's entries SHALL be returned

#### Scenario: Not signed in
- **WHEN** the endpoint is called without a session or API key
- **THEN** it SHALL answer 401

#### Scenario: Read only
- **WHEN** the endpoint is called
- **THEN** no row in any table SHALL change and no model provider SHALL be called

### Requirement: Habit List Entries
A habit entry SHALL describe one habit key (account, institution and text signature) and amount band from the owner's live labels, with the same evidence, filters, window and weights as the habit decision. The band SHALL be anchored at the newest label's amount, and labels outside it SHALL form further entries anchored the same way. An entry SHALL carry `key`, `label`, `account`, `category`, `kakeibo`, `amount_anchor`, `confirmations`, `agreeing`, `agreement`, `state` and `last_confirmed_at`.

#### Scenario: Entry fields and states
- **WHEN** the owner calls `GET /api/habits`
- **THEN** each habit's `label` SHALL be the owner-labelled merchant name or "Tanpa nama toko" with the institution's name, and its `state` SHALL be `learning` below three labels, `ready` at the habit threshold, and `conflicting` at three or more labels below the agreement threshold

#### Scenario: A ready habit
- **GIVEN** three live owner labels "Makanan & Minuman" for Rp 30.000, 31.000 and 28.000 at "Kedai Awan" from Dompet Digital
- **WHEN** the owner calls `GET /api/habits`
- **THEN** one entry SHALL have `label` "Kedai Awan", `category.name` "Makanan & Minuman", `confirmations` 3, `agreeing` 3 and `state` "ready"

#### Scenario: Still learning and conflicting
- **WHEN** one key has two labels and another has three labels of which one disagrees
- **THEN** the first SHALL be `learning` and the second `conflicting`

#### Scenario: Undone labels do not count
- **GIVEN** a key with three labels, one of which was undone in Tinjau
- **WHEN** the owner calls `GET /api/habits`
- **THEN** the entry SHALL report `confirmations` 2 and `state` "learning"

#### Scenario: Two amount bands of one merchant
- **WHEN** a merchant has three labels near Rp 30.000 and two near Rp 250.000
- **THEN** the list SHALL hold two entries for it, anchored near Rp 30.000 (`ready`) and near Rp 250.000 (`learning`)

### Requirement: Learned Top-up Entries
A top-up entry SHALL describe one learned pattern (source account and exact amount) from the owner's top-up answers, with `source_account`, `amount`, the leading `product` (`id`, `name`, `parent_name`), `confirmations`, `state` (the same three values with the top-up thresholds) and `last_confirmed_at`.

#### Scenario: A learned top-up
- **GIVEN** two accepted conversions of Rp 112.590 from "RDN Contoh" into "Bibit Contoh · Reksa Dana A"
- **WHEN** the owner calls `GET /api/habits`
- **THEN** `topups` SHALL hold one entry with that source, amount and product, `confirmations` 2 and `state` "learning"

### Requirement: Kebiasaan Settings Section
Pengaturan SHALL have a Kebiasaan section at `/settings/kebiasaan` that shows `GET /api/habits`: the auto-record state of habits and of top-ups as read-only facts controlled by the server setting, a table of habits ("Saat ditemukan", "Dicatat sebagai", "Dikonfirmasi", "Status") and a table of learned top-ups ("Debit", "Dicatat sebagai", "Dikonfirmasi", "Status"). It SHALL offer no edit or forget action, and amounts SHALL respect "Sembunyikan saldo".

#### Scenario: Viewing learned habits
- **WHEN** the owner opens Kebiasaan with one ready habit and one learning top-up
- **THEN** the habit row SHALL read "Kedai Awan · Dompet Digital", "Makanan & Minuman · Keinginan", "3×" and "Dicatat otomatis", and the top-up row SHALL read "Rp 112.590 dari RDN Contoh", "Bibit Contoh · Reksa Dana A", "2×" and "Masih belajar · 2 dari 3"

### Requirement: Kebiasaan Status and Empty States
Each Kebiasaan row SHALL state its status in words: "Dicatat otomatis", "Masih belajar · <n> dari 3", "Jawaban berbeda, tetap ditanya", or "Siap, pencatatan otomatis nonaktif" when the server switch is off. An empty list SHALL say plainly that nothing has been learned yet and how learning happens. A failed load SHALL show an error with a retry, never an empty table.

#### Scenario: Load fails
- **WHEN** `GET /api/habits` fails
- **THEN** the section SHALL show an error with "Coba lagi" and no table

#### Scenario: Nothing learned yet
- **WHEN** the owner has no habit and no top-up pattern
- **THEN** the section SHALL say "Belum ada kebiasaan yang dipelajari." with one sentence on how answers in Tinjau teach the app

#### Scenario: Server switch off
- **WHEN** `HABIT_AUTO_RECORD_ENABLED` is false and a habit is ready
- **THEN** its status SHALL read "Siap, pencatatan otomatis nonaktif" and the auto-record fact SHALL say it is off

### Requirement: Capture Undo Reverts Its Label
Undoing a typed capture SHALL append an `undo` feedback row whose `reverts_feedback_id` names the capture's `capture` feedback row, in the same database transaction as the rest of the undo, and SHALL NOT update or delete the capture row. Habit evidence, purpose suggestions, the question rate and the habit list SHALL then ignore the capture's label through the shared live-label condition. A capture without a feedback row SHALL undo as before.

#### Scenario: Undone capture is not habit evidence
- **GIVEN** an owner-classified capture
- **WHEN** the owner undoes the capture
- **THEN** `classification_feedback` SHALL gain one `undo` row naming the capture row, the capture row SHALL be unchanged, and the capture's label SHALL no longer be a live label for any reader

#### Scenario: Undo fails midway
- **WHEN** the undo's database transaction fails after the `undo` row was written
- **THEN** the `undo` row SHALL be rolled back with the rest, and the capture SHALL stay recorded

#### Scenario: Replay of a capture undo
- **WHEN** the same capture undo is sent twice
- **THEN** only one `undo` row SHALL exist for the capture row
