## MODIFIED Requirements

### Requirement: Web Catat Dialog
The web "+ Catat" action, the phone tab and the N key SHALL open one capture dialog on "Tulis transaksi", which previews the typed text through the parse endpoint, refuses text without exactly one amount, records through `POST /api/capture/text` declaring the question types it answers, asks follow-up questions inline and offers "Urungkan" within the undo window. "Isi manual" SHALL turn the same dialog into the manual form.

#### Scenario: Typing shows the reading
- **WHEN** the owner types "parkir 5rb jam 2"
- **THEN** within a second the dialog SHALL show the "Terbaca" box with −Rp 5.000 in the expense colour, Jenis Pengeluaran, the account or "ditentukan saat dicatat", and Tanggal 14.00, without recording anything

#### Scenario: Text without an amount is refused
- **WHEN** the owner types "bayar parkir"
- **THEN** "Catat" SHALL be disabled and the dialog SHALL say "Jumlah belum ada. Tulis jumlahnya, misalnya 5rb." ("No amount yet. Write the amount, for example 5k.")

#### Scenario: Recording with a follow-up question
- **GIVEN** the owner has no memory for "pulsa" and no account was named
- **WHEN** the owner records "beli pulsa 50rb"
- **THEN** the dialog SHALL ask "Dari rekening mana?" with account choices in the same dialog, and answering SHALL record the expense and close the dialog with a toast offering "Urungkan"

#### Scenario: Undo right after recording
- **WHEN** the owner presses "Urungkan" in the toast within 10 seconds
- **THEN** the capture SHALL be undone through `/capture/{id}/undo`, the row SHALL be gone, and balances SHALL be as before

#### Scenario: Retry after a network failure
- **WHEN** recording fails with a network error and the owner presses "Catat" again
- **THEN** the dialog SHALL resend the same `client_capture_id`, and at most one transaction SHALL exist

#### Scenario: Manual form fallback
- **WHEN** the owner chooses "Isi manual" after typing "parkir 5rb"
- **THEN** the same dialog SHALL show "Catat transaksi" with the amount 5.000 filled in, no second dialog SHALL open, and "Tulis kalimat" SHALL return to the sentence with "parkir 5rb" still typed

#### Scenario: Preview unavailable
- **WHEN** the parse endpoint fails or answers 429
- **THEN** the dialog SHALL say "Belum bisa membaca sekarang. Teks tetap bisa dicatat." ("Cannot read it right now. You can still record the text.") and "Catat" SHALL stay available

#### Scenario: Examples
- **WHEN** the owner chooses an example under "Contoh"
- **THEN** the field SHALL hold that sentence with focus in it, and the preview SHALL show an amount for it

#### Scenario: English interface
- **WHEN** an English account opens Catat
- **THEN** the dialog SHALL read "Write a transaction", its examples SHALL be English sentences the parser reads, and no untranslated notice or `lang="id"` region SHALL appear
