## Purpose

The companion's screens: how the owner moves between home, activity, questions and settings, what the
home figures count, how the theme is chosen, and the accessibility every screen keeps.

## ADDED Requirements

### Requirement: Bottom navigation
The companion SHALL show a bottom bar with the tabs Beranda, Aktivitas, Tanya and Atur on its four top-level screens, mark the current tab as selected for TalkBack, and hide the bar on the detail page. The Tanya tab SHALL show the number of open questions when there is at least one.

#### Scenario: Switching tabs
- **WHEN** the owner taps Aktivitas, then Tanya, then Atur, then Beranda
- **THEN** each tab opens its screen and TalkBack announces it as the selected tab

#### Scenario: Open questions are counted
- **WHEN** two questions wait for the owner
- **THEN** the Tanya tab shows 2 and TalkBack reads "Tanya, 2 pertanyaan menunggu"

#### Scenario: No open questions
- **WHEN** no question waits for the owner
- **THEN** the Tanya tab shows no count

### Requirement: Home figures come from this phone's rows
Beranda SHALL compute every figure only from rows stored on this phone: money recorded by the tracker today (out, in, moved), a three-day strip of money out, the top three categories of the last three days, open questions, rows that need checking, and rows kept on the phone. Each money figure SHALL say it comes from this phone.

#### Scenario: Recorded rows are summed
- **WHEN** today has a recorded expense of Rp187.500 and one of Rp32.000 and a recorded income of Rp250.000
- **THEN** "Keluar tercatat hari ini" shows Rp219.500 and "Masuk" shows Rp250.000

#### Scenario: Unsettled rows are counted, not summed
- **WHEN** today has a sender question, a source-pocket question and an unsent capture
- **THEN** "Menunggu" shows 3 notifikasi and none of their amounts is added to a money figure

#### Scenario: The phone is not the tracker
- **WHEN** Beranda is shown
- **THEN** the today tile says "Dari notifikasi di ponsel ini" and the page says the figures are not tracker balances

#### Scenario: Nothing recorded in three days
- **WHEN** no expense with a category was recorded in the last three days
- **THEN** the categories tile says that nothing was recorded instead of listing zero amounts

### Requirement: Unknown amounts are never shown as zero
A row whose amount neither the tracker nor the phone knows SHALL show "Rp —", and TalkBack SHALL read "Nominal belum jelas".

#### Scenario: Review row without an amount
- **WHEN** a row needs review and carries no amount
- **THEN** the feed and the detail page show "Rp —" and never Rp0

### Requirement: Home tiles open their list
Each Beranda tile with an arrow SHALL be one control that opens its list: questions open Tanya at the first open question, sync opens Atur, categories open Aktivitas, "Perlu cek" opens Aktivitas filtered to Perlu cek, and "Di ponsel" opens Aktivitas filtered to Di ponsel.

#### Scenario: Perlu cek tile
- **WHEN** the owner taps the "Perlu cek" tile
- **THEN** Aktivitas opens with the Perlu cek filter selected

#### Scenario: Listener access is off
- **WHEN** notification access is not granted
- **THEN** Beranda says that new transactions are not captured and offers to open Android's notification access page

### Requirement: Activity feed
Aktivitas SHALL list rows grouped by day, newest first, each with its title, amount, status badge, account or app, and time, and each day with its recorded money out. It SHALL offer the filters Semua, Perlu dijawab, Perlu cek, Di ponsel and Diabaikan, with counts, followed by the per-app filters, and SHALL note how many cross-app duplicates are hidden.

#### Scenario: Filter chosen
- **WHEN** the owner selects "Perlu cek"
- **THEN** only rows not yet sent, not yet settled, or settled with an error are listed

#### Scenario: Duplicates note
- **WHEN** two cross-app duplicates are stored and the filter is Semua
- **THEN** the feed says "2 notifikasi kembar disembunyikan (bank sama, dua aplikasi, dalam 2 menit)."

#### Scenario: Empty filter
- **WHEN** a filter has no rows
- **THEN** the feed says "Kosong untuk saringan ini"

#### Scenario: Opening a row
- **WHEN** the owner taps a row with an open question
- **THEN** Tanya opens at that question; any other row opens its detail page

### Requirement: Delete with undo from the feed
Swiping a feed row to the left, or the TalkBack action "Hapus dari aplikasi", SHALL hide the row on the phone and offer "Urungkan", which SHALL restore it. Deleting SHALL never change the tracker.

#### Scenario: Undo
- **WHEN** the owner deletes a row and taps "Urungkan"
- **THEN** the row returns with its previous status

### Requirement: Clearing ignored rows asks first
Under Diabaikan, "Hapus semua" SHALL ask for confirmation before hiding every ignored row on the phone.

#### Scenario: Cancelled
- **WHEN** the owner taps "Hapus semua" and then "Batal"
- **THEN** no row changes

### Requirement: Detail page
Opening a row SHALL show a full page with back and delete, a hero (status, amount, title, date, time and source app), a state box with its action, "Yang dicatat tracker" with "Ubah transaksi" and an inline form, the original notification, and "Hapus dari aplikasi" behind a confirmation. A row with an open question SHALL link to Tanya instead of answering it on the page.

#### Scenario: Review row
- **WHEN** the owner opens a row that needs review
- **THEN** the state box gives the reason, with "Coba Proses Ulang" and "Catat manual"

#### Scenario: Kept and ignored rows
- **WHEN** the owner opens a kept row or an ignored row
- **THEN** the state box offers "Kirim ke Tracker" or "Kirim Ulang"

#### Scenario: Question row
- **WHEN** the owner opens a row with an open question
- **THEN** the state box names the question and "Jawab sekarang" opens it in Tanya

#### Scenario: Pocket move
- **WHEN** the owner opens a recorded move between pockets
- **THEN** no phone edit is offered and the page says it is changed on Tracker Web

#### Scenario: Delete from the detail page
- **WHEN** the owner confirms "Hapus dari aplikasi"
- **THEN** the page closes and the deletion can be undone

### Requirement: Questions are answered one at a time in Tanya
Tanya SHALL show one open question per page, newest first: first the original notification as a quote (app, day, time and full text), then the question card with status, amount, question and controls, then a pager with "n dari m". A question answered on the page SHALL stay with its state and offer the next question.

#### Scenario: Original text first
- **WHEN** Tanya shows a purpose question
- **THEN** the full notification text appears above the question

#### Scenario: Paging
- **WHEN** the owner taps "Pertanyaan berikutnya"
- **THEN** the next open question is shown; the button is disabled on the last one

#### Scenario: Question closed elsewhere
- **WHEN** a notification opens a question that is no longer open
- **THEN** Tanya says it is closed and offers "Lihat transaksi"

#### Scenario: Nothing to answer
- **WHEN** no question is open
- **THEN** Tanya says "Semua terjawab"

### Requirement: Theme choice
The companion SHALL follow the system's light or dark setting until the owner chooses Terang or Gelap under Atur → Tampilan. The choice SHALL be stored on the phone, survive restarts and app updates, and "Ikuti sistem" SHALL return to following the system. An unknown stored value SHALL be treated as "Ikuti sistem".

#### Scenario: Owner picks dark
- **WHEN** the owner selects "Gelap"
- **THEN** the app turns dark at once and stays dark after a restart, whatever the system setting

#### Scenario: First run after the update
- **WHEN** 2.0.0 starts for the first time
- **THEN** the theme follows the system

### Requirement: Settings keep every control
Atur SHALL show listener health tiles and all listener figures, the auto-sync switch, "Sinkron sekarang", a switch for every monitored app, the connection fields, links to Android's notification access, battery optimisation and app info pages with the HiOS note, and the theme choice. A settings page Android does not offer SHALL be reported to the owner.

#### Scenario: Battery page missing
- **WHEN** the phone has no battery optimisation page for the app
- **THEN** the companion opens the general battery page, or says the page is unavailable

### Requirement: Accessible screens
Every control SHALL have a target of at least 48 dp and a TalkBack label, every icon-only control SHALL have a description, page and card titles SHALL be headings, status SHALL be given in text with an icon and never by colour alone, and money SHALL use tabular figures. Text SHALL reach 4.5:1 contrast and meaningful non-text 3:1 in both themes. Layouts SHALL stay usable at font scale 2.0, and transitions SHALL be removed when Android's animations are off.

#### Scenario: Large text
- **WHEN** the font scale is 2.0
- **THEN** Beranda's tiles stack in one column and every control on Beranda, Aktivitas, Rincian, Tanya and Atur stays reachable and at least 48 dp high

#### Scenario: Contrast
- **WHEN** the colour tokens of either theme are checked
- **THEN** every text pair reaches 4.5:1 and every meaningful non-text pair 3:1

#### Scenario: Animations off
- **WHEN** Android's animator duration scale is 0
- **THEN** screens change without a transition
