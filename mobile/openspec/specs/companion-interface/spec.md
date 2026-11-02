# companion-interface Specification

## Purpose
The companion's screens: how the owner moves between home, activity, questions and settings, what the
home figures count, how the theme is chosen, and the accessibility every screen keeps.

## Requirements

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
Beranda's own figures SHALL be counts computed only from notifications and typed captures stored on this phone, never money amounts. The "Aktivitas ponsel hari ini" tile SHALL count today's captured notifications, those waiting for the tracker or not yet sent, and typed captures waiting, SHALL say that it comes from this phone, and SHALL open Aktivitas. Open questions, items to check and items kept on the phone keep their own count tiles. A count not yet read SHALL show "—", never 0.

#### Scenario: Recorded rows are summed
- **WHEN** today has a recorded expense of Rp187.500 and one of Rp32.000 and a recorded income of Rp250.000
- **THEN** "Aktivitas ponsel hari ini" counts the three notifications, and neither Rp219.500, Rp250.000 nor any of the three amounts appears on Beranda

#### Scenario: Unsettled rows are counted, not summed
- **WHEN** today has a sender question, a source-pocket question and an unsent capture
- **THEN** "Menunggu" shows 3 notifikasi and none of their amounts appears on Beranda

#### Scenario: The phone is not the tracker
- **WHEN** Beranda is shown
- **THEN** the activity tile says "Dari notifikasi dan catatan di ponsel ini" and the page says that money comes only from the tracker and that the counts come from this phone

#### Scenario: Nothing recorded in three days
- **WHEN** no expense with a category was recorded on the phone in the last three days
- **THEN** Beranda shows no category computed on the phone and no zero amounts

#### Scenario: Typed captures are counted with the notifications
- **WHEN** today has a recorded notification expense of Rp32.000, a recorded typed expense of Rp10.000, a recorded typed move of Rp200.000 and one typed capture waiting in the queue
- **THEN** "Menunggu" adds "· 1 catatan", and none of Rp32.000, Rp10.000, Rp42.000 or Rp200.000 appears on Beranda

#### Scenario: Counts tile
- **WHEN** today this phone captured 7 notifications, 3 of them wait or are unsent, and 1 typed capture waits
- **THEN** "Aktivitas ponsel hari ini" shows 7 "notifikasi tertangkap", "Menunggu 3 notifikasi · 1 catatan", "Dari notifikasi dan catatan di ponsel ini", and tapping it opens Aktivitas with Semua

#### Scenario: Counts not yet read
- **WHEN** Beranda is shown before the phone's notification rows or typed captures have been read
- **THEN** the activity tile, "Perlu cek" and "Di ponsel" show "—" for each unread count and TalkBack reads "belum dimuat", and "Pertanyaan" says "Belum dimuat"; no tile shows 0 or "Semua terjawab" before then

#### Scenario: Nothing captured today
- **WHEN** the phone's rows have been read and none was captured today
- **THEN** the activity tile shows 0 notifikasi tertangkap and "Menunggu 0 notifikasi"

### Requirement: Unknown amounts are never shown as zero
A row whose amount neither the tracker nor the phone knows SHALL show "Rp —", and TalkBack SHALL read "Nominal belum jelas".

#### Scenario: Review row without an amount
- **WHEN** a row needs review and carries no amount
- **THEN** the feed and the detail page show "Rp —" and never Rp0

### Requirement: Home tiles open their list
Each Beranda tile with an arrow SHALL be one control that opens its list: questions open Tanya at the first open question, sync opens Atur, the phone's activity opens Aktivitas, "Perlu cek" opens Aktivitas filtered to Perlu cek, and "Di ponsel" opens Aktivitas filtered to Di ponsel.

#### Scenario: Perlu cek tile
- **WHEN** the owner taps the "Perlu cek" tile
- **THEN** Aktivitas opens with the Perlu cek filter selected

#### Scenario: Listener access is off
- **WHEN** notification access is not granted
- **THEN** Beranda says that new transactions are not captured and offers to open Android's notification access page

#### Scenario: Activity tile
- **WHEN** the owner taps "Aktivitas ponsel hari ini"
- **THEN** Aktivitas opens with the Semua filter selected

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

#### Scenario: Bottom bar at large text
- **WHEN** the font scale is 2.0 on a phone 320 or 360 dp wide
- **THEN** every bottom-bar label (Beranda, Aktivitas, Catat, Tanya, Atur) is shown whole on one line at the scaled size, or reduced only as far as the word needs and never below its size at font scale 1.0, never cut or broken, with its full TalkBack name and a target of at least 48 dp

#### Scenario: Amounts and day headers at large text
- **WHEN** the font scale is 2.0 on a phone 320 or 360 dp wide
- **THEN** no amount on Beranda or in Aktivitas' day headers breaks across lines: a label such as "Dikurangi penggantian" wraps or puts its amount below it, with TalkBack text unchanged, and a day header wraps only between words, keeps its date ("Kamis, 29 Okt") on one line and puts "Keluar …" below it

### Requirement: Beranda money comes only from the tracker's summary
The only money amounts on Beranda SHALL come from the tracker's cash-flow summary for today, read from `GET /api/dashboard/cash-flow-summary?period=today` with the phone's Bearer key: its totals in the "Arus kas tracker" tile and its top categories in "Kategori teratas", each with the summary's period and freshness. The phone SHALL never add its own rows or captures to these amounts, nor show on Beranda a money total computed from them.

#### Scenario: No local money amount on Beranda
- **WHEN** today has recorded notification and typed expenses, incomes and moves on the phone, and the tracker summary says Rp200.000 out
- **THEN** Beranda shows "Uang keluar tracker Rp200.000" and no money amount other than the tracker's: no "Keluar tercatat hari ini", no local Masuk or Pindah amount, no three-day money bars and no local category amount

#### Scenario: Summary just refreshed
- **WHEN** the summary request succeeds
- **THEN** the tile shows money out, Masuk and Neto, and "Keluar kotor" and "Dikurangi penggantian" when reimbursements apply, then "Periode" with the start, end and timezone, and "Diperbarui" with the time the tracker generated it

#### Scenario: Summary restored from the phone
- **WHEN** Beranda opens with a stored summary for the current pairing and the refresh has not finished
- **THEN** the tile shows the stored amounts with "Tersimpan di ponsel, terakhir diperbarui" and its time, and the refresh button reads "Memperbarui arus kas…" and is disabled

#### Scenario: Refresh failed or offline
- **WHEN** a summary is shown and the next refresh fails with a server error or no connection
- **THEN** the tile keeps the last amounts, says "Belum diperbarui. Terakhir berhasil" with its time, and gives the reason ("Arus kas belum dapat diperbarui." or "Tidak terhubung ke tracker.")

#### Scenario: No summary available
- **WHEN** the phone is not paired, the key is refused, or no summary was ever received
- **THEN** the tile says why ("Hubungkan tracker untuk melihat arus kas.", "Hubungkan kembali tracker untuk melihat arus kas." or "Arus kas belum tersedia.") and shows no amount, never Rp0

### Requirement: Top categories come from the tracker or are not shown
Beranda SHALL show "Kategori teratas" only from the `top_categories` the tracker sends with its cash-flow summary: at most three categories, one of which may be the spending without a category, with their amount, Kakeibo pillar and a bar relative to the largest, and the summary's period and freshness, stored, restored and marked stale with the summary. When the field is absent or breaks its contract, Beranda SHALL show no categories tile, and the phone SHALL never compute category totals.

#### Scenario: Tracker offers no categories for the period
- **WHEN** Beranda is shown with a summary from a tracker that does not send `top_categories`
- **THEN** Beranda shows no "Kategori teratas" tile and no category amount

#### Scenario: Recorded categories on the phone
- **WHEN** the phone holds recorded expenses with categories from the last three days
- **THEN** no category name or amount from them appears on Beranda

#### Scenario: Tracker sends its top categories
- **WHEN** the summary's `top_categories` are Belanja Rumah Rp410.000 (need), Hiburan Contoh Rp150.000 (want) and Lain-lain Rp80.000 (no pillar)
- **THEN** "Kategori teratas" lists them in that order with their amounts, "Kebutuhan" and "Keinginan", bars relative to Rp410.000, the summary's "Periode" and "Diperbarui" lines, and TalkBack reads the first as "Belanja Rumah, Rp410.000, Kebutuhan"

#### Scenario: Spending without a category
- **WHEN** the summary's `top_categories` are Belanja Rumah Rp410.000 (need), an entry with a null `category_id` named "Tanpa kategori" Rp150.000, and Lain-lain Rp80.000
- **THEN** "Kategori teratas" lists "Tanpa kategori" second with Rp150.000, its bar and no pillar, TalkBack reads it as "Tanpa kategori, Rp150.000", and it is stored and restored with its null id
- **AND** the same entry with a blank or missing name, or with a pillar, is shown and read the same way

#### Scenario: No spending in the period
- **WHEN** the summary's `top_categories` is an empty list
- **THEN** the tile says "Belum ada pengeluaran pada periode ini" and shows no Rp0

#### Scenario: Stored or not updated
- **WHEN** the summary shown was restored from the phone, or the last refresh failed
- **THEN** the tile shows the stored categories with the same "Tersimpan di ponsel, terakhir diperbarui" or "Belum diperbarui. Terakhir berhasil" line and reason as the "Arus kas tracker" tile

#### Scenario: Categories that break the contract
- **WHEN** `top_categories` holds more than three entries, entries not sorted by outflow descending, a negative or missing outflow, a blank name on an entry with a category id, an unknown pillar, or more than one entry with a null `category_id`
- **THEN** Beranda shows no "Kategori teratas" tile and still shows the summary's totals

### Requirement: Rows recorded from a habit say so
When the tracker's result for a recorded expense or income carries a `habit` with a label and a basis, the phone SHALL keep it with the row's stored result, show a "Dari kebiasaan" tag beside the row's status in Aktivitas, include "Dari kebiasaan" in the row's TalkBack description, and show "Dicatat dari kebiasaan: <kategori> · <basis>" in Rincian under "Yang dicatat tracker". A result without a complete habit SHALL show neither.

#### Scenario: Habit-recorded expense
- **WHEN** the tracker records Warung Contoh Rp27.000 at 12.10 with the habit label "Dicatat dari kebiasaan: Makan & Minum" and basis "3 dari 3 jawaban sebelumnya"
- **THEN** its Aktivitas row shows "Tercatat" and "Dari kebiasaan", TalkBack reads "Warung Contoh, Keluar Rp27.000, Tercatat, Dari kebiasaan, GoPay Contoh, 12.10", and Rincian shows "Dicatat dari kebiasaan: Makan & Minum · 3 dari 3 jawaban sebelumnya"

#### Scenario: Tracker without habits
- **WHEN** a tracker older than 1.8.0 sends a recorded result without `habit`
- **THEN** the row is recorded as before, with no "Dari kebiasaan" tag and no habit line in Rincian

#### Scenario: Incomplete habit
- **WHEN** the result's `habit` lacks its label or its basis, or either is blank
- **THEN** no tag or habit line is shown and the row is still recorded

#### Scenario: Habit no longer reported
- **WHEN** a later result for the same row no longer carries `habit`, because the owner changed the transaction
- **THEN** the tag and the Rincian line disappear

#### Scenario: Habit tag at large text
- **WHEN** the font scale is 2.0, in the light or the dark theme
- **THEN** the "Dari kebiasaan" tag is shown whole beside the status with its icon and text, and the row's TalkBack description still includes it
