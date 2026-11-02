# companion-interface Specification

## Purpose
The companion's screens: the status hub and the sheets over it (questions, capture log, details, settings,
status and web links), what the hub counts and never shows, how problems are fixed, how the theme is chosen,
and the accessibility every screen keeps.

## Requirements

### Requirement: Home figures come from this phone's rows
Beranda's own figures SHALL be counts computed only from notifications and typed captures stored on this phone, never money amounts. The status card SHALL count the notifications captured today, and its sync line SHALL count what waits to reach the tracker: notifications not yet sent, typed captures queued or being sent, and answers waiting to be sent. A count not yet read SHALL never show as 0.

#### Scenario: Recorded rows are summed
- **WHEN** today has a recorded expense of Rp187.500 and one of Rp32.000 and a recorded income of Rp250.000
- **THEN** the status card counts the three notifications, and neither Rp219.500, Rp250.000 nor any of the three amounts appears on Beranda

#### Scenario: Unsettled rows are counted, not summed
- **WHEN** today has one unsent notification, one queued typed capture and one queued answer
- **THEN** the sync line says "3 nunggu dikirim" and no amount appears

#### Scenario: The phone is not the tracker
- **WHEN** Beranda is shown
- **THEN** it shows no money figure at all, and the web is where money is read

#### Scenario: Nothing recorded in three days
- **WHEN** no expense with a category was recorded on the phone in the last three days
- **THEN** Beranda shows no category and no zero amounts

#### Scenario: Typed captures are counted with the notifications
- **WHEN** a typed capture waits in the queue
- **THEN** it is counted in the waiting items, and no amount of it appears on Beranda

#### Scenario: Counts tile
- **WHEN** today this phone captured 7 notifications, the last from GoPay at 12.41
- **THEN** the status card says "7 notifikasi ketangkep hari ini · terakhir GoPay, 12.41"

#### Scenario: Counts not yet read
- **WHEN** Beranda is shown before the phone's notification rows or typed captures have been read
- **THEN** the card says "Tangkapan hari ini belum kebaca" or "Antrean belum kebaca", and no count shows 0 and the questions do not say "none" before then

#### Scenario: Nothing captured today
- **WHEN** the phone's rows have been read and none was captured today
- **THEN** the card says "Belum ada notifikasi yang ketangkep hari ini"

### Requirement: Unknown amounts are never shown as zero
A row whose amount neither the tracker nor the phone knows SHALL show "Rp —", and TalkBack SHALL read "Nominal belum jelas".

#### Scenario: Review row without an amount
- **WHEN** a row needs review and carries no amount
- **THEN** the feed and the detail page show "Rp —" and never Rp0

### Requirement: Home tiles open their list
Each of Beranda's tiles and links SHALL be one control of at least 48 dp that opens its sheet: Riwayat opens the capture log, Pengaturan the settings, Buka web the web links, "Rincian" the status details, "Lihat semua" the question sheet, and "Catat transaksi" the capture sheet.

#### Scenario: Perlu cek tile
- **WHEN** the owner opens Riwayat and selects "Perlu cek"
- **THEN** only rows and captures not yet settled are listed

#### Scenario: Listener access is off
- **WHEN** notification access is not granted
- **THEN** Beranda says that it cannot hear notifications and its fix opens Android's notification access page

#### Scenario: Activity tile
- **WHEN** the owner taps "Riwayat"
- **THEN** the capture log opens with the Semua filter selected

### Requirement: Activity feed
Riwayat (the capture log) SHALL list rows grouped by day, newest first, each with its title, its own amount, status badge (recorded with the habit tag, needs an answer, kept on the phone, ignored and the others), account or app, and time. A day heading SHALL name the day only, with no money total. It SHALL offer the filters Semua, Perlu dijawab, Perlu cek, Di ponsel and Diabaikan, with counts, followed by the per-app filters, and SHALL note how many cross-app duplicates are hidden.

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
- **THEN** the question sheet opens at that question; any other row opens its Rincian over Riwayat

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
Opening a row SHALL show Rincian with back and delete, a hero (status, amount, title, date, time and source app), a state box with its action, "Yang dicatat tracker" with "Perbaiki label" and "Lihat di Buku Kas (web)" for a recorded row, the original notification, and "Hapus dari aplikasi" behind a confirmation. A row with an open question SHALL link to the question sheet instead of answering it on the page.

#### Scenario: Review row
- **WHEN** the owner opens a row that needs review
- **THEN** the state box gives the reason, with "Coba Proses Ulang" and "Catat manual"

#### Scenario: Kept and ignored rows
- **WHEN** the owner opens a kept row or an ignored row
- **THEN** the state box offers "Kirim ke Tracker" or "Kirim Ulang"

#### Scenario: Question row
- **WHEN** the owner opens a row with an open question
- **THEN** the state box names the question and "Jawab sekarang" opens it in the question sheet

#### Scenario: Pocket move
- **WHEN** the owner opens a recorded move between pockets
- **THEN** no label correction is offered and "Lihat di Buku Kas (web)" leads to the web

#### Scenario: Delete from the detail page
- **WHEN** the owner confirms "Hapus dari aplikasi"
- **THEN** the page closes and the deletion can be undone

### Requirement: Questions are answered one at a time in Tanya
The question sheet SHALL show one open question per page, urgent questions first: a tag "Perlu sekarang" or "Nanti malam · <time>", then the original notification as a quote (app, day, time and full text), then the question card with status, amount, question and controls, then a pager with "n dari m". A question answered on the page SHALL stay with its state and offer the next question.

#### Scenario: Original text first
- **WHEN** the question sheet shows a purpose question
- **THEN** the full notification text appears above the question

#### Scenario: Paging
- **WHEN** the owner taps "Pertanyaan berikutnya"
- **THEN** the next open question is shown; the button is disabled on the last one

#### Scenario: Question closed elsewhere
- **WHEN** a notification opens a question that is no longer open
- **THEN** the sheet says it is closed and offers "Lihat transaksi"

#### Scenario: Nothing to answer
- **WHEN** no question is open
- **THEN** the sheet says "Semua terjawab"

### Requirement: Theme choice
The companion SHALL follow the system's light or dark setting until the owner chooses Terang or Gelap under Pengaturan → Tampilan. The choice SHALL be stored on the phone, survive restarts and app updates, and "Ikuti sistem" SHALL return to following the system. An unknown stored value SHALL be treated as "Ikuti sistem".

#### Scenario: Owner picks dark
- **WHEN** the owner selects "Gelap"
- **THEN** the app turns dark at once and stays dark after a restart, whatever the system setting

#### Scenario: First run after the update
- **WHEN** 2.0.0 starts for the first time
- **THEN** the theme follows the system

### Requirement: Settings keep every control
Pengaturan SHALL be plain grouped lists: monitored apps, Kirim (auto-sync and "Sinkron sekarang"), Pengingat malam, the connection, Izin & baterai with "Tes HP-ku", Tampilan, and Tentang with the version. The listener figures SHALL be in "Rincian status". A settings page Android does not offer SHALL be reported to the owner.

#### Scenario: Battery page missing
- **WHEN** the phone has no battery optimisation page for the app
- **THEN** the companion opens the general battery page, or says the page is unavailable

#### Scenario: Connection and permissions
- **WHEN** the owner opens Pengaturan
- **THEN** the connection offers the device name, "Tes Koneksi", "Sambungin ulang" and "Pakai kunci API", and Izin & baterai links Android's notification access, battery optimisation and app info pages with this phone's battery-saver steps

### Requirement: Accessible screens
Every control SHALL have a target of at least 48 dp and a TalkBack label, every icon-only control SHALL have a description, page, sheet and card titles SHALL be headings, status SHALL be given in text with an icon and never by colour alone, and money SHALL use tabular figures. Text SHALL reach 4.5:1 contrast and meaningful non-text 3:1 in both themes. Layouts SHALL stay usable at font scale 2.0, and transitions SHALL be removed when Android's animations are off.

#### Scenario: Large text
- **WHEN** the font scale is 2.0
- **THEN** the hub's tiles stack in one column and every control on the hub, Riwayat, Rincian, the question sheet and Pengaturan stays reachable and at least 48 dp high

#### Scenario: Contrast
- **WHEN** the colour tokens of either theme are checked
- **THEN** every text pair reaches 4.5:1 and every meaningful non-text pair 3:1

#### Scenario: Animations off
- **WHEN** Android's animator duration scale is 0
- **THEN** screens change without a transition

#### Scenario: Bottom bar at large text
- **WHEN** the font scale is 2.0 on a phone 320 or 360 dp wide
- **THEN** there is no bottom bar; "Catat transaksi" stays at least 60 dp high and the Riwayat, Pengaturan and Buka web tiles are shown whole, stacked, with their full TalkBack names and targets of at least 48 dp

#### Scenario: Amounts and day headers at large text
- **WHEN** the font scale is 2.0 on a phone 320 or 360 dp wide
- **THEN** a Riwayat day header wraps only between words, keeps its date ("Kamis, 29 Okt") on one line and holds no money total

### Requirement: Rows recorded from a habit say so
When the tracker's result for a recorded expense or income carries a `habit` with a label and a basis, the phone SHALL keep it with the row's stored result, show a "Dari kebiasaan" tag beside the row's status in Riwayat, include "Dari kebiasaan" in the row's TalkBack description, and show "Dicatat dari kebiasaan: <kategori> · <basis>" in Rincian under "Yang dicatat tracker". A result without a complete habit SHALL show neither.

#### Scenario: Habit-recorded expense
- **WHEN** the tracker records Warung Contoh Rp27.000 at 12.10 with the habit label "Dicatat dari kebiasaan: Makan & Minum" and basis "3 dari 3 jawaban sebelumnya"
- **THEN** its Riwayat row shows "Tercatat" and "Dari kebiasaan", TalkBack reads "Warung Contoh, Keluar Rp27.000, Tercatat, Dari kebiasaan, GoPay Contoh, 12.10", and Rincian shows "Dicatat dari kebiasaan: Makan & Minum · 3 dari 3 jawaban sebelumnya"

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

### Requirement: Beranda is one status hub
Beranda SHALL be one screen (direction C, "Satu layar status"): the status card, one large "Catat transaksi" button, a "Laporan siap" card when one applies, the next open question, and the tiles Riwayat, Pengaturan and Buka web, with no bottom bar. The calm card SHALL say "Lagi jagain notifikasimu", today's capture count with the last capture, the sync line and "Rincian". Every other screen SHALL open as a sheet over the hub.

#### Scenario: Calm phone
- **WHEN** the listener is connected, access is granted, the tracker answered and 7 notifications were captured today, the last from GoPay at 12.41
- **THEN** the card says "Lagi jagain notifikasimu", "7 notifikasi ketangkep hari ini · terakhir GoPay, 12.41" and "Kekirim semua, 12.42", and offers "Rincian"

#### Scenario: Counts not yet read
- **WHEN** the hub opens before the phone's rows or queue have been read
- **THEN** the card says "Tangkapan hari ini belum kebaca" and "Antrean belum kebaca", never 0, and the question area says nothing until the questions are read

#### Scenario: Next question in place
- **WHEN** two questions wait, a sender question and a purpose question
- **THEN** the hub says "Ada 2 pertanyaan" with "1 bisa nanti malam, 20.00", shows the sender question first with "Perlu sekarang", answerable in place with its original notification, with the next card peeking behind it, and "Lihat semua" opens the question sheet

#### Scenario: No questions
- **WHEN** no question waits
- **THEN** the hub says "Nggak ada pertanyaan" and "Aku kabarin kalau ada yang baru."

### Requirement: Problems take over the status card with one fix
When something stops or delays capture, the status card SHALL show the one problem that matters most, with its symbol, title, explanation, the sync line and one full-width fix. The order SHALL be: not paired, key refused, notification access lost, listener disconnected or frozen, server unreachable, items waiting to send, a quiet afternoon. A running fix SHALL read "Bentar…", and Catat SHALL step back while a fix shows. Softer issues SHALL be notes with their own button.

#### Scenario: Access lost after an update
- **WHEN** notification access is revoked
- **THEN** the card says "Aku nggak bisa denger notifikasi" and "Nyalain lagi" opens Android's notification access page

#### Scenario: Frozen by HiOS
- **WHEN** the listener disconnected at 09.40 on a TECNO phone
- **THEN** the card says "HP-mu nidurin aku", "Penghemat baterai HiOS ngebekuin aplikasi ini dari jam 09.40. Notifikasi setelah itu bisa kelewat.", shows the Phone Master steps, and "Atur baterai" opens the battery exemption page

#### Scenario: Key refused
- **WHEN** the tracker refused this phone's key
- **THEN** the card says "Sambungan ke akunmu putus" and "Sambungin ulang" opens pairing

#### Scenario: Server unreachable
- **WHEN** the last contact with the tracker failed and 3 items wait
- **THEN** the card says "Tenang, 3 yang nunggu aman di HP. Aku kirim begitu nyambung." and "Coba lagi" sends now and reports health

#### Scenario: Quiet afternoon
- **WHEN** no notification was captured today, it is past 12.00 and no self-test passed today
- **THEN** the card says "Hari ini masih sepi" and "Tes sekarang" runs the self-test; a passing test leaves "Tes oke, aku jalan normal. Mungkin emang belum ada transaksi hari ini." and the card returns to calm

#### Scenario: Waiting offline
- **WHEN** two items wait and the phone is offline
- **THEN** the card says "Lagi offline" and "Kirim sekarang" sends them; online, items waiting more than 15 minutes say "Ada 2 yang belum kekirim"

#### Scenario: Listener disconnected without a battery saver
- **WHEN** the listener is disconnected and the app may run in the background
- **THEN** the card says "Aku lagi putus dari notifikasi" and "Sambungin lagi" asks Android to reconnect the listener

#### Scenario: Soft issues
- **WHEN** the battery is not exempt, notifications are off and auto-sync is off, and nothing else is wrong
- **THEN** the calm card shows three notes with "Izinin", "Nyalain" and "Kirim sekarang"

### Requirement: No money total on the phone
The hub SHALL show no money amount: no "Arus kas tracker", no "Kategori teratas", no inflow, outflow or net, and no amount computed from this phone's rows. The phone SHALL no longer request `GET /api/dashboard/cash-flow-summary` and SHALL delete a summary cached by an older version. Riwayat SHALL show each item's own amount and no day total. The only amounts on the hub SHALL be those of the question being asked.

#### Scenario: Hub without money
- **WHEN** the hub is calm and no question is open
- **THEN** no text on it contains "Rp", "Arus kas tracker" or "Kategori teratas"

#### Scenario: Upgrade from 2.3
- **WHEN** 2.4.0 starts on a phone that cached a tracker summary
- **THEN** the cached summary is removed from the phone

### Requirement: Laporan siap from the tracker
The phone SHALL read `GET /api/reports/monthly/latest` with its Bearer key when the app opens and at most every 6 hours in the background. A ready report with a label, a local start date and a `/laporan` path SHALL show "Laporan <label> udah siap" on the hub until opened, and one "Laporan siap" notification per period that opens the web report. Anything else SHALL show nothing.

#### Scenario: Report ready
- **WHEN** the tracker answers ready for "Oktober 2026" with `/laporan?basis=month&start=2026-10-01`
- **THEN** the hub shows "Laporan Oktober 2026 udah siap", one notification says so, and both open that report in the browser

#### Scenario: Older tracker
- **WHEN** the tracker answers 404, or `ready: false`
- **THEN** no card or notification appears and no error is shown

#### Scenario: A path that is not the report
- **WHEN** the answer's `url` is not a `/laporan` path of the tracker
- **THEN** nothing is stored or opened

### Requirement: Sheets over the hub
Riwayat, Pengaturan, Buka di web, Rincian status and the question sheet SHALL open as sheets over the hub, each with a title heading and "Tutup"; Rincian SHALL open on top of Riwayat with a way back to it. Android's back SHALL step back one sheet, and the hub behind an open sheet SHALL be hidden from TalkBack. "Buka di web" SHALL list Laporan bulanan, Buku Kas, Tinjau and Pengaturan web and confirm before leaving for the browser.

#### Scenario: Rincian from Riwayat
- **WHEN** the owner opens Riwayat, then a recorded row, then "Kembali"
- **THEN** Rincian opens over Riwayat and "Kembali" returns to Riwayat

#### Scenario: Web page confirmed
- **WHEN** the owner taps "Buku Kas" under "Buka di web"
- **THEN** a dialog asks before the browser opens the tracker's `/ledger`

### Requirement: Perbaiki label corrects a recorded notification
Rincian of a recorded notification expense or income of the current pairing SHALL offer "Perbaiki label": category, Kakeibo pillar (expenses only, including "Belum diklasifikasi") and merchant, with nothing preselected. Only what the owner changed SHALL be sent with `PATCH /api/transactions/{id}`, so the tracker records it as owner feedback; amount, account, date and type SHALL NOT be sent. The row's result SHALL then be read again.

#### Scenario: Pillar cleared
- **WHEN** the owner chooses "Belum diklasifikasi" and saves
- **THEN** the phone sends `{"kakeibo_type": null}` to that transaction and says "Label diperbarui. Makasih, aku belajar dari koreksimu."

#### Scenario: Category changed
- **WHEN** the owner picks another expense category and saves
- **THEN** only `category_id` is sent, and the tracker derives the pillar from that category

#### Scenario: Nothing chosen
- **WHEN** the owner saves without choosing anything
- **THEN** nothing is sent and the form says "Pilih kategori, pilar, atau isi merchant dulu."

#### Scenario: Refused
- **WHEN** the tracker answers 409
- **THEN** the label is unchanged and the phone says to change it on the web

### Requirement: Full edits of notification rows are on the web
Rincian of a recorded notification SHALL NOT offer the full transaction form; it SHALL say that amount, account and date are changed on the web and offer "Lihat di Buku Kas (web)". Typed captures SHALL keep "Ubah". "Catat manual" for a row that needs review and "Periksa lalu kirim" for an unsent row SHALL stay.

#### Scenario: Recorded notification
- **WHEN** the owner opens a recorded notification expense
- **THEN** "Perbaiki label" and "Lihat di Buku Kas (web)" are offered and "Ubah transaksi" is not
