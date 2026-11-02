## ADDED Requirements

### Requirement: Beranda money comes only from the tracker's summary
The only money amounts on Beranda SHALL be the tracker's cash-flow summary for today, read from `GET /api/dashboard/cash-flow-summary?period=today` with the phone's Bearer key and shown in the "Arus kas tracker" tile with its period and freshness. The phone SHALL never add its own rows or captures to these amounts, nor show on Beranda a money total computed from them.

#### Scenario: No local money amount on Beranda
- **WHEN** today has recorded notification and typed expenses, incomes and moves on the phone, and the tracker summary says Rp200.000 out
- **THEN** Beranda shows "Uang keluar tracker Rp200.000" and no other money amount: no "Keluar tercatat hari ini", no local Masuk or Pindah amount, no three-day money bars and no local category amount

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
Beranda SHALL show top categories only from per-category amounts the tracker reports for the same period as its cash-flow summary, with that period and freshness. While the tracker offers no such figures, Beranda SHALL show no categories tile, and the phone SHALL never compute category totals from its own rows or captures for Beranda.

#### Scenario: Tracker offers no categories for the period
- **WHEN** Beranda is shown and the tracker's API gives per-category amounts only for a salary cycle, 30 or 90 days or a month, not for today
- **THEN** Beranda shows no "Kategori teratas" tile and no category amount

#### Scenario: Recorded categories on the phone
- **WHEN** the phone holds recorded expenses with categories from the last three days
- **THEN** no category name or amount from them appears on Beranda

## MODIFIED Requirements

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
- **WHEN** no expense with a category was recorded in the last three days
- **THEN** Beranda shows no categories tile and no zero amounts

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
