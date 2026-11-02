## ADDED Requirements

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

## MODIFIED Requirements

### Requirement: Top categories come from the tracker or are not shown
Beranda SHALL show "Kategori teratas" only from the `top_categories` the tracker sends with its cash-flow summary: at most three categories with their amount, Kakeibo pillar and a bar relative to the largest, and the summary's period and freshness. They SHALL be stored, restored and marked stale with the summary. When the field is absent or breaks its contract, Beranda SHALL show no categories tile, and the phone SHALL never compute category totals from its own rows or captures.

#### Scenario: Tracker offers no categories for the period
- **WHEN** Beranda is shown with a summary from a tracker that does not send `top_categories`
- **THEN** Beranda shows no "Kategori teratas" tile and no category amount

#### Scenario: Recorded categories on the phone
- **WHEN** the phone holds recorded expenses with categories from the last three days
- **THEN** no category name or amount from them appears on Beranda

#### Scenario: Tracker sends its top categories
- **WHEN** the summary's `top_categories` are Belanja Rumah Rp410.000 (need), Hiburan Contoh Rp150.000 (want) and Lain-lain Rp80.000 (no pillar)
- **THEN** "Kategori teratas" lists them in that order with their amounts, "Kebutuhan" and "Keinginan", bars relative to Rp410.000, the summary's "Periode" and "Diperbarui" lines, and TalkBack reads the first as "Belanja Rumah, Rp410.000, Kebutuhan"

#### Scenario: No spending in the period
- **WHEN** the summary's `top_categories` is an empty list
- **THEN** the tile says "Belum ada pengeluaran pada periode ini" and shows no Rp0

#### Scenario: Stored or not updated
- **WHEN** the summary shown was restored from the phone, or the last refresh failed
- **THEN** the tile shows the stored categories with the same "Tersimpan di ponsel, terakhir diperbarui" or "Belum diperbarui. Terakhir berhasil" line and reason as the "Arus kas tracker" tile

#### Scenario: Categories that break the contract
- **WHEN** `top_categories` holds more than three entries, entries not sorted by outflow descending, a negative or missing outflow, a blank name or an unknown pillar
- **THEN** Beranda shows no "Kategori teratas" tile and still shows the summary's totals

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
