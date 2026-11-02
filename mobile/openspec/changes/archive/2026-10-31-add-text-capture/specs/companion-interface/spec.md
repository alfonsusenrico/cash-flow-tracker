## MODIFIED Requirements

### Requirement: Home figures come from this phone's rows
Beranda SHALL compute every figure only from records stored on this phone: notifications and typed captures. These are money recorded by the tracker today (out, in, moved), a three-day strip of money out, the top three categories of the last three days, open questions, rows and captures that need checking, and rows and captures kept on the phone. Each money figure SHALL say it comes from notifications and captures on this phone.

#### Scenario: Recorded rows are summed
- **WHEN** today has a recorded expense of Rp187.500 and one of Rp32.000 and a recorded income of Rp250.000
- **THEN** "Keluar tercatat hari ini" shows Rp219.500 and "Masuk" shows Rp250.000

#### Scenario: Unsettled rows are counted, not summed
- **WHEN** today has a sender question, a source-pocket question and an unsent capture
- **THEN** "Menunggu" shows 3 notifikasi and none of their amounts is added to a money figure

#### Scenario: The phone is not the tracker
- **WHEN** Beranda is shown
- **THEN** the today tile says "Dari notifikasi dan catatan di ponsel ini" and the page says the figures are not tracker balances

#### Scenario: Nothing recorded in three days
- **WHEN** no expense with a category was recorded in the last three days
- **THEN** the categories tile says that nothing was recorded instead of listing zero amounts

#### Scenario: Typed captures are counted with the notifications
- **WHEN** today has a recorded notification expense of Rp32.000, a recorded typed expense of Rp10.000, a recorded typed move of Rp200.000 and one typed capture waiting in the queue
- **THEN** "Keluar tercatat hari ini" shows Rp42.000, "Pindah" shows Rp200.000, and "Menunggu" adds "1 catatan" without its amount
