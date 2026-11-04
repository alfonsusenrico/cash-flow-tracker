## MODIFIED Requirements

### Requirement: Ringkasan Figures and Names
Ringkasan SHALL show Masuk, Pengeluaran and Ditabung & investasi as explainable figures. Pengeluaran SHALL equal the ledger summary's `spending` that Transaksi and Beranda show, and Ditabung & investasi the Kakeibo saving bucket. The explanations SHALL list only server lines whose total equals the figure, and the client SHALL compute no money value.

#### Scenario: Keluar equals Beranda
- **GIVEN** the current cycle with Rp 1.834.000 of spending and Rp 500.000 of net fresh saving
- **WHEN** Ringkasan and Beranda are shown
- **THEN** Ringkasan and Beranda SHALL show the same Pengeluaran Rp 1.834.000 and the same Ditabung & investasi Rp 500.000, and Ringkasan's Pengeluaran SHALL equal Transaksi's for the cycle

#### Scenario: Explanation of Belanja
- **WHEN** the owner opens the explanation of Pengeluaran in a period with a Rp 40.000 reimbursement
- **THEN** it SHALL list gross spending, the applied reimbursement and any saving-pillar expense moved to Ditabung & investasi, and end with the Pengeluaran figure

#### Scenario: Older API without explanations
- **WHEN** the analytics response has no `explanations`
- **THEN** the figures SHALL show their values without an explanation trigger, and no figure SHALL be shown as Rp 0 in place of a missing value

### Requirement: Ringkasan Kakeibo Comparison
Ringkasan SHALL show the running period's Pengeluaran so far against the previous whole period's total, in total and per pillar, as a share without a verdict, then the last completed period against its three-period average (`monthly-report`) with each pillar's change as an amount, never a percentage. It SHALL NOT compare the same elapsed days.

#### Scenario: Kakeibo against the same day
- **GIVEN** day 9 of the cycle and a completed previous cycle
- **WHEN** Ringkasan shows the progress card
- **THEN** each pillar SHALL show its amount so far and its share of the previous cycle's pillar total without "Naik" or "Turun", no marker at the same day SHALL be drawn, and the amounts SHALL add up to Pengeluaran so far

#### Scenario: Unclassified spending
- **WHEN** the period has spending without a pillar
- **THEN** the unclassified bucket SHALL read "Perlu dipilih" with a link to classify it

### Requirement: Ringkasan Supporting Cards
Ringkasan SHALL show compact cards for the detected bills and subscriptions, "Seberapa sering ditanya" and "Kebiasaan dihafal", each with its headline figure first and details behind "Rincian", without method paragraphs. Its last card SHALL be "Kategori & anggaran" with unchanged behaviour. The weekday heatmap and the same-day increases and decreases SHALL NOT be shown.

#### Scenario: Category budget editing stays
- **WHEN** the owner edits a category's budget in "Kategori & anggaran"
- **THEN** the change SHALL be saved through the existing category endpoint and the Kakeibo and budget figures SHALL refresh

#### Scenario: No weekday heatmap
- **WHEN** Ringkasan is shown
- **THEN** no weekday heatmap card SHALL be rendered, and the analytics response SHALL still include `day_of_week_heatmap`

#### Scenario: Headline first
- **WHEN** Ringkasan shows "Tagihan & langganan terdeteksi" with three detected charges
- **THEN** the card SHALL show "3" and their monthly total first, and the charge list SHALL appear only after "Rincian" is opened
