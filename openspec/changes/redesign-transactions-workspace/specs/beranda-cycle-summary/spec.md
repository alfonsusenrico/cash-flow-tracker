# Spec Delta

## MODIFIED Requirements

### Requirement: Uang Siklus Ini Card
Beranda SHALL show "Uang siklus ini": masuk for the period and a segmented bar of "Sudah dipakai" (keluar), "Terjadwal" and "Aman dipakai" (remaining). Each segment SHALL carry its amount as text with a label beside it, every amount SHALL be explainable, and the bar SHALL NOT be the only carrier of the values.

#### Scenario: Segments add up
- **WHEN** the card shows masuk Rp 9.800.000
- **THEN** the three labelled amounts SHALL add up to Rp 9.800.000, or the card SHALL say by how much the period is over when they exceed it

#### Scenario: Screen reader
- **WHEN** a screen reader reaches the card
- **THEN** it SHALL read each segment's name and amount as text, and the decorative bar SHALL be hidden from it

### Requirement: Kerjakan Dulu
Beranda SHALL list what needs the owner first: open questions, awaiting or ambiguous transfers, groups that need an answer, and plan occurrences that are due or due within two days. Each item SHALL name the merchant or plan, amount and time and offer one action. A due plan SHALL keep its link, skip and record actions. When the Beranda cards form one column, "Kerjakan dulu" SHALL come directly after "Uang siklus ini", before recent activity, in both the visual and the reading and focus order. The card SHALL be titled "Perlu tindakan".

#### Scenario: Items and order
- **GIVEN** one open question, two awaiting transfer legs, one group that needs an answer and a plan due tomorrow
- **WHEN** the owner opens Beranda
- **THEN** "Perlu tindakan" SHALL show a count of 4 Tinjau items, list the question first and the plan last, open Tinjau at the matching item from each review action, and show "Buka Tinjau"

#### Scenario: One-column Beranda
- **WHEN** the Beranda main column is 900 px wide or narrower above the phone layout
- **THEN** "Perlu tindakan" SHALL follow "Uang siklus ini" directly and come before "Aktivitas terbaru", and keyboard focus SHALL reach it in that order

#### Scenario: Recording a due plan
- **WHEN** the owner chooses to record a due plan from "Perlu tindakan"
- **THEN** the same confirmation as before the redesign SHALL appear, same-amount captures SHALL be offered for linking first, and nothing SHALL be posted without the owner's explicit confirmation

#### Scenario: Nothing to do
- **WHEN** nothing waits and no plan is due
- **THEN** the card SHALL say "Semua sudah ditinjau" and SHALL show the count of rows recorded automatically today with an optional link to check them


### Requirement: Recent Activity on Beranda
Beranda SHALL list up to eight transactions from today and yesterday, newest first, with merchant or movement, account, time, Kakeibo bucket or status, provenance and amount in its semantic colour, and a link to all transactions. Selecting a row SHALL open the shared transaction drawer of Transaksi for that row, with the same detail and actions.

#### Scenario: Waiting row in the list
- **WHEN** a row with an open question is listed
- **THEN** it SHALL show "Menunggu jawaban" as its status instead of a pillar and SHALL show its amount

#### Scenario: Movement row
- **WHEN** a "Pindah Saldo" between two of the owner's accounts is listed
- **THEN** it SHALL appear once as "source → target" with a sky amount, and SHALL say "Tidak dihitung" instead of a pillar

#### Scenario: Row opens the transaction drawer
- **WHEN** the owner selects "Kedai Awan Rp 30.000" in "Aktivitas terbaru"
- **THEN** the transaction drawer SHALL open for that row with its provenance, "Ubah", "Tandai sudah dicek" where allowed and "Hapus"
- **AND** closing it SHALL return focus to the row, and a delete confirmed there SHALL refresh Beranda's figures

#### Scenario: Row deleted elsewhere
- **GIVEN** the row was deleted in another tab
- **WHEN** the owner selects it on Beranda
- **THEN** the drawer SHALL say "Transaksi ini sudah tidak ada" and Beranda's list SHALL refresh
