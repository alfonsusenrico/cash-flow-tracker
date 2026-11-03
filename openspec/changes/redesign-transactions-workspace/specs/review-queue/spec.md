# Spec Delta

## MODIFIED Requirements

### Requirement: Triage Folders and Counts
Tinjau SHALL offer the folder chips "Semua", "Belum dikenali", "Perlu jawaban" and "Pindah saldo belum lengkap" (awaiting and ambiguous legs), each with its row count. "Semua" SHALL be the default and SHALL list the three in that order. Without truncation, the three counts SHALL add up to the summary's `attention`. The header SHALL show no amount. The header label SHALL read "Perlu ditinjau", and the folder labels and messages SHALL use the neutral register of "Interface Copy Register" (ui-copy-register).

#### Scenario: Counts match the badge
- **GIVEN** one open question, two needs-review rows in one group, one awaiting send and one ambiguous credit
- **WHEN** Tinjau loads
- **THEN** the chips SHALL read Belum dikenali 1, Perlu jawaban 2 and Pindah saldo belum lengkap 2, the header and the sidebar badge SHALL both read 5, and "Semua" SHALL list four items

#### Scenario: Folder filter
- **WHEN** the owner chooses the "Pindah saldo belum lengkap" chip
- **THEN** only the awaiting and ambiguous legs SHALL be listed and the chip SHALL be marked pressed

### Requirement: Semua Beres
When no item needs the owner, Tinjau SHALL show "Semua sudah ditinjau" with this cycle's Keluar, the safe-to-spend amount (or the remaining amount when no daily figure is available) and the unclassified amount from the cycle summary, a link back to Beranda and, when automatic rows exist, "Cek yang otomatis". A failed summary SHALL omit the figures rather than show zero.

#### Scenario: Nothing waiting
- **GIVEN** no question, group, awaiting or ambiguous leg, and three automatic rows
- **WHEN** Tinjau loads
- **THEN** it SHALL show "Semua sudah ditinjau", the three cycle figures and "Cek yang otomatis"

#### Scenario: Summary unavailable
- **GIVEN** nothing is waiting and the cycle summary request fails
- **WHEN** Tinjau loads
- **THEN** it SHALL show "Semua sudah ditinjau" without figures and SHALL NOT show Rp 0

### Requirement: Tinjau Deep Links
`/review?item=<transaction id>` SHALL select the item that contains that transaction. `folder=apa|jawab|pindah|saran|otomatis`, or else the legacy hashes `#review-questions`, `#review-groups`, `#review-awaiting` and `#review-ambiguous`, SHALL choose its folder. An id that is no longer waiting SHALL open the folder at its first item and say so.

#### Scenario: Link from Kerjakan dulu
- **WHEN** the owner follows Beranda's "Pasangkan" link `/review?item=<send id>#review-awaiting`
- **THEN** Tinjau SHALL open "Pindah saldo belum lengkap" with that send selected and its heading focused

#### Scenario: Resolved meanwhile
- **WHEN** the owner opens `/review?item=<id>#review-questions` for a question already answered
- **THEN** "Belum dikenali" SHALL open at its first item, or "Semua sudah ditinjau" if empty, with the message that the item was already resolved
