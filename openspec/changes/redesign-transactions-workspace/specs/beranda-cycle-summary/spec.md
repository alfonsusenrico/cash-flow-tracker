# Spec Delta

## MODIFIED Requirements

### Requirement: Recent Activity on Beranda
Beranda SHALL list up to eight transactions from today and yesterday, newest first, with merchant or movement, account, time, Kakeibo bucket or status, provenance and amount in its semantic colour, and a link to all transactions. Selecting a row SHALL open the shared transaction drawer of Transaksi for that row, with the same detail and actions.

#### Scenario: Waiting row in the list
- **WHEN** a row with an open question is listed
- **THEN** it SHALL show "Nunggu jawaban" as its status instead of a pillar and SHALL show its amount

#### Scenario: Movement row
- **WHEN** a "Pindah Saldo" between two of the owner's accounts is listed
- **THEN** it SHALL appear once as "source → target" with a sky amount, and SHALL say "Nggak dihitung" instead of a pillar

#### Scenario: Row opens the transaction drawer
- **WHEN** the owner selects "Kedai Awan Rp 30.000" in "Aktivitas terbaru"
- **THEN** the transaction drawer SHALL open for that row with its provenance, "Ubah", "Tandai sudah dicek" where allowed and "Hapus"
- **AND** closing it SHALL return focus to the row, and a delete confirmed there SHALL refresh Beranda's figures

#### Scenario: Row deleted elsewhere
- **GIVEN** the row was deleted in another tab
- **WHEN** the owner selects it on Beranda
- **THEN** the drawer SHALL say "Transaksi ini udah nggak ada" and Beranda's list SHALL refresh
