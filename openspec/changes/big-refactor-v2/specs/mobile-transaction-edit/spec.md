# Spec: Mobile Transaction Edit

## ADDED Requirements

### Requirement: TransactionEditSheet Form
The mobile companion app SHALL replace `NotificationDetailSheet` with `TransactionEditSheet`. The sheet SHALL display: Catatan (notes), Jumlah (Rp-formatted amount), Jenis (income/expense toggle), Rekening (dropdown from API), Kategori (dropdown from API filtered by type), and Waktu Transaksi. The sheet SHALL NOT display developer jargon such as Payload Hash, Raw JSON, or Ground Truth.

#### Scenario: Open unsynced notification in edit sheet
- **WHEN** user taps a notification card in the inbox feed
- **THEN** `TransactionEditSheet` opens pre-populated with natural Indonesian field labels and clean values
- **AND** action button reads "Kirim Transaksi ke Tracker"

### Requirement: Patch Synced Transactions from Mobile
The mobile app SHALL persist `server_transaction_id` in Room. When editing a synced transaction, the app SHALL call `PATCH /api/transactions/{server_transaction_id}` with updated fields.

#### Scenario: Patch synced transaction
- **WHEN** user modifies fields on a synced notification and taps "Perbarui Transaksi di Tracker"
- **THEN** `PATCH /api/transactions/{id}` is called on the server
- **AND** local entity and feed reflect the update

### Requirement: Floating Action Button for Sync
The mobile feed screen SHALL display a FloatingActionButton (FAB) at the bottom-right corner to initiate immediate synchronization.

#### Scenario: Tap sync FAB
- **WHEN** user taps the sync FAB at bottom-right
- **THEN** sync worker is enqueued to send pending notifications to server
