## MODIFIED Requirements

### Requirement: Provenance in the Ledger Edit Dialog
The transaction dialog SHALL show the row's source ("Dicatat"), classifier ("Diklasifikasikan") and check state ("Dicek") in the interface language. "Ubah transaksi" SHALL let the owner edit the merchant. For an automatic row without an open question, "Dicek" SHALL offer a keyboard-reachable "Tandai sudah dicek" that accepts the row through the existing bulk review action.

#### Scenario: Reviewing an automatic row
- **GIVEN** an auto row from a bank notification
- **WHEN** the owner opens it in the ledger
- **THEN** the dialog SHALL show "Notifikasi bank" under "Dicatat" and "Belum dicek · Tandai sudah dicek" under "Dicek"
- **AND** activating "Tandai sudah dicek" SHALL send `POST /api/review/bulk` with `action: "accept"` for that row only, and offer "Urungkan".

#### Scenario: Editing the merchant
- **WHEN** the owner types a merchant in "Ubah transaksi" and saves
- **THEN** the update SHALL include the merchant.

#### Scenario: Provenance in English
- **WHEN** an English account opens the same row
- **THEN** the dialog SHALL label the rows "Recorded", "Classified by" and "Checked", with the source and classifier names in English
