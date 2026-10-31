## MODIFIED Requirements

### Requirement: Pairing in Web Settings
Web Settings SHALL show "Perangkat terhubung" in its Perangkat & akses section, with "Hubungkan ponsel" opening a dialog. The dialog SHALL contain the QR code, the code as text, a countdown and "Buat kode baru". Settings SHALL list each device with "Cabut" behind a confirmation dialog.

#### Scenario: Expired code in the dialog
- **WHEN** the countdown reaches zero
- **THEN** the dialog SHALL say the code expired and offer "Buat kode baru".

#### Scenario: Closing the pairing dialog
- **WHEN** the owner presses Escape in the pairing dialog
- **THEN** only the dialog SHALL close, the Settings page SHALL stay open, and focus SHALL return to "Hubungkan ponsel"
