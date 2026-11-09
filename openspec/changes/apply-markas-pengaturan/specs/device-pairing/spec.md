## MODIFIED Requirements

### Requirement: Pairing in Web Settings
Pengaturan › HP & perangkat SHALL show "Perangkat terhubung" with "Hubungkan ponsel", which SHALL open a centered 520 px dialog (a bottom sheet below 768 px) with the pairing QR code drawn at least 200 px wide on a white quiet border in both themes, three numbered steps, the code as text, a countdown and "Buat kode baru". Each phone SHALL be listed with "Cabut" behind a confirmation dialog.

#### Scenario: Expired code in the dialog
- **WHEN** the countdown reaches zero
- **THEN** the dialog SHALL say the code expired and offer "Buat kode baru".

#### Scenario: Closing the pairing dialog
- **WHEN** the owner presses Escape in the pairing dialog
- **THEN** only the dialog SHALL close, the Settings page SHALL stay open, and focus SHALL return to "Hubungkan ponsel"

#### Scenario: Same pairing requests
- **WHEN** the owner opens the dialog, lets the code expire and creates a new one
- **THEN** the dialog SHALL send `POST /api/devices/pairing-codes` twice and poll `GET /api/devices` every 3 seconds as before

### Requirement: Companion Download in Settings
Pengaturan › HP & perangkat SHALL show "Unduh aplikasi pendamping" after "Status HP" and before "Perangkat terhubung". It SHALL show a QR code of at least 136 px, so each module is at least 3 px, in a white box, and a link for the stable latest-release APK, a note to compare the file's SHA-256 with the release page, and the minimum Android version (Android 8.0). When the QR code cannot be drawn, the link SHALL remain and an alert SHALL say to use it.

#### Scenario: Settings offers the download
- **WHEN** the owner opens Pengaturan → HP & perangkat
- **THEN** "Unduh aplikasi pendamping" shows a QR code for `https://github.com/alfonsusenrico/cash-flow-tracker/releases/latest/download/cashflow-companion.apk`, the same link, the SHA-256 note with a link to the release page, and the Android 8.0 minimum

#### Scenario: The QR code cannot be drawn
- **WHEN** drawing the download QR code fails
- **THEN** an alert says "Kode QR gagal ditampilkan. Pakai tautan unduhan." ("The QR code could not be shown. Use the download link.") and the download link stays available
