# device-pairing Specification

## Purpose
Pair the companion phone with the owner's account by scanning a single-use QR code from web Settings, with one revocable API key per device.

## Requirements

### Requirement: Single-Use Pairing Codes
`POST /api/devices/pairing-codes` SHALL require a browser session. It SHALL return a new 10-character code valid for 5 minutes, invalidate the owner's previous unused code, and store only the code's hash. Its `qr_payload` SHALL name the fixed server.

#### Scenario: New code replaces the old one
- **WHEN** the owner creates a code and then another
- **THEN** only the second code SHALL be exchangeable.

#### Scenario: Bearer clients cannot create codes
- **WHEN** a request carries only a Bearer key
- **THEN** the response SHALL be 401 or 403, and no code SHALL be created.

### Requirement: Code Exchange Creates a Device Key
`POST /api/devices/pair` SHALL exchange a valid, unexpired, unused code exactly once for a new API key labelled with the given device name and id. Any invalid, expired or reused code SHALL return 410 `pairing_code_invalid`. A code SHALL be burned after 5 failed attempts. Re-pairing the same device id SHALL replace that device's previous key.

#### Scenario: Successful pairing
- **WHEN** the phone "TECNO CN7c" sends a fresh code
- **THEN** the response SHALL contain a new API key once
- **AND** the device list SHALL show "TECNO CN7c" paired just now.

#### Scenario: Reused or expired code
- **WHEN** the same code is sent again, or a code older than 5 minutes is sent
- **THEN** the response SHALL be 410, and no key SHALL be created.

### Requirement: Device List and Revoke
`GET /api/devices` SHALL list the owner's active keys with name, origin (pairing or manual), prefix, created and last-used times. `DELETE /api/devices/{id}` SHALL revoke one key so that every later request with it gets 401. Another owner's device SHALL return 404.

#### Scenario: Revoke a lost phone
- **WHEN** the owner revokes "TECNO CN7c"
- **THEN** the companion's next request SHALL get 401, while other devices keep working.

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

### Requirement: Companion Download in the Pairing Dialog
While no phone has paired, the pairing dialog SHALL offer the same download content in a collapsed disclosure, "Belum punya aplikasinya? Unduh aplikasi pendamping", so the download QR is not mistaken for the pairing code.

#### Scenario: The pairing dialog keeps the download collapsed
- **WHEN** the owner opens "Hubungkan ponsel"
- **THEN** the pairing QR code is shown and the download disclosure stays collapsed until the owner opens it
