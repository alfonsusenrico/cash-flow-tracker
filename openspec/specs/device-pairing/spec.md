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
Web Settings SHALL show "Perangkat terhubung", with "Hubungkan ponsel" opening a dialog. The dialog SHALL contain the QR code, the code as text, a countdown and "Buat kode baru". Settings SHALL list each device with "Cabut" behind a confirmation dialog.

#### Scenario: Expired code in the dialog
- **WHEN** the countdown reaches zero
- **THEN** the dialog SHALL say the code expired and offer "Buat kode baru".
