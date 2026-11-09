# web-api-key-management Specification

## Purpose
Let the owner manage the companion API key from web Settings and enter amounts and local times in quick capture without losing the chosen instant.

## Requirements

### Requirement: API Key Section in Settings
Pengaturan › HP & perangkat SHALL end with the section "Kunci API manual (cadangan)", labelled as the fallback to QR pairing, showing the key's prefix, creation date and last use. Rotating with an inline confirmation SHALL replace only manually created keys, show the new key once with a copy button, and SHALL NOT disconnect paired devices. The key SHALL never be logged.

#### Scenario: Rotate API key
- **WHEN** the owner chooses "Buat kunci baru (reset)" and confirms
- **THEN** a new API key is generated, displayed once in full with a copy button, and only its prefix remains visible after closing

#### Scenario: Rotating keeps paired phones
- **GIVEN** a phone paired by QR
- **WHEN** the owner rotates the manual key
- **THEN** the paired phone's key SHALL keep working.

#### Scenario: Manual key collapsed by default
- **WHEN** the owner opens HP & perangkat
- **THEN** "Kunci API manual (cadangan)" SHALL be the last section, shown open rather than collapsed, with the prefix row and the reset button reachable by keyboard

### Requirement: Timezone Preserving Datetime Input
The web-UI quick capture and ledger edit modals SHALL preserve local timezone offsets during datetime serialization rather than silently converting to UTC with `.toISOString()`.

#### Scenario: Datetime preserved in local offset
- **WHEN** user selects local datetime "2026-09-17 10:00" in UTC+7
- **THEN** serialized date sent to API preserves the +07:00 timezone offset

### Requirement: Quick Capture Without Calculator Keypad
`QuickCaptureModal` SHALL NOT show a 4×4 calculator keypad or a keypad toggle button.

#### Scenario: Quick capture without keypad
- **WHEN** QuickCaptureModal is opened
- **THEN** amount is input via text field without virtual keypad
