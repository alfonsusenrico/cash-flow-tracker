# web-api-key-management Specification

## Purpose
Let the owner manage the companion API key from web Settings and enter amounts and local times in quick capture without losing the chosen instant.

## Requirements

### Requirement: API Key Section in Settings
The Settings modal SHALL display a manual API key section, labelled as the fallback to QR pairing, showing the prefix and last-used timestamp. Rotating with confirmation SHALL replace only manually created keys, show the new key once with a copy button, and SHALL NOT disconnect paired devices.

#### Scenario: Rotate API key
- **WHEN** user clicks "Buat Kunci Baru" and confirms
- **THEN** a new API key is generated, displayed once in full with a copy button, and only prefix remains visible after closing

#### Scenario: Rotating keeps paired phones
- **GIVEN** a phone paired by QR
- **WHEN** the owner rotates the manual key
- **THEN** the paired phone's key SHALL keep working.

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
