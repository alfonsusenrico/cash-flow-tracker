# web-api-key-management Specification

## Purpose
Let the owner manage the companion API key from web Settings and enter amounts and local times in quick capture without losing the chosen instant.

## Requirements

### Requirement: API Key Section in Settings
The Settings modal in the web-UI SHALL display an API key section showing prefix and last-used timestamp. It SHALL support rotating the key with a confirmation dialog and a one-time copyable display of the new key.

#### Scenario: Rotate API key
- **WHEN** user clicks "Buat Kunci Baru" and confirms
- **THEN** a new API key is generated, displayed once in full with a copy button, and only prefix remains visible after closing

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
