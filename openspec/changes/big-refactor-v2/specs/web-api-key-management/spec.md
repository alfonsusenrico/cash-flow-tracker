# Spec: Web API Key Management

## ADDED Requirements

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

## REMOVED Requirements

### Requirement: Calculator Keypad in Quick Capture
The 4×4 calculator keypad and toggle button SHALL be removed from `QuickCaptureModal`.

#### Scenario: Quick capture without keypad
- **WHEN** QuickCaptureModal is opened
- **THEN** amount is input via text field without virtual keypad
