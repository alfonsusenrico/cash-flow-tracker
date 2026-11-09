## MODIFIED Requirements

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
