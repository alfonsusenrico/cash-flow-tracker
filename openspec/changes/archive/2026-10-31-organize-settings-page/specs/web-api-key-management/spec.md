## MODIFIED Requirements

### Requirement: API Key Section in Settings
The Settings page SHALL display a manual API key section in Perangkat & akses, after the paired devices, labelled as the fallback to QR pairing and collapsed until the owner opens it, showing the prefix and last-used timestamp. Rotating with confirmation SHALL replace only manually created keys, show the new key once with a copy button, and SHALL NOT disconnect paired devices.

#### Scenario: Rotate API key
- **WHEN** user clicks "Buat Kunci Baru" and confirms
- **THEN** a new API key is generated, displayed once in full with a copy button, and only prefix remains visible after closing

#### Scenario: Rotating keeps paired phones
- **GIVEN** a phone paired by QR
- **WHEN** the owner rotates the manual key
- **THEN** the paired phone's key SHALL keep working.

#### Scenario: Manual key collapsed by default
- **WHEN** the owner opens Perangkat & akses
- **THEN** "Kunci API manual (cadangan)" SHALL be shown closed after the paired devices and SHALL expand by keyboard or pointer
