## MODIFIED Requirements

### Requirement: API Key Section in Settings
The Settings modal SHALL display a manual API key section, labelled as the fallback to QR pairing, showing the prefix and last-used timestamp. Rotating with confirmation SHALL replace only manually created keys, show the new key once with a copy button, and SHALL NOT disconnect paired devices.

#### Scenario: Rotate API key
- **WHEN** user clicks "Buat Kunci Baru" and confirms
- **THEN** a new API key is generated, displayed once in full with a copy button, and only prefix remains visible after closing

#### Scenario: Rotating keeps paired phones
- **GIVEN** a phone paired by QR
- **WHEN** the owner rotates the manual key
- **THEN** the paired phone's key SHALL keep working.
