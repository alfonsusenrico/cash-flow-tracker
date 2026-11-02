# companion-pairing Specification

## Purpose
Pair the companion to the owner's tracker with a confirmed, single-use web QR, while preserving manual entry and existing device data.

## Requirements

### Requirement: Guided setup
An unconfigured companion SHALL guide notification access, battery/background access, then connection in three steps, with skip actions. Configured upgrades SHALL enter the existing app directly. Connection SHALL offer QR first and a manual API key fallback, without a server URL field.

#### Scenario: First setup
- **WHEN** an unconfigured phone opens the app
- **THEN** it shows step 1 for notification access, step 2 for battery access with the HiOS note, and step 3 for connection
- **AND** each permission step can be skipped

#### Scenario: Existing phone
- **WHEN** an already configured phone upgrades
- **THEN** its records and stable device identity remain and it opens the existing home screen

### Requirement: Local QR validation and confirmation
Camera and cashflow://pair links SHALL accept only a hierarchical URI with authority pair, no path or fragment, exactly one each of v=1, server matching the fixed build URL, and code containing ten uppercase Crockford characters. Unknown or duplicate query parameters SHALL be rejected. A valid QR SHALL show confirmation before any exchange request.

#### Scenario: Foreign or malformed QR
- **WHEN** a QR has another server, version, malformed code, extra parameter, duplicate parameter, path, fragment or authority
- **THEN** the app shows QR tidak valid and sends no exchange request

#### Scenario: Valid link
- **WHEN** a valid QR is scanned or opened as a link
- **THEN** the app shows the automatically detected device name and an explicit Hubungkan action
- **AND** cancellation sends nothing

### Requirement: Camera permission and lifecycle
Camera access SHALL be requested only when opening the in-app scanner. Denial or unavailable hardware SHALL offer manual entry. The scanner SHALL show a dark viewfinder, torch control when supported, and stop camera use when it closes or leaves the foreground. Camera images SHALL not be persisted.

#### Scenario: Camera denied
- **WHEN** camera permission is denied
- **THEN** manual API key entry remains available and no pairing code is sent

#### Scenario: Leaving scanner
- **WHEN** the scanner closes or the app enters the background
- **THEN** the camera and image analysis stop

### Requirement: Automatic device name
The companion SHALL show the detected device name read-only, using the system name or manufacturer and model fallback. It SHALL send a printable name of 1 to 80 characters with the existing stable device id.

#### Scenario: No system device name
- **WHEN** the system name is empty
- **THEN** the companion uses manufacturer and model without duplicating a manufacturer prefix

### Requirement: Exchange and authenticated verification
After confirmation the companion SHALL exchange a code at the fixed server, save the returned key before checking authenticated connectivity, and preserve that key when the check fails. It SHALL distinguish expired/used codes (410), rate limiting (429), and network failures without displaying credentials or raw server errors. Tes Koneksi SHALL authenticate the key, rather than only call public health.

#### Scenario: Successful exchange
- **WHEN** the server accepts a confirmed code
- **THEN** the key is stored, an authenticated connection check runs and success is shown only when the key is accepted

#### Scenario: Used or expired QR
- **WHEN** exchange returns 410
- **THEN** the companion offers Pindai lagi and requests a fresh web QR

#### Scenario: Rate limited
- **WHEN** exchange returns 429
- **THEN** the companion asks the owner to wait and does not automatically retry the exchange

#### Scenario: Connection fails after exchange
- **WHEN** exchange succeeds but connectivity verification fails
- **THEN** the saved new key remains and Tes Koneksi retries verification without exchanging the code again

### Requirement: Re-pairing follows server replacement
Re-pairing SHALL use the existing stable device id. The companion SHALL explain that a same-device exchange replaces the previous key immediately. A previous manual key SHALL be revoked only after authenticated verification of the new key and confirmation that both keys belong to the same account. Revocation failure SHALL leave a visible web-settings recovery action.

#### Scenario: Same device pairs again
- **WHEN** a new QR is exchanged for the existing device id
- **THEN** the server replaces the prior device key during exchange and the companion keeps the returned key for retry

#### Scenario: Previous manual key belongs to another owner
- **WHEN** the new key belongs to a different account
- **THEN** the companion never revokes the prior account's key

### Requirement: Pairing context survives compatible upgrade
Ignoring the saved URL SHALL preserve the pairing context and known-pairing fingerprints byte for byte when the saved normalized URL equals the fixed server. A saved different-server key SHALL never be sent to the fixed server and SHALL require a new pairing. Room SHALL remain version 7.

#### Scenario: Production upgrade
- **WHEN** the saved server matches the fixed server
- **THEN** pending answers, pairing context and records remain unchanged

#### Scenario: Another saved server
- **WHEN** the saved server differs from the fixed server
- **THEN** the old credentials are cleared and a different context prevents sending prior pending answers to the fixed server
