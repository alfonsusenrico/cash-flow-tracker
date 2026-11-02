# companion-pairing Specification

## Purpose
Pair the companion to the owner's tracker with a confirmed, single-use web QR, while preserving manual entry and existing device data.

## Requirements

### Requirement: Guided setup
An unconfigured companion SHALL guide setup as story cards (onboarding C): a welcome card, five steps with a progress bar (pairing, notification access, battery and background with this phone's battery-saver steps, the notification permission, the self-test), then "HP-mu siap". Permission steps SHALL be skippable. Configured upgrades SHALL enter the existing app. Pairing SHALL offer QR, the typed code and the manual API key, without a server URL field.

#### Scenario: First setup
- **WHEN** an unconfigured phone opens the app
- **THEN** it shows the welcome card, then step 1 for pairing, step 2 for notification access, step 3 for battery access with the HiOS note, step 4 for notifications and step 5 for the self-test
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

### Requirement: Pairing by a typed code
The pairing card SHALL offer "Ketik kode aja" next to "Scan QR", and SHALL fall back to it when camera access is refused. The phone SHALL fold the typed code as the tracker does (case, spaces and dashes ignored; O read as 0, I and L as 1) and accept only exactly ten code characters before showing the same confirmation as a scanned QR. A code the phone rejects SHALL say "Kodenya 10 huruf atau angka, ada di bawah QR." and send nothing.

#### Scenario: Typed code
- **WHEN** the owner types "7k4q-9mzx-b0" and taps "Sambungin"
- **THEN** the confirmation "Kodenya kebaca" asks before anything is exchanged

#### Scenario: Short code
- **WHEN** the owner types "7K4-Q9"
- **THEN** the card says "Kodenya 10 huruf atau angka, ada di bawah QR." and stays on the code

### Requirement: Setup ends with a self-test
After the notification permission, setup SHALL run the self-test ("Tes bentar, ya") as three lines, each with its state in words and an icon: show the test notification, catch it with the listener, tell the web. Each failure SHALL offer its fix. A passing test, or a skipped notification step, SHALL lead to "HP-mu siap".

#### Scenario: Test passes
- **WHEN** the test notification shows, the listener catches it and the tracker stores the report
- **THEN** the three lines read "Beres" and the card "HP-mu siap" follows

#### Scenario: Listener cannot catch it
- **WHEN** the test notification is not caught within 10 seconds
- **THEN** the card says "Notifikasinya muncul, tapi aku nggak bisa nangkep." and offers "Nyalain akses" and "Tes ulang"

#### Scenario: Notifications not allowed
- **WHEN** the test notification cannot show
- **THEN** the card offers "Izinin notifikasi" and "Lewati bagian ini"

#### Scenario: Tracker not reached
- **WHEN** the listener caught the notification but the report could not reach the tracker
- **THEN** the card says "Aku nggak bisa ngabarin web." and "Coba lagi" repeats only the report

#### Scenario: Notification step skipped
- **WHEN** notifications are not allowed and the owner taps "Lewati bagian ini"
- **THEN** "HP-mu siap" says "Tes nangkep notifikasi dilewati. Aku cek sendiri pas notifikasi bank pertama masuk."
