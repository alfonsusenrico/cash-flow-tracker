## ADDED Requirements

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

## MODIFIED Requirements

### Requirement: Guided setup
An unconfigured companion SHALL guide setup as story cards (onboarding C): a welcome card, five steps with a progress bar (pairing, notification access, battery and background with this phone's battery-saver steps, the notification permission, the self-test), then "HP-mu siap". Permission steps SHALL be skippable. Configured upgrades SHALL enter the existing app. Pairing SHALL offer QR, the typed code and the manual API key, without a server URL field.

#### Scenario: First setup
- **WHEN** an unconfigured phone opens the app
- **THEN** it shows the welcome card, then step 1 for pairing, step 2 for notification access, step 3 for battery access with the HiOS note, step 4 for notifications and step 5 for the self-test
- **AND** each permission step can be skipped

#### Scenario: Existing phone
- **WHEN** an already configured phone upgrades
- **THEN** its records and stable device identity remain and it opens the existing home screen
