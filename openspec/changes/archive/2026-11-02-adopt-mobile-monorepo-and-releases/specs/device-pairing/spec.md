## ADDED Requirements

### Requirement: Companion Download in Settings
Web Settings SHALL show "Unduh aplikasi pendamping" in its Perangkat & akses section, before "Perangkat terhubung". It SHALL show a QR code and a link for the stable latest-release APK, a note to compare the file's SHA-256 with the release page, and the minimum Android version (Android 8.0). When the QR code cannot be drawn, the link SHALL remain and an alert SHALL say to use it.

#### Scenario: Settings offers the download
- **WHEN** the owner opens Settings → Perangkat & akses
- **THEN** "Unduh aplikasi pendamping" shows a QR code for `https://github.com/alfonsusenrico/cash-flow-tracker/releases/latest/download/cashflow-companion.apk`, the same link, the SHA-256 note with a link to the release page, and "Butuh Android 8.0 atau lebih baru."

#### Scenario: The QR code cannot be drawn
- **WHEN** drawing the download QR code fails
- **THEN** an alert says "Kode QR gagal ditampilkan. Pakai tautan unduhan." and the download link stays available

### Requirement: Companion Download in the Pairing Dialog
While no phone has paired, the pairing dialog SHALL offer the same download content in a collapsed disclosure, "Belum punya aplikasinya? Unduh aplikasi pendamping", so the download QR is not mistaken for the pairing code.

#### Scenario: The pairing dialog keeps the download collapsed
- **WHEN** the owner opens "Hubungkan ponsel"
- **THEN** the pairing QR code is shown and the download disclosure stays collapsed until the owner opens it
