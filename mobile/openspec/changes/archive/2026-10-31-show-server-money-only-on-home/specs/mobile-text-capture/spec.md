## MODIFIED Requirements

### Requirement: Captured money never enters the tracker's summary
Recorded captures SHALL count only in figures that come from this phone, such as Aktivitas' money out per day, and SHALL never appear in a money figure on Beranda. The tracker's own cash-flow summary SHALL be fetched again after a capture is recorded, undone, edited or deleted, and SHALL never be added to or from local captures.

#### Scenario: Recorded capture and the summary
- **WHEN** a capture of Rp10.000 is recorded
- **THEN** Beranda shows no local amount for it, and the tracker summary is requested again rather than increased on the phone
