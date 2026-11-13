## ADDED Requirements

### Requirement: A Past Word After a Date Qualifies It
A past word ("kemarin", "kmrn", "kemaren", "kmarin", "yesterday" or "lalu", optionally after "yang" or "yg") right after an explicit day number or day-and-month date SHALL be read as part of that date, not as a second day. The date SHALL resolve as it does without the word. A past word elsewhere in the text SHALL keep its own meaning.

#### Scenario: The owner's SPayLater purchase
- **WHEN** "tgl 11 kemarin, beli pedigree spaylater kredit 47619" is sent on 13 November
- **THEN** the reading SHALL be 11 November, the amount 47.619 and the kind expense

#### Scenario: Other spellings
- **WHEN** "tanggal 11 kmrn", "tgl 20 kemarin" or "11 november kemarin" is sent on 13 November
- **THEN** the readings SHALL be 11 November, 20 October and 11 November

#### Scenario: Two different days are still refused
- **WHEN** "kemarin tgl 11 beli" or "tgl 11 beli kemarin" is sent on 13 November
- **THEN** the capture SHALL be refused as naming several dates, as before
