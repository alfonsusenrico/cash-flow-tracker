## ADDED Requirements

### Requirement: A recorded capture shows its tags
When the tracker's view of a recorded expense or income lists tags, the result card SHALL show one line "Tag: " followed by the tag names in the tracker's order, separated by ", ", and Rincian's recorded facts SHALL include a "Tag" row with the same names. A view without tags, including one from a tracker that does not send them, SHALL show neither. Tag names SHALL be shown as plain text.

#### Scenario: A tagged capture
- **WHEN** the tracker records "fellow coach: bayar makan di Kafe Contoh 45rb" with the tag "fellowship coach"
- **THEN** the result card shows "Tag: fellowship coach" and Rincian lists "Tag" with "fellowship coach"

#### Scenario: Two tags
- **WHEN** the recorded transaction carries "fellowship coach" and "Malang trip"
- **THEN** the line reads "Tag: fellowship coach, Malang trip"

#### Scenario: No tags or an older tracker
- **WHEN** the view has an empty tag list, or has no tag field at all
- **THEN** neither the tag line nor the "Tag" row is shown

#### Scenario: A stored capture from before this version
- **WHEN** a capture recorded before this version is opened in Rincian
- **THEN** it opens as before, without a "Tag" row

### Requirement: Editing on the phone keeps tags
Editing a recorded capture on the phone SHALL NOT change its tags, and the refreshed view after the edit SHALL show the tags the tracker still holds.

#### Scenario: Edit a tagged capture
- **WHEN** the owner changes the category of a capture tagged "fellowship coach" with "Ubah"
- **THEN** the request sent to the tracker carries no tag fields, and the result still shows "Tag: fellowship coach"
