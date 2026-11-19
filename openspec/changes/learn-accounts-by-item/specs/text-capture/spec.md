## ADDED Requirements

### Requirement: Item Groups of Captures
Each recorded capture SHALL belong to an item group that names what was paid for. A phrase SHALL keep the group it was first recorded with; a new phrase SHALL take its first meaningful word when the owner has captures in that group, otherwise the group the model names from the owner's groups or from the text, otherwise its first word.

#### Scenario: A new parking place
- **GIVEN** recorded captures "parkir ptc 10rb cash" and "parkir k24 2rb cash"
- **WHEN** the owner sends "parkir lenmarc 5rb"
- **THEN** its item group SHALL be "parkir" without asking the model for the group

#### Scenario: Different wording for the same item
- **GIVEN** the item group "parkir" and no group "parkiran"
- **WHEN** the owner sends "parkiran mall 5rb" and the model names "parkir"
- **THEN** its item group SHALL be "parkir" and SHALL be kept for that phrase

#### Scenario: Numbers and one-letter words are not items
- **WHEN** the phrase is "20 45 parkir ptc" or "07 55 e money"
- **THEN** its first word SHALL be "parkir" or "money"

#### Scenario: An unproven group
- **WHEN** the model names a group that is neither one of the owner's groups nor words of the text
- **THEN** the group SHALL be the phrase's first word

### Requirement: Accounts Learned by Item
When the text names no account and the exact phrase has no learned account, the account SHALL come from the item group's latest 10 recorded captures, a later correction counting twice: recorded when (agreeing + 1) / (total + 2) is at least 0.75, otherwise asked with that account preselected. An explicit mention SHALL still win.

#### Scenario: Parking is paid in cash
- **GIVEN** 6 recorded parking captures on Cash under different phrases
- **WHEN** the owner sends "parkir 10rb"
- **THEN** it SHALL be recorded on Cash without a question

#### Scenario: Mixed history asks
- **GIVEN** parking captures 3 on Cash and 2 on GoPay
- **WHEN** the owner sends "parkir 10rb"
- **THEN** the account question SHALL be asked with Cash preselected

#### Scenario: A correction counts twice
- **GIVEN** 2 parking captures on GoPay and 1 moved by the owner from GoPay to Cash
- **WHEN** the owner sends "parkir 10rb"
- **THEN** Cash SHALL be preselected

#### Scenario: A named account wins
- **GIVEN** parking captures all on Cash
- **WHEN** the owner sends "parkir 10rb gopay"
- **THEN** it SHALL be recorded on GoPay

#### Scenario: Two accounts named
- **GIVEN** a pocket "Potong Rambut" and tip captures on Cash
- **WHEN** the owner sends "cash 10rb buat tip potong rambut"
- **THEN** the account question SHALL offer Cash and Potong Rambut with Cash preselected

### Requirement: Preview Shows the Learned Account
The capture preview SHALL show the account the capture would be recorded on from the owner's habit (the phrase's memory, its history or its item group, known without the model) with "dari kebiasaan", and SHALL never call the model.

#### Scenario: Preview before sending
- **GIVEN** parking captures all on Cash
- **WHEN** the owner types "parkir 10rb"
- **THEN** the preview's account SHALL read "Cash · dari kebiasaan"

### Requirement: Bare Clock Time After a Day Word
A clock time written as `HH.MM` or `HH:MM` directly after a day word ("kemarin", "tadi", "hari ini", "semalam" or a weekday) SHALL be read as the capture's time and SHALL not be part of the description or the item.

#### Scenario: Yesterday at 20.45
- **WHEN** "kemarin 20.45 bayar parkir ptc cash 10rb" is sent on 18 Nov
- **THEN** the transaction SHALL be recorded on 17 Nov at 20:45, its phrase key SHALL be "parkir ptc", and its amount SHALL be 10.000
