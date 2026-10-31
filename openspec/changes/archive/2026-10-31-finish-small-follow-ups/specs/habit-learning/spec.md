## MODIFIED Requirements

### Requirement: Habit Guess First
When a payment with habit labels is asked, its first suggestion SHALL be the habit's guess, kind `habit`, labelled "Sepertinya <kategori>?", carrying the guessed notes, category, pillar and its basis; an identical history suggestion SHALL NOT be repeated. On Tinjau, the guess's button SHALL show the basis as "<n> dari <m> jawaban sebelumnya", and the basis SHALL be part of the button's accessible name.

#### Scenario: Guess after two answers
- **GIVEN** two answers "makan siang" in "Makanan & Minuman"
- **WHEN** a third payment of the key is asked
- **THEN** its first suggestion SHALL be "Sepertinya Makanan & Minuman?" with notes "makan siang" and basis 2 of 2

#### Scenario: Basis on Tinjau
- **GIVEN** an open question whose habit guess has a basis of 3 agreeing of 4 labelled payments
- **WHEN** the owner opens Tinjau
- **THEN** the guess's button SHALL read "Sepertinya Makanan & Minuman?" with "3 dari 4 jawaban sebelumnya", its accessible name SHALL include both, and the other suggestions SHALL show no basis
