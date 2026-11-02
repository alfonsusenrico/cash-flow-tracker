## ADDED Requirements

### Requirement: Refused purpose answers from the notification stay answerable

When the companion refuses or fails to store an answer sent from the "Apa ini?" prompt, it SHALL show the row's open question again in the same slot, silently, with its actions and a statement that the answer was not saved, unless an answer other than a failed one is already stored or the row asks no question.

#### Scenario: Blank or missing text from the prompt
- **WHEN** the owner sends "Tulis" from the prompt with blank text or the reply text does not arrive
- **THEN** nothing is stored and the prompt still offers its suggestions and "Tulis" and says the answer was not saved

#### Scenario: Prompt answered on a locked device
- **WHEN** a prompt action reaches the companion while the device is locked
- **THEN** nothing is stored and the prompt stays answerable and says to unlock and answer again

#### Scenario: Prompt action for a changed question
- **WHEN** an action for an earlier question of the row arrives
- **THEN** nothing is stored and the row's open question is offered with its actions

### Requirement: Purpose answer attempts are traced privately

The companion SHALL record each step of every purpose answer attempt from the notification or the app with the same private, bounded reason-code record that sender answers use. Tanya SHALL state that an answer from the notification was not saved while the newest settled attempt for the shown purpose question is a refused notification answer.

#### Scenario: Refused prompt answer is traced
- **WHEN** a prompt answer is refused because the device is locked
- **THEN** the record holds a received step and a locked step for that question with the answer kind `purpose`, without the answer text

#### Scenario: Tanya shows an unsaved prompt answer
- **WHEN** the owner opens Tanya on a purpose question whose newest settled attempt is a refused notification answer
- **THEN** the card says the answer from the notification was not saved and keeps every option available
