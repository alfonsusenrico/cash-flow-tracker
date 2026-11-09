## MODIFIED Requirements

### Requirement: Concise Labels and Accessible Contextual Help
Every active financial input form SHALL keep a concise persistent label for each control and SHALL avoid captions that restate the label, section title or evident operation. Guidance for an unusual field or domain concept SHALL be one visible hint line under its control, programmatically associated with it. Validation, outcomes, destructive consequences and financial warnings SHALL stay visible when relevant and never depend on hover.

#### Scenario: Required behavior and constraints
- **WHEN** an owner reads or interacts with a financial input form
- **THEN** the following required behavior and constraints hold:

  Every active financial input form SHALL keep a concise persistent label for each control and SHALL avoid visible captions that merely restate the label, section title, or evident operation. Guidance required only to explain an unusual field or domain concept SHALL be one hint line under the control, referenced by `aria-describedby`. No information icon or hover-only explanation SHALL be used. Essential validation, transaction outcomes, destructive consequences, and financial warnings SHALL remain visible when relevant.

#### Scenario: Opening field guidance without a pointer
- **WHEN** a keyboard or touch user reaches "Pilar Kakeibo", "Kaitkan ke target", "Bayar tagihan / utang" or "Biaya / bunga" in Catat
- **THEN** its hint line SHALL be visible under the control and announced with it, without any extra control to open

#### Scenario: Correcting an invalid amount
- **WHEN** an amount fails validation
- **THEN** the error remains visibly adjacent to its labelled field and is not hidden in a hint
