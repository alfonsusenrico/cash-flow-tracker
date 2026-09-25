# Spec Delta

## Purpose

Defines consistent, accessible interaction and validation behavior for every financial data-entry form across desktop and mobile presentations.

## ADDED Requirements

### Requirement: Semantic Financial Forms
Every financial input flow SHALL use a semantic form with a submit action and SHALL provide each control with a programmatically associated label, stable name, appropriate input mode, and autocomplete value when applicable. Pressing Enter from a compatible field SHALL submit the form once.

#### Scenario: Submitting with the keyboard
- **WHEN** a keyboard user completes a valid financial form and presses Enter from an amount or text field
- **THEN** the same validated submission runs as when the visible submit button is activated

### Requirement: Actionable Validation and Submission Feedback
Forms SHALL validate required and cross-field constraints before submission, associate each error with its control, announce asynchronous errors and success states, focus the first invalid control, and prevent duplicate submission while a request is pending. User-entered values SHALL remain available after a failed request.

#### Scenario: Correcting multiple invalid fields
- **WHEN** a user submits a form with an invalid amount and missing account
- **THEN** both controls expose their errors, the first invalid control receives focus, and no request is sent

#### Scenario: Preserving input after server rejection
- **WHEN** a server rejects a completed form for a financial constraint
- **THEN** the error is announced and the user's entered values remain available for correction

### Requirement: Accessible Modal and Bottom-Sheet Focus
Each modal or bottom sheet SHALL have a unique accessible name, move focus to a meaningful initial element, contain sequential focus while open, close with Escape when safe, restore focus to its trigger, prevent background scrolling, and contain overscroll. Motion SHALL honor reduced-motion preferences.

#### Scenario: Completing a dialog with keyboard focus
- **WHEN** a keyboard user opens and closes a financial modal
- **THEN** focus remains inside while open and returns to the opening control after close

### Requirement: Accessible Choice Controls
Icon-only actions and visual color, icon, category, account, and segmented-choice controls SHALL expose an accessible name, selected state, disabled state, and keyboard-operable target of adequate size without relying on color alone.

#### Scenario: Selecting a category color without vision
- **WHEN** a screen-reader user navigates the category color choices
- **THEN** each choice announces its name and current selected state

### Requirement: Canonical Responsive Form Behavior
Each financial action SHALL have one canonical state and validation contract reused by desktop modal and mobile bottom-sheet presentations. Entry points MAY choose different responsive layouts but SHALL expose equivalent fields, defaults, errors, and results.

#### Scenario: Opening transfer from different screens
- **WHEN** the user opens internal movement from Home, Accounts, or global navigation
- **THEN** each entry point uses the same fields, date behavior, validation, and submission contract

### Requirement: Investment Position Choices Match Trade-Only Contract
Generic movement, recurring-transfer, and payroll-allocation forms SHALL offer only eligible liquid accounts as source or destination. They SHALL explain that investment positions are changed through Beli/Jual, while liquid investment-funding accounts remain selectable.

#### Scenario: Selecting movement accounts
- **WHEN** the user opens a generic movement form while both liquid accounts and investment positions exist
- **THEN** only eligible liquid accounts are offered and the form explains that investment positions use Beli/Jual

#### Scenario: Configuring recurring or payroll transfer accounts
- **WHEN** the user configures a generic recurring or payroll transfer
- **THEN** investment positions are not selectable as source or destination, while liquid funding accounts remain selectable

### Requirement: Deposit Principal Entry
The account and pocket forms SHALL represent a deposit position using its opening principal. They SHALL NOT ask for an annual interest rate unless the backend persists and calculates that rate under a distinct contract. Existing deposit records SHALL remain unchanged by this form correction.

#### Scenario: Creating a deposit pocket
- **WHEN** a user chooses Deposito for a new investment pocket and enters the opening principal
- **THEN** the form sends that principal as the opening value without submitting it as an average purchase price or annual interest rate

### Requirement: Concise Labels and Accessible Contextual Help
Every active financial input form SHALL keep a concise persistent label for each control and SHALL avoid visible captions that merely restate the label, section title, or evident operation. Guidance required only to explain an unusual field or domain concept SHALL be available from a labelled information control adjacent to the relevant label or section. The explanation SHALL be available by keyboard and touch as well as optional hover, and SHALL have a programmatic relationship to the field or group. Essential validation, transaction outcomes, destructive consequences, and financial warnings SHALL remain visible when relevant rather than depending on hover.

#### Scenario: Opening field guidance without a pointer
- **WHEN** a keyboard or touch user activates an information control beside a field label
- **THEN** its explanation becomes visible and accessible without changing the field value, and can be dismissed predictably

#### Scenario: Correcting an invalid amount
- **WHEN** an amount fails validation
- **THEN** the error remains visibly adjacent to its labelled field and is not hidden in an information control
