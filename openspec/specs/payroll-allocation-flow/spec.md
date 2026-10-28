# payroll-allocation-flow Specification

## Purpose
Configure payroll allocation bundles and execute the chosen allocations through explicit user confirmation.

## Requirements

### Requirement: Payroll Allocation Bundle Configuration
The system SHALL allow users to explicitly flag eligible recurring transfer or investment-funding rules as payroll allocations.

#### Scenario: Required behavior and constraints
- **WHEN** an owner configures the monthly payroll allocation bundle
- **THEN** the following required behavior and constraints hold:

  The system SHALL allow users to explicitly flag eligible recurring transfer or investment-funding rules as payroll allocations:
  1. Payroll inclusion SHALL default to false and SHALL be explained at the point of selection.
  2. Each included rule SHALL identify a valid salary source and distinct allocation destination.
  3. The interface SHALL list included allocations, calculate total allocation, compare it with the selected source's available balance, and show the remaining unallocated balance.
  4. Users SHALL be able to open the originating rule for complete editing rather than delete and recreate it.
  5. Generic allocation source and destination choices SHALL exclude investment positions and explain that positions are changed through Beli/Jual; liquid investment-funding accounts remain available.

#### Scenario: Explicitly adding an allocation to payroll
- **WHEN** a user enables payroll inclusion while creating or editing an eligible transfer rule
- **THEN** the rule appears in the payroll bundle with its source, destination, amount, and next schedule visible

#### Scenario: Leaving an ordinary rule outside payroll
- **WHEN** a user saves a recurring expense without enabling payroll inclusion
- **THEN** the rule does not appear in the payroll allocation bundle

#### Scenario: Configuring payroll allocations
- **WHEN** a user explicitly configures three payroll allocations totaling 7,000,000 IDR from the primary salary account
- **THEN** the bundle lists all three destinations, total allocation, and remaining source balance

### Requirement: 1-Tap Payroll Allocation Modal & Execution
The application SHALL provide a 1-tap "Alokasikan Gaji" cockpit modal on the Dashboard and Accounts page.

#### Scenario: Required behavior and constraints
- **WHEN** an owner reviews and executes a payroll allocation batch
- **THEN** the following required behavior and constraints hold:

  The application SHALL provide a 1-tap "Alokasikan Gaji" cockpit modal on the Dashboard and Accounts page:
  1. When payday cycle day arrives or on user request via "Alokasikan Gaji" button, the modal SHALL open displaying all configured payroll allocations.
  2. The user SHALL be able to review all allocations in a unified split card, tweak individual nominal amounts if necessary, or deselect items with a checkbox.
  3. Clicking "Jalankan Alokasi Sekarang" SHALL execute all selected transfers atomically via `POST /api/recurring/payroll/execute`.
  4. All affected account and pocket balances SHALL update immediately without requiring manual multi-step entries.

#### Scenario: Executing monthly payroll allocation in 1 tap
- **WHEN** user opens the Payroll Allocation modal, verifies the Rp 10.000.000 split across 4 accounts/pockets, and clicks "Jalankan Alokasi Sekarang"
- **THEN** system executes all 4 transfers atomically, updates account balances, advances rule next run dates, and presents a success summary
