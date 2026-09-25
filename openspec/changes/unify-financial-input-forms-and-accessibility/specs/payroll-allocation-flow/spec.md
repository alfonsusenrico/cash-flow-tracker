# Spec Delta

## MODIFIED Requirements

### Requirement: Payroll Allocation Bundle Configuration
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
