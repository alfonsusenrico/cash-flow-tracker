# payroll-allocation-flow Specification

## Purpose
TBD - created by archiving change automated-transactions-and-payroll-allocation. Update Purpose after archive.
## Requirements
### Requirement: Payroll Allocation Bundle Configuration
The system SHALL allow users to flag and group recurring transfer and investment rules as part of their Monthly Payroll Allocation bundle (`is_payroll_allocation = true`):
1. Users SHALL be able to define the primary salary source account (e.g., BCA Payroll).
2. Users SHALL be able to configure multiple destination accounts/pockets with target allocation amounts (e.g., Living Expenses pocket, Emergency Fund, Investment portfolio).
3. The system SHALL calculate the total allocated payroll sum and show the remaining unallocated balance.

#### Scenario: Configuring payroll allocations
- **WHEN** user defines 3 payroll allocation rules totaling Rp 7.000.000 from primary BCA to Jago Pockets and Bibit
- **THEN** system saves the bundle configuration tagged with `is_payroll_allocation = true`

### Requirement: 1-Tap Payroll Allocation Modal & Execution
The application SHALL provide a 1-tap "Alokasikan Gaji" cockpit modal on the Dashboard and Accounts page:
1. When payday cycle day arrives or on user request via "Alokasikan Gaji" button, the modal SHALL open displaying all configured payroll allocations.
2. The user SHALL be able to review all allocations in a unified split card, tweak individual nominal amounts if necessary, or deselect items with a checkbox.
3. Clicking "Jalankan Alokasi Sekarang" SHALL execute all selected transfers atomically via `POST /api/recurring/payroll/execute`.
4. All affected account and pocket balances SHALL update immediately without requiring manual multi-step entries.

#### Scenario: Executing monthly payroll allocation in 1 tap
- **WHEN** user opens the Payroll Allocation modal, verifies the Rp 10.000.000 split across 4 accounts/pockets, and clicks "Jalankan Alokasi Sekarang"
- **THEN** system executes all 4 transfers atomically, updates account balances, advances rule next run dates, and presents a success summary

