# Specification: Modern Fintech UI & Kakeibo Lifestyle Engine

## ADDED Requirements

### Requirement: Kakeibo Lifestyle Tracking
The system SHALL classify expense categories and transactions into three Kakeibo pillars: `need` (Kebutuhan), `want` (Keinginan), and `saving` (Tabungan / Investasi).

#### Scenario: Categorize and evaluate Kakeibo spending ratios
- **GIVEN** a user has recorded income of Rp 10.000.000 in the current monthly cycle
- **WHEN** the user spends Rp 4.500.000 on primary categories (`need`), Rp 3.500.000 on lifestyle categories (`want`), and allocates Rp 2.000.000 to emergency fund/savings (`saving`)
- **THEN** the system SHALL compute `need_pct` as 45.0%, `want_pct` as 35.0%, and `saving_pct` as 20.0%
- **AND** the system SHALL flag `want_status` as "warning" or "elevated" since it exceeds the 30% ideal ceiling.

### Requirement: Narrative Financial Insights
The system SHALL compute period-over-period comparison deltas and generate natural language summaries identifying top spend drivers.

#### Scenario: Generate comparative narrative insight
- **GIVEN** the user spent Rp 8.000.000 in the previous cycle and Rp 10.000.000 in the current cycle
- **AND** the category "Makan & Minum" increased from Rp 2.000.000 to Rp 3.500.000
- **WHEN** the dashboard overview endpoint `GET /api/dashboard/overview` is called
- **THEN** the response SHALL include `comparison.outflow_delta_pct` as +25.0%
- **AND** the response SHALL include a natural language narrative string explicitly mentioning the overall spend jump (+25.0%) and the primary driver ("Makan & Minum").

### Requirement: Tactile Calculator Keypad
The system SHALL provide a tactile 4x4 keypad with built-in arithmetic operators for mobile touch inputs and rapid modal entry.

#### Scenario: Evaluate inline arithmetic in transaction entry
- **GIVEN** the quick capture modal is open
- **WHEN** the user enters `25000 + 15000` via the numpad or keyboard
- **THEN** the system SHALL evaluate the expression in real time to `40000`
- **AND** displaying `= Rp 40.000` as the evaluated total before submission.

### Requirement: Floating Capsule Navigation
The system SHALL render a floating rounded capsule navigation dock on mobile screens (< 1024px) with an active pill indicator.

#### Scenario: Mobile viewport navigation docking
- **GIVEN** the application is rendered on a viewport width below 1024px
- **WHEN** a user navigates between routes (`/`, `/ledger`, `/insights`, `/accounts`)
- **THEN** the active tab SHALL be wrapped in a highlighted pill container
- **AND** the dock SHALL float above the bottom edge with backdrop-filter blur.
