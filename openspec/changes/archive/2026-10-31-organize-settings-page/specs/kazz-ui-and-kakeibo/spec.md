## MODIFIED Requirements

### Requirement: Narrative Financial Insights
The system SHALL compute period-over-period comparison deltas and generate natural language summaries identifying top spend drivers. The overview response SHALL keep these fields for compatibility, but the web Beranda SHALL NOT present them as its insight headline; Beranda follows the fair period comparison in `observed-insights`.

#### Scenario: Generate comparative narrative insight
- **GIVEN** the user spent Rp 8.000.000 in the previous cycle and Rp 10.000.000 in the current cycle
- **AND** the category "Makan & Minum" increased from Rp 2.000.000 to Rp 3.500.000
- **WHEN** the dashboard overview endpoint `GET /api/dashboard/overview` is called
- **THEN** the response SHALL include `comparison.outflow_delta_pct` as +25.0%
- **AND** the response SHALL include a natural language narrative string explicitly mentioning the overall spend jump (+25.0%) and the primary driver ("Makan & Minum").

#### Scenario: Beranda does not show the unequal-window narrative
- **WHEN** the owner opens Beranda on desktop or phone
- **THEN** the overview narrative text and its percentage SHALL NOT be shown
