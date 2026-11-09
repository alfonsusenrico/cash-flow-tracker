## MODIFIED Requirements

### Requirement: Seberapa Sering Ditanya Panel
Pengaturan › Kebiasaan SHALL show a "Seberapa sering ditanya" panel listing the weeks newest first with "Ditanya <n> dari <m> (<p>%)", the shares by memory, habit, AI, other rules and answered, and the corrections; a week without recorded payments SHALL say so, no data SHALL give an honest empty state, and a failed load SHALL show no figures. Analisis SHALL NOT show the panel.

#### Scenario: Weekly figures
- **WHEN** the current week has 20 recorded payments, 3 asked and 5 by habit
- **THEN** Pengaturan › Kebiasaan SHALL mark it "berjalan" and show "Ditanya 3 dari 20 (15%)" and "Kebiasaan 5 (25%)"

#### Scenario: No data
- **WHEN** no notification or capture payment was recorded in the eight weeks
- **THEN** the panel SHALL say so and show no figures

#### Scenario: Not on Analisis
- **WHEN** the owner opens any tab of Analisis
- **THEN** no "Seberapa sering ditanya" or "Kebiasaan dihafal" panel SHALL be shown, and Pengaturan › Kebiasaan SHALL show the weekly panel after the habit and top-up tables
