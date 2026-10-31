## ADDED Requirements

### Requirement: Beranda insight uses the fair comparison
The Beranda insight card, on desktop and phone, SHALL be built from the fair period comparison for the selected salary cycle or calendar month. It SHALL name the period and the number of compared days, state the change in outflow and the largest category increases and decreases as amounts, and SHALL NOT show a percentage change or a savings verdict. Below 3 compared days, or without previous data, it SHALL say so instead of comparing.

#### Scenario: Day one of a salary cycle
- **GIVEN** a salary cycle that started today and a previous cycle with Rp 4.000.000 of spending
- **WHEN** the owner opens Beranda
- **THEN** the card SHALL say that only 1 day has passed and that the comparison appears after 3 days, and SHALL show no decrease, percentage or "HEMAT" badge

#### Scenario: Enough elapsed days
- **GIVEN** the 10th day of the current month with Rp 1.200.000 spent and Rp 1.000.000 spent in the first 10 days of the previous month
- **WHEN** the owner opens Beranda with the calendar-month basis
- **THEN** the card SHALL say that outflow over the first 10 days is Rp 200.000 higher than the previous month over the same days, and SHALL list the largest category increases and decreases with their amounts

#### Scenario: Previous period without transactions
- **WHEN** the previous period has no recorded transactions
- **THEN** the card SHALL say that there is no previous data to compare with and SHALL show only the current outflow

#### Scenario: Comparison unavailable
- **WHEN** the comparison request fails
- **THEN** the card SHALL say that the comparison could not be loaded and SHALL show no figures
