## MODIFIED Requirements

### Requirement: Whole-Period Comparison API
`GET /api/reports/monthly/comparison` SHALL return, for the owner and the requested basis and period, the totals, the previous period, the baseline, running progress, pillars with categories, facts and history. Running progress SHALL carry `baseline_share`, Pengeluaran so far as a whole percent of the baseline's, rounded half up, or null. `GET /api/reports/monthly` SHALL return the same object as `comparison`. Every amount SHALL equal the ledger summary of its window and filters.

#### Scenario: Equal to the ledger
- **WHEN** the comparison for Oktober is requested
- **THEN** each history period's Pengeluaran and each pillar and category amount SHALL equal `GET /api/transactions` summaries for the same window and filters

#### Scenario: Another owner
- **WHEN** another owner has spending in the same months
- **THEN** none of it SHALL appear in any figure, fact or history bar

#### Scenario: Unauthenticated or unknown period
- **WHEN** the request has no valid session or Bearer key, or names a period that is not listed
- **THEN** the response SHALL be 401, or 404 `period_not_found`, with no figures

#### Scenario: Running share of the average
- **GIVEN** Rp 1.450.000 of Pengeluaran in November so far and a baseline of Rp 3.910.167 over Agustus to Oktober
- **WHEN** the November comparison is requested on 4 November
- **THEN** `progress.baseline_share` SHALL be 37, `progress.share` and every other field SHALL keep their values, and no change against the baseline SHALL be given

#### Scenario: No baseline share
- **WHEN** the running period has fewer than two whole, non-partial periods before it, or a completed period is requested
- **THEN** `progress.baseline_share` SHALL be null, or `progress` SHALL be null as before
