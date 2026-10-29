## ADDED Requirements

### Requirement: History-Based Capture Suggestions
`GET /api/capture/suggestions` SHALL return at most 6 suggestions built only from the owner's own captures and reviewed manual or capture transactions of the last 90 days. They SHALL be ranked by time-of-day and weekday fit, frequency and recency, with no model call. Each suggestion SHALL carry a prefill `text`, `label`, `amount`, category, account and `reason`.

#### Scenario: Morning parking habit
- **GIVEN** the owner recorded "parkir ptc 10rb" at about 08:00 on five weekdays in the last month
- **WHEN** suggestions are requested on a weekday at 08:30
- **THEN** the first suggestion SHALL be "parkir ptc 10rb" with amount 10.000 and reason "biasanya jam segini".

#### Scenario: Bank-notified spending is not suggested
- **GIVEN** frequent café payments recorded only from bank notifications
- **WHEN** suggestions are requested
- **THEN** none of those payments SHALL appear.

#### Scenario: No history
- **WHEN** a new owner requests suggestions
- **THEN** the list SHALL be empty.

#### Scenario: Invalid or foreign requests
- **WHEN** `device_time` has no offset or the request is unauthenticated
- **THEN** the response SHALL be 422 or 401, and no other owner's history SHALL ever be used.
