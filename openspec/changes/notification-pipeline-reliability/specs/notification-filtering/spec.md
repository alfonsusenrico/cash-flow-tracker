# Spec Delta

## MODIFIED Requirements

### Requirement: Discard Non-Financial Notifications at Listener Entrance
The companion mobile notification listener SHALL forward every status bar notification from an enabled registered financial package to the backend, discarding locally only notifications with no title and no text, and notifications that carry a one-time password or verification code.

#### Scenario: Required behavior and constraints
- **WHEN** the companion listener receives a financial application notification
- **THEN** the following required behavior and constraints hold:

  The companion mobile notification listener SHALL forward every status bar notification from an enabled registered financial package to the backend, discarding locally only notifications with no title and no text, and notifications that carry a one-time password or verification code. The listener SHALL attach parser hints (`event_class`, `expected_amount`, `expected_direction`, `expected_counterparty`) only when a known pattern matches, and SHALL send `is_financial = null` with no hints otherwise. The backend's acceptance result is the sole authority on whether the notification is financial.

#### Scenario: Promotional or marketing notifications are dropped immediately
- **GIVEN** a notification from an enabled registered package (e.g. `com.gojek.app` or `com.shopeepay.id`)
- **WHEN** the notification contains promotional keywords, marketing slogans, or ride discounts without a settled transaction
- **THEN** the listener forwards it with `is_financial = null`
- **AND** the backend answers `ignored` without storing it
- **AND** the listener marks the local row ignored so it is pruned and never alerts the owner

#### Scenario: Verified transaction notifications are captured and synced
- **GIVEN** a notification from an enabled registered package (e.g. `com.gojek.app` or `com.bca`)
- **WHEN** the notification matches a known local transaction pattern with a positive numeric amount
- **THEN** the listener SHALL extract the structured hints (`event_class`, `expected_amount`, `expected_direction`, `expected_counterparty`)
- **AND** the listener SHALL persist the entity to the local database with `is_financial = true`
- **AND** the listener SHALL schedule immediate synchronization to the backend

#### Scenario: Unknown bank notification format is forwarded
- **WHEN** an enabled package posts a settled transaction notification whose wording matches no local pattern (for example a Bank Jago outbound transfer)
- **THEN** the listener persists the entity with `is_financial = null` and no hints, schedules synchronization, and shows it in the inbox as unrecognised until the backend result arrives

#### Scenario: One-time password never leaves the device
- **WHEN** an enabled package posts a notification containing an OTP or verification code
- **THEN** the listener discards it without persisting or synchronizing it
