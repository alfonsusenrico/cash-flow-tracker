# Spec Delta

## ADDED Requirements

### Requirement: Immediate Pairing Window Setting
The time window for pairing a new notification leg with a counterpart at once SHALL be configured by `NOTIFICATION_PAIRING_WINDOW_SECONDS`, default 900 seconds, accepted from 30 to 86400 seconds. A value outside that range or not a whole number SHALL fall back to the defaults of the processor settings and report `processor_configuration_invalid`.

#### Scenario: Default window
- **WHEN** `NOTIFICATION_PAIRING_WINDOW_SECONDS` is not set
- **THEN** counterparts up to 900 seconds apart SHALL be eligible for immediate pairing

#### Scenario: Out-of-range window
- **WHEN** `NOTIFICATION_PAIRING_WINDOW_SECONDS` is `29`
- **THEN** the window SHALL be 900 seconds and the processor configuration error SHALL be `processor_configuration_invalid`
