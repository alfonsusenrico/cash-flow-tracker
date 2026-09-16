## ADDED Requirements

### Requirement: Bank Jago Pocket Movement Notifications
The mobile companion app and the backend notification ingestion pipeline SHALL recognize and parse all Bank Jago pocket transfer notifications (both single-pocket and dual-pocket syntax) as internal financial transfers.

#### Scenario: Single-pocket movement out of a pocket
- **GIVEN** a Bank Jago notification with text "You've moved Rp500.000 out of your My Emergency Fund Pocket."
- **WHEN** evaluated by the whitelist parser
- **THEN** it SHALL be recognized as a verified financial transaction with `event_class = "transfer"`, `expected_amount = 500000`, `expected_direction = "internal"`, and `source_pocket = "My Emergency Fund"`
- **AND** the destination SHALL default to the main account balance ("Kantong Utama").

#### Scenario: Single-pocket movement into a pocket
- **GIVEN** a Bank Jago notification with text "You've moved Rp500.000 into your Tabungan Pocket."
- **WHEN** evaluated by the whitelist parser
- **THEN** it SHALL be recognized as a verified financial transaction with `event_class = "transfer"`, `expected_amount = 500000`, `expected_direction = "internal"`, and `target_pocket = "Tabungan"`
- **AND** the source SHALL default to the main account balance ("Kantong Utama").

#### Scenario: Dual-pocket movement between two pockets
- **GIVEN** a Bank Jago notification with text "Rp500.000 has been moved from your Main Pocket Pocket to your GoPay Tabungan Pocket."
- **WHEN** evaluated by the whitelist parser
- **THEN** it SHALL be recognized as a verified financial transaction with `event_class = "transfer"`, `expected_amount = 500000`, `expected_direction = "internal"`, `source_pocket = "Main Pocket"`, and `target_pocket = "GoPay Tabungan"`.
