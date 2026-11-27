## ADDED Requirements

### Requirement: Capture Merchant Excludes Amount Token
When validating a model normalization for a typed capture, the backend SHALL strip any substring matching the capture's parsed money expression from the proposed `merchant` (and trailing amount duplicate from `description`), cleaning leftover whitespace and separators, so transaction amounts never leak into stored merchant names.

#### Scenario: Model appends parsed amount to merchant
- **GIVEN** a typed capture `"makan warung contoh 125.000 tunai"` whose parsed amount span is `"125.000"`
- **WHEN** the model returns `merchant: "Warung Contoh 125.000"` and `description: "Makan di Warung Contoh 125.000"`
- **THEN** validation SHALL normalize `merchant` to `"Warung Contoh"` and `description` to `"Makan di Warung Contoh"`

#### Scenario: Genuine number in merchant name is preserved
- **GIVEN** a typed capture `"beli obat di apotek k24 35rb tunai"` whose parsed amount span is `"35rb"`
- **WHEN** the model returns `merchant: "Apotek K24"`
- **THEN** validation SHALL preserve `merchant` as `"Apotek K24"`
