## ADDED Requirements

### Requirement: Active Account Name Beats AI-Learned Alias
When resolving an institution or pocket name from a notification, the backend SHALL check owner-confirmed aliases first, then active account names (exact normalized match, then singular/plural stem), and SHALL fall back to AI-learned aliases only when no active account matches by name.

#### Scenario: Active pocket overrides stale AI alias
- **GIVEN** an active Jago pocket named `"Bayar Kredit"` and an AI-learned alias mapping `"bayar kredit"` to `"Cicilan"`
- **WHEN** a Jago notification names `"Bayar Kredit Pocket"`
- **THEN** the backend SHALL resolve it to `"Bayar Kredit"` rather than `"Cicilan"`

#### Scenario: Owner-confirmed alias still overrides account name
- **GIVEN** an owner-confirmed alias mapping `"goPay tabungan"` to `"Dompet Utama"` and a pocket named `"GoPay Tabungan"`
- **WHEN** a Jago notification names `"GoPay Tabungan Pocket"`
- **THEN** the backend SHALL resolve it to `"Dompet Utama"`

### Requirement: Locked Pocket Names Normalize to Their Base Pocket
Notification pocket-name normalization SHALL strip standalone `"locked"` and `"terkunci"` words alongside `"pocket"`, `"kantong"`, `"your"`, and `"my"`, so locked Bank Jago pockets resolve directly to their matching active pocket.

#### Scenario: Locked Jago pocket matches base pocket
- **GIVEN** an active Bank Jago pocket named `"Dana Tagihan"`
- **WHEN** a Jago notification reports a transfer to `"Dana Tagihan Locked Pocket"`
- **THEN** the backend SHALL resolve the target directly to `"Dana Tagihan"` without requiring AI mapping or confirmation
