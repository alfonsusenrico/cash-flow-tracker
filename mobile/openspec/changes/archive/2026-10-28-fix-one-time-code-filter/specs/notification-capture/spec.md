## Purpose

Define which notifications from enabled financial apps the listener stores locally and forwards to the tracker, so that real transactions are never lost on the device.

## ADDED Requirements

### Requirement: One-time codes never leave the device
The listener SHALL discard a notification before storing or sending it when its title contains a one-time-code phrase, or when its text shows a 4–8 digit code within 30 characters of such a phrase. A Rupiah amount SHALL NOT count as a code. The phrases are: OTP, one-time password, verification code, security code, kode verifikasi/OTP/akses/keamanan/rahasia, and PIN when a code sits next to it.

#### Scenario: Code in the body is dropped
- **WHEN** a notification reads "Kode OTP 123456 untuk transfer Rp50.000"
- **THEN** the listener SHALL NOT store or send it.

#### Scenario: Code before the phrase is dropped
- **WHEN** a notification reads "998877 adalah kode verifikasi Anda. Jangan berikan kepada siapa pun"
- **THEN** the listener SHALL NOT store or send it.

#### Scenario: One-time-code title is dropped
- **WHEN** a notification's title is "Your verification code"
- **THEN** the listener SHALL NOT store or send it.

#### Scenario: PIN with a code is dropped
- **WHEN** a notification reads "PIN sementara Anda 4821. Segera ganti"
- **THEN** the listener SHALL NOT store or send it.

### Requirement: Safety footers do not hide transactions
The listener SHALL store and forward a notification from an enabled app that mentions PIN, OTP or verification codes only as a warning, without a code next to the phrase, like any other notification.

#### Scenario: Transaction with a safety footer is kept
- **WHEN** a notification reads "Pembayaran Rp45.000 ke Kedai Contoh berhasil. Jangan berikan PIN/OTP kepada siapa pun"
- **THEN** the listener SHALL store and forward it.

#### Scenario: Amount near a phrase is not a code
- **WHEN** a notification reads "Transfer Rp1500000 berhasil. Jangan bagikan kode OTP"
- **THEN** the listener SHALL store and forward it.
