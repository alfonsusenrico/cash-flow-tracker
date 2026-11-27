## MODIFIED Requirements

### Requirement: Promotional Wording Without a Transaction Is Ignored
The backend SHALL classify a notification as `ignored` when it is not a recognised transaction and either contains promotional or market-information wording (including dividend, yield, issuer, or commodity news) or is an unsettled commentary alert from Stockbit, before foreign-currency review checks run. Promotional wording SHALL NOT, on its own, cause a recognised transaction to be ignored.

#### Scenario: Top-up promotion
- **GIVEN** a notification "Top up sekarang dan dapatkan bonus Rp10.000!"
- **WHEN** it is ingested
- **THEN** it SHALL be `ignored`.

#### Scenario: Stockbit dividend or commodity news with US dollar figures is ignored
- **GIVEN** a Stockbit notification "Emiten Contoh: Dividen Interim Yield 8,5% senilai US$120 juta, setara Rp450/saham" or "Komoditas Sell Off: Harga turun ke US$9.200/ton"
- **WHEN** it is ingested
- **THEN** it SHALL be `ignored`, not `needs_review` with `unsupported_currency`.

#### Scenario: Real Stockbit trade match and RDN deposit still record
- **GIVEN** a Stockbit notification "Pembelian 2 lot ABCD match di harga Rp4.500" or "Deposit Berhasil: Dana kamu senilai Rp500.000 sudah dapat digunakan"
- **WHEN** it is ingested
- **THEN** it SHALL NOT be `ignored` and SHALL process normally.

#### Scenario: Settled foreign-currency bank payment still routes to review
- **GIVEN** a Bank Jago notification "You've paid USD 10.00 using your debit card"
- **WHEN** it is ingested
- **THEN** it SHALL NOT be `ignored` and SHALL remain `needs_review` with `unsupported_currency`.
