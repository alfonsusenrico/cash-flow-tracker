## REMOVED Requirements

### Requirement: Credit Lines in the Pengeluaran Breakdown
**Reason**: The Rincian now explains the cash-based Aman dipakai (Uang harian, Tagihan kredit, Terjadwal) and no longer has a Pengeluaran line to open (count-safe-amount-from-daily-money D7). Pengeluaran links to Transaksi since open-beranda-figures.
**Migration**: Credit spending stays visible per account in the Tagihan kredit dialog and in Transaksi · Kredit; `spending.credit` stays in the cycle summary API.
