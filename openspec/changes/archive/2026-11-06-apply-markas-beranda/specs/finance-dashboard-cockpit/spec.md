## REMOVED Requirements

### Requirement: Executive KPI Summary Ribbon
**Reason**: Beranda no longer has a KPI strip. `beranda-cycle-summary` already replaced this ribbon's "Aman dipakai hari ini", "Keluar" and comparison chips. On 2026-11-05 the owner accepted option 6 "Markas+", whose Beranda has one hero, quiet supporting figures and a context panel.
**Migration**:
- The Beranda figures are owned by `beranda-cycle-summary` ("Beranda Hero", "Safe-to-Spend Today", "Beranda Context Panel").
- The net-worth composition (liquid funds, investments and each active debt counted once) and the separate liquid, investment and total balances of the overview and net-worth APIs are owned by `net-worth-analytics`.
- Ketahanan Dana with its target multiplier is shown on Target & Tagihan and as one line in the Beranda panel.
