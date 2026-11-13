## Context

`build_cycle_summary` computes `remaining` from Masuk, Keluar and the scheduled lines, and `safe_to_spend.per_day` from it. It already receives every non-archived account with its balance, role and credit facts. `summarize_account_balances` treats cash, bank and wallet accounts as liquid; roles split them into harian, tabungan and dana_investasi, and only leaves count in figures. Credit accounts carry `credit_owed`. `unpaid_plan_lines` and `_goal_lines` build Terjadwal; transfer plans into credit accounts are counted only up to opening debt.

Production check on 2026-11-13 (read-only): daily pockets Rp 4.420.334, of which Rp 2.998.395 sit in a pocket the owner uses to set credit money aside; Spaylater owes Rp 3.046.014; no plans due.

## Decisions

1. **Formula.** Aman dipakai = daily money − credit owed − scheduled from daily money, never below 0. A shortfall is reported as `short_by`. Rejected: keeping the salary view with carry-over and returned-money lines added (option 3 offered on 2026-11-13); it reaches the same number with more parts to explain, and the owner does not care about the month.
2. **Daily money.** The own balance of every active cash, bank or wallet leaf account with role `harian`, negative balances included because a pocket below zero is money short. Savings (`tabungan`), investment cash (`dana_investasi`), investments and credit accounts are not daily money. Rejected: a per-pocket "not spendable" switch; with credit owed subtracted in full, money set aside for credit already nets out, and no current case needs it.
3. **Credit owed in full.** Everything owed on active credit accounts, including instalments due after the next salary, the same total as Beranda's "Tagihan kredit". It is money already spent. Rejected: only bills due before the next salary, which would count money set aside for a later bill as free. An overpaid credit account adds nothing.
4. **Scheduled from daily money.** Unpaid plan occurrences in the existing window whose source is a daily-money account or not set, plus the unfunded part of goal set-asides. Transfer plans into credit accounts are left out (decision 3 already subtracts that debt), and plans paid from savings are left out (that money is not daily money). The lines and their window are the existing ones otherwise.
5. **Response.** `safe_amount`: `daily_money` {`total`, `accounts`: id, name, parent name, balance}, `credit_owed` {`total`, `accounts`: id, name, owed, next due}, `scheduled` {`total`, `lines`, `window_end`, `window_basis`}, `amount`, `short_by`. `remaining` is removed; Beranda is its only reader. `safe_to_spend.per_day` divides `amount`.
6. **Hero.** Label "Aman dipakai" ("Safe to spend"); its line keeps the days until the expected salary, or the reason, and when short says "Kurang Rp X untuk tagihan kredit dan rencana". The split bar divides daily money into Tagihan kredit, Terjadwal and Aman dipakai, named with their shares.
7. **Rincian.** Rule line, then Uang harian (opens per pocket, largest first), − Tagihan kredit (opens per account with its next due), − Terjadwal (opens per bill and set-aside, as today), = Aman dipakai, or "Kurang" and Rp 0. The salary view leaves Rincian; the supporting figures keep this cycle's Masuk, Pengeluaran and Ditabung & investasi. Its Masuk, Pengeluaran (with the credit lines), Ditabung and waiting-row breakdowns go with it, together with their messages; the figures themselves link to Transaksi. Terjadwal on Beranda reads `safe_amount.scheduled`, while Target & Tagihan keeps reading `scheduled`, unchanged.
8. **Tagihan kredit dialog.** Its meta becomes "Sudah dikurangi dari Aman dipakai" ("Already taken out of Safe to spend"), because the figure now subtracts it.

## Failure behaviour

Unchanged: the summary fails as a whole with the existing error state. A credit account without due dates still counts what it owes.

## Risks

Long instalment plans lower the figure by their whole remaining debt; that is the conservative reading of "safe" and is visible in Rincian. Balances that are stale lower or raise the figure until reconciled, as they already do in Rekening.

## Rollback

Revert the commits. No data change.
