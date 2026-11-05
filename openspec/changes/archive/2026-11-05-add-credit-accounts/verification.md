## Verification: add-credit-accounts

**Revision tested:** the working tree on `feat/credit-accounts`, based on `ecf2288` (main). The tested tree is the one committed.

**Data:** synthetic only. The tests use fresh synthetic owners. The browser check used the synthetic seed owner `raka_contoh` on a loopback stack, with the AI off and outbound network blocked.

### Database

- **Migrations:** Flyway 10 applies V1 → V50 on PostgreSQL 16. The seed dump (at V44) migrates to V50.
- **Re-run:** running V50 a second time is a no-op.

### Backend

| Check | Result |
| --- | --- |
| `pytest tests/ -ra` (pinned CI tools, migrated PostgreSQL) | 1865 passed, 0 failed, 0 skipped |
| `tests/test_credit_accounts.py` | 20 passed |

The 20 tests cover:
- create, with the owed amount, limit and payment day;
- each refusal: `credit_parent_not_allowed`, `credit_balance_sign`, `credit_fields_not_allowed`, `credit_type_change_has_transactions`, `credit_fee_not_allowed`, `credit_movement_needs_cash` and `credit_account_not_allowed` (investment funding, debt payment, reconcile, goal), plus `balance_check_not_liquid`;
- another owner's account answers 404;
- a purchase counts as spending at once (Pengeluaran, "Aman dipakai", no cash change);
- over the limit is saved and flagged;
- a refund lowers what is owed;
- the Bayar Kredit payment (owed 0, cycle figures unchanged);
- a payment with interest ("Biaya kredit", `unexpected`, only the fee counts as spending);
- split and delete keep or remove the fee correctly;
- a cash advance;
- insufficient funds on the bank side;
- completing a one-sided debit with credit;
- net worth with credit as utang (lines and `/dashboard/net-worth`);
- an overpaid account owes 0 and is not an asset.

`test_cycle_summary.py`'s net-worth invariant now subtracts `credit_owed`.

### Frontend

| Check | Result |
| --- | --- |
| `npm run type-check` | exit 0 |
| `npm run lint` | exit 0 |
| `npm test` (vitest) | 77 files, 710 tests passed (685 before, 25 new) |
| `npm run build` | exit 0 |

### Browser (task 3.6)

**Setup:** a loopback stack (API, `next start`, proxy) on the synthetic seed. The steps ran through the real API in the page session:
1. add "PayLater Contoh" (owed Rp 350.000, limit Rp 2.000.000, day 11) and a "Bayar Kredit Contoh" bank account;
2. a Rp 120.000 purchase;
3. a Rp 480.000 payment with a Rp 10.000 fee;
4. a further Rp 2.100.000 purchase.

**Figures, as the server reported them:**
- **Purchase:** Pengeluaran +120.000, "Aman dipakai" −120.000, owed 470.000.
- **Liabilities:** 2.200.200 → 2.670.200, and the net-worth page matches Beranda.
- **Payment:** owed 0, Masuk unchanged, Pengeluaran +10.000 (the fee only), net worth −10.000.
- **Over the limit:** the last purchase returned `over_limit: true`.

**Screens at 1440 px and 390 px, light and dark:**
- the Beranda caption reads "aset Rp 69.262.049 · utang Rp 2.670.200";
- the purchase is listed as "PayLater Contoh · Belanja Harian";
- the Rekening card shows "Utang Rp 2.100.000", "Rp 2.100.000 dari Rp 2.000.000" with the bar, "dibayar tiap tgl 11" and "Melewati limit", with no add-pocket and no "Cocokkan saldo";
- "Komposisi aset" leaves credit out;
- axe found no serious or critical violations, the console had no errors, and nothing overflows horizontally.

The screenshots stay in the session scratchpad.

### Not verified

- **Creating a credit account through the dialog in a real browser.** Component tests cover the dialog with axe, and the browser run created the account through the API.
- **Production.** It is checked read-only after the merge (V50 and the deploy).
- **Notifications.** Recognising SPayLater and Kredivo notifications is the follow-up change.
