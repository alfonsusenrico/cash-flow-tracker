## Verification: organize-accounts-by-use

**Revision tested:** the working tree on `feat/accounts-by-use`, based on `1e0fcfa` (main). The tested tree is the one committed.

**Data:** synthetic only. The tests use fresh synthetic owners, and the browser check used the synthetic seed owner on a loopback stack.

### Database

- **Migrations:** Flyway 10 applies V1 → V51 on PostgreSQL 16, and the seed dump (at V44) migrates to V51.
- **Re-run:** running V51 again exits 0 and leaves the roles and layouts unchanged, including an owner-chosen role.

### Backend

| Check | Result |
| --- | --- |
| `pytest tests/ -ra` (pinned CI tools, migrated PostgreSQL), run by me on the final tree | 1914 passed, 0 failed, 0 skipped |
| New: `test_account_roles.py` | 11 tests |
| New: `test_savings_by_role.py` | 8 tests |
| New: `test_account_layouts.py` | 18 tests |
| New: `test_restructured_accounts_notifications.py` | 12 tests |

**Existing tests changed:**
- `test_kakeibo_savings.py`: the stored-leg gate is replaced by the read-time role check (D3).
- `test_kakeibo_and_insights.py`: only the mocks change, for one more query, and no expected figure changes.
- `test_cycle_summary.py`: its net-worth identity now includes `dana_investasi`.

**Notification regression (4.1):** all of these pass with no rule change, in a synthetic structure with GoPay Tabungan as a Jago pocket (linked to the Jago app) and RDN BCA under BCA:
- the BCA → GoPay top-up in three report orders, with Jago's repeat skipped through the link;
- GoPay Tabungan → Main;
- Main → GoPay Tabungan;
- a GoPay QRIS payment;
- Jago → own BCA, both orders;
- an RDN deposit, both orders;
- ordinary BCA spending, which never lands on the RDN.

### Frontend

| Check | Result |
| --- | --- |
| `npm run type-check` | exit 0 |
| `npm run lint` | exit 0 |
| `npm test` (vitest) | 77 files, 722 tests passed |
| `npm run build` | exit 0 |

The test count includes the new Beranda test for the savings words and the asset caption with Dana investasi. The copy register test passes after "Belanja dari tabungan" became "Pengeluaran dari tabungan".

### Palette (task 5.1)

The dataviz validator was run in this session on the use tokens:
- **Light** `#059669, #B7791F, #0EA5E9, #8B5CF6`: all checks pass. Sky is below 3:1 contrast, so every segment is labelled.
- **Dark** `#0F9A6E, #A9772A, #2290CC, #8F6BEF` on `#0E1118`: it passes, with a colour-blind separation warning (6.2) covered by labels and gaps.
- **Investment-kind steps:** violet, always labelled. `--kind-emas` `#C4B5FD` is below 3:1 as a mark on white, and its label carries the meaning.

### Browser (task 5.7)

**Setup:** a loopback stack (API, `next start` built from this tree, a proxy) on the synthetic seed migrated to V51.

**Restructuring through the real API:**
- Gopay moved under the Jago parent (200), and RDN BCA under BCA (200).
- RDN set to `dana_investasi` and a Jago pocket to `tabungan` (200).
- `investasi` on the RDN was refused with `account_role_type_mismatch`.

**Figures, as the server reported them:**
- **Net worth:** liquid 48.032.749 + Dana investasi 1.500.000 + investment 19.129.300 = assets 68.662.049. The lines are Dana likuid, Dana investasi, Investasi and Cicilan Contoh.
- **Cycle summary:** `tabungan` = masuk 3.600.000, total 3.600.000.
- **Beranda:** shows "Tabungan bertambah Rp 3.600.000" and "aset Rp 68.662.049 · utang Rp 2.200.200".

**Rekening:** checked in Kartu and Grafik × Per kegunaan and Per rekening, at 1440 px and 390 px, light and dark (16 views).
- axe found no serious or critical violations, the console had no errors, and nothing overflows horizontally.
- Untung/Rugi shows −Rp 130.700 (0,7%): Reksa dana +157.300 and Saham −288.000.
- The Tagihan card sits in Utang.

**Reorder:**
- Alt + → on the first Uang harian card, and Alt + ↑ three times on the Investasi row, were saved to `/accounts/layout?grouping=use`, which was read back.
- After a reload the page showed the moved card and Investasi first.
- The Per rekening layout stayed separate.

The screenshots stay in the session scratchpad.

### Not verified yet

- **4.2:** replaying the owner's 2026-11-05 Sesi uji export with Gopay moved under Jago. It needs the owner's go-ahead for a fresh read-only export, because the earlier copy was deleted.
- **Mouse drag in a real browser.** It is covered by component tests (dragStart and drop). Automation used the keyboard path.
- **Production:** checked read-only after the merge (V51, then the owner's restructuring in the app).
