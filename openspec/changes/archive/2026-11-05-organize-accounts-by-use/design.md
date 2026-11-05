## Context

The code was mapped on `1e0fcfa` and is cited by file.
- **Savings:** `get_kakeibo_breakdown` (`routers/dashboard.py:144-262`) works in two parts:
  - saving-pillar expenses;
  - net saving transfers: +amount into a savings account, −amount out, decided at read time from the accounts' current state (`type='investment'`, `instrument_type`, or a goal link).

  It counts only outbound legs whose stored `kakeibo_type='saving'` (`:212`), and clamps `saving_spent = max(0, …)` (`:225`). `movement_kakeibo` writes that stored value at creation (`ledger_mutations.py:125-140`). `spending_split` raises an error unless spending + saving equals Keluar (`cycle_summary.py:124-136`).
- **Liquid funds:** `summarize_account_balances` (`accounts.py:267-295`) counts every cash, bank or wallet account as liquid, so an RDN is liquid today. Ketahanan Dana falls back to all liquid funds when no emergency goal exists (`dashboard.py:325`).
- **Order:** `accounts.display_order` is the only order there is. `/accounts/reorder` writes 0..n. The web uses HTML5 drag on handles on desktop, and an "Ubah urutan" mode with move buttons on phones. No per-user preferences storage exists.
- **Re-parenting:** "Pindahkan ke rekening di bawah" moves a top-level account with history under a parent (`AccountDialogs.tsx:678-698`, `accounts.py:555-770`).
- **Notifications:** routing follows the current parents. `institution_parent` prefers a top-level match, then a unique pocket match (`notification_resolution.py:139-165`). Aliases and links are per account and survive a move. Pairing uses roots (`movement_pairing.py:102-105, 296-304, 362-375`). Moving Gopay under Jago puts a GoPay-app leg and a Jago-app leg on one root.

## Goals / Non-Goals

**Goals:**
- One role per kantong or produk that decides Saldo tersedia, Dana investasi, savings and display.
- A Rekening page that shows money by use and by rekening, with capital gain everywhere.
- A saved layout per grouping.
- Safe restructuring for the owner's GoPay and RDN.

**Non-goals:** see the proposal.

## Decisions

### D1. `accounts.role`, with defaults from today's meaning

`role TEXT NOT NULL` takes one of `harian`, `tabungan`, `dana_investasi`, `investasi` or `utang`. V51 backfills it:
- `credit` → `utang`;
- investment type or instrument → `investasi`;
- goal-linked → `tabungan`, which keeps today's saving figures;
- everything else → `harian`.

The API refuses a role that contradicts the type (`account_role_type_mismatch`):
- credit is always `utang`;
- an investment product is always `investasi`;
- a cash, bank or wallet account takes `harian`, `tabungan` or `dana_investasi`.

A parent's role is not used for figures. Only leaves count, and parents show their pockets.

Rejected alternatives:
- **Inferring the role from names** ("RDN", "Tabungan"). It is a guess that changes when a name changes.
- **Deriving the role from goal links.** A pocket can be savings without a goal, which is the owner's request.

### D2. Dana investasi is its own bucket

`summarize_account_balances` adds `dana_investasi` for role `dana_investasi`, taken out of `liquid_balance`.
- **Net worth:** `assets = liquid + dana_investasi + investment`, and the lines gain "Dana investasi".
- **Saldo tersedia and Ketahanan Dana's fallback** read `liquid_balance`, so they exclude it.
- **Movements and funds checks** still treat the account as cash, because money really moves through an RDN (`LIQUID_ACCOUNT_TYPES` is unchanged).

### D3. Savings by role, read at report time (OD-B)

A savings destination is any of:
- role `tabungan`;
- a goal link;
- an investment product.

A movement into one adds to Ditabung, and a movement out of one subtracts. This is computed at read time from both legs' accounts as they are now. The stored-leg gate (`dashboard.py:212`) gives way to this classification. `movement_kakeibo` keeps writing the stored value for other readers, unchanged.

Consequence: setting a pocket to Tabungan also reclassifies its past moves. This is the same rule that already applies when a goal is linked at read time.

Rejected alternative: classifying only from the role's start date. That needs a role history, and two moves of the same kind in one month could count differently.

### D4. The savings change may be negative, and paying from savings reduces it (OD-A)

`savings change = saving-pillar expenses + moves into savings − moves out of savings − expenses paid from a savings account`.
- **Clamp:** the `max(0, …)` clamp is removed, and the figure can be negative.
- **Keluar:** stays `Pengeluaran + savings change`, and `spending_split` keeps its check with the new figure.
- **Aman dipakai:** stays `Masuk − Keluar − Terjadwal`.
- **Beranda line:** a positive figure reads "Tabungan bertambah Rp X", and a negative one "Tabungan berkurang Rp X". The lines under it are "Masuk ke tabungan", "Diambil dari tabungan" and "Pengeluaran dari tabungan".

Worked example: Masuk 10.000.000, a move of 3.000.000 into Dana Darurat, and a 1.000.000 repair paid from Dana Darurat.
- Pengeluaran = 1.000.000.
- Savings change = 3.000.000 − 1.000.000 = 2.000.000.
- Aman dipakai = 10.000.000 − 3.000.000 = 7.000.000, which is the cash still in daily pockets.

Keeping the old clamp would understate Aman dipakai by the repair.

Monthly reports and comparisons read the same figure and follow it.

Rejected alternatives:
- **Keeping the clamp and showing "Pengeluaran dari tabungan" only as information.** Aman dipakai would be wrong whenever savings pay for something.
- **Leaving expenses paid from savings out of Pengeluaran.** The money was spent, and the category ranking must show it.

### D5. Layout per grouping

V51 adds `account_layouts (user_id, grouping CHECK IN ('use','rekening'), sections JSONB, cards JSONB, updated_at)`, with a primary key on (user_id, grouping).
- `sections` is the ordered list of section keys: uses, or rekening ids.
- `cards` maps each section key to the ordered account ids.

`GET /accounts/layout?grouping=` returns the saved order, and defaults to `display_order`. `PUT` validates every id against the owner's accounts and section keys, drops unknown ids, and appends missing ones at the end.

`/accounts/reorder` keeps its job of ordering pockets within a parent elsewhere in the app. The per-rekening layout's first save seeds from `display_order`.

Rejected alternatives:
- **Reusing `display_order` for both groupings.** One order cannot serve two groupings, which the owner asked for ("keep the draggable position").
- **Browser storage.** The order would differ between devices.

### D6. Kinds and gains from the server

Rows gain a `kind`:
- `emas` for instrument `gold`;
- `reksa_dana` for `mutual_fund`;
- `saham` for `stock`;
- `lainnya` for anything else.

Rows also gain `capital_gain` and `capital_gain_pct` on products, which already exist. The page sums gains for a kind, a rekening and the total only over products whose cost basis is known. A total with unknown parts shows "Modal belum diisi", as AssetKpis does today. The web never shows a partial sum as a gain.

### D7. The Rekening page

It follows `wallet.html` (prototypes repository), which the owner accepted on 2026-11-05.
- **Tabs:** Kartu (default) and Grafik.
- **Grouping:** Per kegunaan and Per rekening, saved per viewer in browser storage as a convenience.
- **Summary:** Kekayaan bersih, Saldo tersedia, Dana investasi, Investasi, untung/rugi and Utang. A share bar of uses sits under it, with a mini ring for Per kegunaan or composition bars per rekening for Per rekening.
- **Rails:** one per use or per rekening, with a header total and gain. Investasi has type sub-totals, and a rekening has a composition bar.
- **Cards:** equal size. A card opens the existing account detail modal.
- **Grafik tab:** a two-ring donut with a focus list.

**Colours:**
- **Uses:** the four validated categorical slots (light `#059669, #B7791F, #0EA5E9, #8B5CF6`; dark `#0F9A6E, #A9772A, #2290CC, #8F6BEF`). They become tokens `--use-harian`, `--use-tabungan`, `--use-dana`, `--use-investasi`.
- **Utang:** the expense coral.
- **Text:** uses text tokens only.
- **Investment kinds:** violet steps, always labelled.

**Reorder:**
- desktop: pointer drag on cards and a row handle, plus Alt + arrow keys;
- phone: the existing "Ubah urutan" mode with move buttons, for rows and for cards within a row.

Every reorder PUTs the layout. A failure restores the previous order and says so.

### D8. Restructuring without rule changes

The owner moves "Gopay" under Bank Jago and "RDN BCA" under BCA with "Pindahkan ke rekening". The rules already cover both:
- aliases and links are per account;
- `institution_parent` resolves "gopay" to a unique pocket match;
- the BCA RDN exclusion is by name;
- BCA already has pockets and a default.

Pairing between GoPay-app and Jago-app legs then shares the Jago root. A reference, or a remote account with owner identity, still decides.

The change adds no new rule. It adds regression tests for the owner's flows with that structure:
- BCA → GoPay top-up (link skip);
- GoPay Tabungan → Main (complete movement);
- Main → GoPay Tabungan;
- Jago → BCA;
- an RDN deposit.

With the owner's go-ahead at verification time, the 2026-11-05 Sesi uji export is also replayed with Gopay moved under Jago. If a test shows a regression, the finding is surfaced and the plan is revised before any rule changes.

## Open decisions (with defaults)

- **OD-A:** the savings change can be negative, and expenses paid from Tabungan reduce it (D4). *Default: yes.*
- **OD-B:** the role reclassifies past moves at read time (D3). *Default: yes.* Past cycles' Ditabung changes for pockets the owner marks Tabungan.
- **OD-C:** the grouping choice (Per kegunaan or Per rekening) is remembered per browser, and the order of rows and cards is saved to the account (D5, D7). *Default: yes.*

## Data, API and failure behaviour

- **Rollback:** V51 is additive. An older release ignores `role` and `account_layouts`. Its figures return to the old rules (RDN liquid, clamp), and no data is lost.
- **Failure:** the layout PUT is idempotent. On failure the page keeps the previous order and shows "Urutan gagal disimpan".
- **Validation:** role validation errors use the 422 `{code}` shape.

## Risks / Trade-offs

- **[Past monthly figures shift when roles are set (OD-B, D4)].** This is intended, and the owner sees it once. The verification record compares the owner's cycle figures before and after on a local copy.
- **[Pairing behaviour after GoPay moves under Jago].** It is covered by regression tests and the replay (D8). Any regression is surfaced before rule changes.
- **[A large page rewrite].** The existing detail modal, dialogs and reorder logic are reused. Tests are ported from `page.test.tsx`, with a browser check at 1440 px and 390 px, light and dark.

## Implementation notes (2026-11-05)

- **Role trigger:** V51 adds the trigger `accounts_role_follows_type`, on top of the API check. It keeps a credit account `utang` and an investment product `investasi`, and stops a cash account from holding either of those roles. Notification code creates broker stock positions without a role, and the trigger also covers an older release after a rollback.
- **Type change on edit:** an edit without a role keeps the current role while it still fits the new type; otherwise the role resets to the type's default.
- **Savings at read time:** a movement that touches an investment product counts as saving only if it is an investment top-up, or a row stored as `saving` before. Beli/Jual trades stay excluded.
  - A purchase from a savings account uses its gross amount.
  - A saving-pillar purchase from savings counts in both `pilar_tabungan` and `belanja`, so its net effect is zero.
- **Cycle summary:** `tabungan { total, masuk, diambil, belanja, pilar_tabungan }` is added, with `total = masuk − diambil − belanja + pilar_tabungan`. It equals the saving line of Keluar.
- **Default layout order:** tree order. Top-level accounts sort by `display_order`, `created_at`, then `name`, each followed by its pockets.
- **Tagihan:** active obligations show as fixed "Tagihan" cards. Under Per kegunaan they sit after the credit cards in Utang; under Per rekening they form a last "Tagihan" row. The cards are not in the saved layout and open Target & Tagihan, so the debt rows add up to the summary's Utang.
- **Rekenings with pockets:** they have no card of their own unless their own balance is not zero. Under Per rekening the row title opens the rekening, and a pocket's detail offers "Buka {rekening}".
- **Label change:** "Belanja dari tabungan" became **"Pengeluaran dari tabungan"**. The copy register forbids "Belanja" as a spending figure label.
- **Beranda:** the asset caption now adds `dana_investasi`. The KPI shows the savings change unsigned, with "Tabungan bertambah/berkurang Rp X" as its caption. A decrease takes no width in the Uang siklus ini bar.
