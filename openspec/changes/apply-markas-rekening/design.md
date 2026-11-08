## Context

Rekening & Kantong (`/accounts`) on `main` `0308b7b`, inside the phase 2 shell (frame 1200 px, title block, `PageActions`):
- `app/accounts/page.tsx` renders `AccountsView` (`components/accounts/AccountsView.tsx`, 290 lines). The title and subtitle come from the untranslated registry (`untranslatedPages.ts:16`), inside `lang="id"` with the notice line in an English interface.
- "Phone" is `useWidthAtMost(rootRef, 767)`, the width of the page root, not the viewport (the cause phase 5 found on Transaksi). The open account is `?akun=<id>`.
- `PageActions`: "Perbarui harga" (hidden on a phone, where "…" carries it), "Tambah rekening", and "…" with "Rencana", which opens `RecurringRulesModal`.
- A sync notice, an "Urutan gagal disimpan" alert and the action error sit above a custom `role=tablist` "Kartu · Grafik" (React state, not kept) and a `Segmented` "Per kegunaan · Per rekening" (localStorage `rekening-grouping`).
- `WalletSummary` (`panel summary`):
  - "Kekayaan bersih" as `ExplainableFigure` (the dotted popover, opening `ExplanationDialog` with `lines` relabelled by `lineLabel`);
  - a five-item `dl`: Saldo tersedia (`liquid`), Dana investasi, Investasi, Untung/Rugi (`GainChip` over all products) and Utang (`total_liabilities`, credit plus obligations);
  - a share bar with a % legend;
  - on Kartu, `MiniRing` (Per kegunaan, segments jump to their row) or `RekeningComposition` (Per rekening).
- **Kartu:**
  - an always-visible desktop drag hint;
  - on a phone, an "Ubah urutan" toggle that swaps the rails for `ReorderPanel`;
  - `WalletRails`, one rail per section, with a `RailHead` (drag handle, swatch or logo, title, count, `ReconciliationFlag`, `GainChip`, total) and a `RailExtra` (Investasi per jenis under Per kegunaan, a stacked composition bar under Per rekening);
  - then a horizontal strip of `WalletCard`s: a coloured top edge, use tag, provider logo, name, extra line, amount and `GainChip`; the credit limit meter; notes ("Melewati limit" pill, `ReconciliationFlag`, "N pindah saldo belum lengkap").
  - Obligations become `BillCard`s inside Utang (Per kegunaan) or a fixed last "Tagihan" row (Per rekening), so Utang adds up to `total_liabilities`.
- **Grafik:** `WalletChart`, a two-ring SVG (uses inside as `role=button` segments, kantong and products outside), a `ws-select-row` legend, and a focus list of the chosen use grouped by kind (Investasi) or by rekening, with gains and size bars.
- **Dialogs:** every dialog uses the legacy `Modal`:
  - `AccountDetail` (three fact tiles, facts, "Juga dilaporkan", pockets or products, "Transaksi terakhir" with Beranda's coral `RowAmount`, and a row of up to eight action buttons);
  - the add and edit account form (1636-line `AccountDialogs.tsx`) and the add pocket form;
  - Perbarui nilai and Sesuaikan Saldo;
  - `BalanceCheckDialog` (Cocokkan saldo);
  - `InvestmentTradeModal` (Beli/Jual), `InvestmentTopupModal` (Top up) and `AmountTrackingSwitchModal`;
  - `RecurringRulesModal`.
- Every string is hard-coded Indonesian, and money is formatted by `bal` from `useAppCtx()`. None of the phase 3 shared pieces (`HeroFigure`, `SplitBar`, `ListRow`, `MaskedMoney`) is used.

Data, unchanged by this phase:
- reads:
  - `GET /accounts` (the `AccountItem` fields, including `role`, `kind`, `color`, `capital_gain`, `capital_gain_pct`, `cost_basis`, `group_capital_gain`, `group_cost_basis`, `reconciliation_required`, `credit_limit`, `payment_day`, `credit_owed`, `over_limit`, and after `add-credit-spending-flow` `statement_day`, `limit_left` and `next_due`);
  - `GET /dashboard/net-worth` (`net_worth`, `total_assets`, `total_liabilities`, `liquid`, `dana_investasi`, `investment`, `credit_owed`, `reconciliation_pending`, `lines`, `obligations`);
  - `GET /review/queue` (`awaiting[].account_id`);
  - `GET` and `PUT /accounts/layout?grouping=use|rekening`;
  - `GET /transactions?account_id=&limit=4` and `GET /ingest/account-links?account_id=`;
  - after `add-credit-spending-flow`, `GET /api/credit/accounts/{id}`;
- writes: `POST /accounts/sync-prices`, `POST /accounts`, `PATCH /accounts/{id}`, `DELETE /accounts/{id}`, `POST /accounts/{id}/valuation`, `/reconcile`, `/balance-check`, `/balance-matches`, `/amount-tracking`, `POST /transactions` (trades), `POST`, `PATCH` and `DELETE /investment-topups`, and `DELETE /ingest/account-links/{id}`.

The prototype (`option-6/js/pages2.js` `PAGES.rekening`, `accTile`, `prodTile`, `creditTile`; `js/dialogs.js` `rekening`, `produk`, `beli`, `cocokkan`, `rekening-tambah`, `rincian:netWorth` and the `rekening`, `akun-*` and `produk-*` menus; `styles.css` §Rekening, Phone):
- a title block with two secondary buttons and "…";
- a hero with "Rincian", "dari aset", a split bar and a % legend;
- `Kartu · Grafik` and `Per kegunaan · Per rekening`;
- groups with 2 px rails and flat tiles;
- Grafik as horizontal bars;
- a phone wallet;
- centered dialogs: detail 560, Beli 520, Cocokkan 560, Tambah rekening 560.

Inputs this phase builds on:
- **Phase 1:** tokens, type roles, the straight selection bar (`ws-select-row`) and group rail (`ws-group-rail`) guarded by `selectionMarks.test.ts`, buttons, fields, segmented control, menu, dialog and swatch focus.
- **Phase 2:** the title block, `PageActions` with "…", `ShellDialog`, the frame 1200, the bell.
- **Phase 3:** `HeroFigure`, `FigureBlock`, `SplitBar`, `SectionHeader`, `ListRow`, `MaskedMoney`, `ExplanationDialog`, and the skeleton and error rules.
- **Phase 4 (`apply-markas-tinjau`):** `ListRow`'s `rowKey` and `current`.
- **Phase 5 (`apply-markas-transaksi`):**
  - the shared `RowAmount` in `components/transactions/rows.tsx`, whose expense is red (`--out-fg`) and which its D22 names for the Rekening detail;
  - the `ShellDialog` conventions for a detail with a footer and a quiet left action;
  - `.dl` and `.box`;
  - `InvestmentTopupModal` handed to this phase.
- **`add-language-system`:** next-intl catalogues, `terms`, `common`, `lib/format.ts`, `translatedRoutes.ts`, the copy register per language, the both-language gate and the en-versus-id figure check.
- **`localize-server-text`:** every error response is `{code, params, message}`.
- **`add-credit-spending-flow` (H8):** the credit fields and `GET /api/credit/accounts/{id}`.

Owner decisions that bind this page:
- the accounts-by-use choices of 2026-11-05: display 2 (Dompet) as the default, the composition ring as a supporting view, the draggable order kept ("I organize the display based on my usual usage"), and capital gain everywhere;
- the request to take Tagihan off Rekening (2026-11-05);
- straight bars and thin rails (headquarter);
- KPI cards, not text walls; whole-row click; centered modals; neutral firm labels; expense amounts red; no unused or redundant components; charts follow the dataviz rules.

Constraints:
- No money logic or API change. Every figure is a server field, or today's client sum of server balances (`assetSum`, `netOf`, `gainOf` over `buildHoldings`, which counts a parent's own balance once and its pockets once), formatted by `lib/format.ts`.
- The layout API, the saved order per grouping, the restore on a failed save, and every request keep their behaviour and tests.
- The held branch `fix/selection-bars-and-tinjau` is not merged; its Rekening parts are folded in here (D19).

## Goals / Non-Goals

**Goals:**
- Rekening & Kantong and its dialogs match the prototype at 1440, 1024 and 390 px in both themes and both languages, measured against `styles.css`. The only exceptions are the recorded deviations.
- Obligations leave the page; Utang is credit only, with one link to Target & Tagihan.
- The credit tile, the credit detail and the bill days of H8.
- Capital gain on every product, kind, platform and the Investasi group, with "Modal belum diisi" instead of a partial sum.
- The owner's order survives, reachable by pointer, keyboard and phone.
- The page's copy lives in the `accounts` namespace in English and Indonesian, and `/accounts` leaves the untranslated set.
- Nothing obsolete remains: the summary panel, the second ring, the bill cards, `ReorderPanel`, `GainChip`, the coloured edges and the Rekening-only CSS.

**Non-Goals:**
- Backend, API, data or schema changes; the companion.
- Changing net worth, the role rules, the gain rule or Ketahanan Dana.
- The Target & Tagihan page and the Rencana dialog body (phase 9), Catat (phase 11).
- Touch drag on phones.

## Decisions

### D1. Page structure and breakpoints

One reading column in the 1200 px frame, with no context panel (the prototype has none on Rekening):
- the title block;
- the hero (`.hero`);
- the controls row (`.toolbar`, margin 32 0 24);
- the reorder bar, while in Atur urutan;
- the body: Kartu groups or Grafik sections.

Breakpoints are viewport media queries, which replace `useWidthAtMost(rootRef, 767)`:
- ≥ 768: tiles in `repeat(auto-fill, minmax(240px, 1fr))`, gap 16, so four tiles a row at 1440 (1104 px) and three at 1024 with the sidebar;
- < 768: the phone composition (D13).

Rejected alternatives:
- Keeping the container measure: it disagrees with the shell's viewport breakpoints. Phase 5 found it gave a laptop the phone layout.
- A context panel with the summary: the prototype has none, and the summary figures become group totals (D3).

### D2. Title block and "…" menu

- **Title:** `nav.accounts` ("Rekening & Kantong" / "Accounts & Pockets"); subtitle `accounts.page.subtitle`.
- **Visible actions** (`PageActions`): "Perbarui harga" (secondary, refresh icon, `accounts.page.updatePrices`; "Memperbarui harga…" while pending; hidden below 768) and "Tambah rekening" (secondary, plus icon).
- **"…":**
  - "Perbarui harga" (phone only, as today);
  - "Atur urutan" (grip icon; "Selesai mengatur urutan" while on), which toggles D12;
  - "Rencana" (calendar icon), which opens the Rencana dialog in place, as today.

Rejected alternatives:
- The prototype's "Rencana di Target & Tagihan" as a link: it takes the owner away from the page, and until phase 9 `/goals` cannot open the dialog from a link (deviation 9).
- "Atur urutan" as a visible button: ≤ 2 visible actions.

### D3. Hero "Kekayaan bersih" — flagged: presentation of figures

The shared `HeroFigure`, without a colour mark:
- the label row "Kekayaan bersih" (`terms.netWorth`) and the quiet small "Rincian" (h 28, padding 0 8), the page's only Rincian control;
- the figure `net_worth`, Manrope 44/600 tabular (32 below 360 px);
- the line "dari aset {total_assets}" (`accounts.hero.fromAssets`), 14/20 `--muted`;
- the shared `SplitBar`: 8 px, 2 px gaps, square segments for Uang harian, Tabungan, Dana investasi and Investasi, in the use colours (`--use-*`, mapped to the prototype's `--k-none`, `--k-saving`, `--m-transfer` and `--m-invest` by phase 1). Each segment's amount is today's use total (`assetSum` of the use's holdings). `role="img"` with the percentages in its label;
- the legend `.chart-foot` (gap 24, margin-top 12): a square swatch and "Uang harian 20%" in 12/16 `--muted`. The percentages are today's shares (`Math.round(100 × use / total_assets)`). A use at 0 is left out.

"Rincian" opens the shared 480 px `ExplanationDialog`:
- the title "Kekayaan bersih" and the meta "Rincian hitungan";
- the rule (`accounts.rincian.rule`): "Dana likuid, Dana investasi dan Investasi dikurangi utang, tagihan dan saldo minus yang perlu dicocokkan. Saldo kantong dihitung sekali.";
- one row per server line:
  - `liquid` → `terms.availableCash` ("Dana likuid");
  - `dana_investasi` → `terms.investmentCash`;
  - `investment` → `terms.investments`;
  - `reconciliation_pending` → `terms.negativeBalanceToMatch`;
  - `debt:*` → "Tagihan · {name}";
  - `credit:*` → "Utang · {name}";
- the total row "Kekayaan bersih" = `net_worth`;
- the footer link "Buka Target & Tagihan" when an obligation line exists, which replaces the prototype's self-link "Buka Rekening & Kantong".

On failure of `/dashboard/net-worth`, the hero is the SPEC error box "Kekayaan bersih belum dapat dihitung." with "Coba lagi"; no Rp 0 and no browser sum. The groups still render from `/accounts`.

Where the five summary figures go (every figure once, audit principle 4):
- **Saldo tersedia:** the Rincian line "Dana likuid", under the glossary term (`ui-copy-register` "One Term per Concept"; Beranda already says "Dana likuid").
- **Dana investasi and Investasi:** their group totals.
- **Untung/Rugi:** the Investasi group header.
- **Utang:** the Utang group total (credit), the Tagihan row (obligations, D5) and the Rincian (both).

Rejected alternatives:
- "dari aset … · utang …" in the line: Utang would appear twice. The group and the Tagihan row already carry it.
- Keeping the five-figure `dl` as supporting figures: the prototype has none, and four of the five repeat group totals.
- The dotted `ExplainableFigure`: SPEC's one quiet "Rincian".

### D4. Controls

`.toolbar` with `margin: 32px 0 24px`:
- **Left:** the segmented control (`.seg`, `role="group"`, `aria-label` `accounts.controls.view`) "Kartu · Grafik" (`aria-pressed` buttons). The choice is kept in the URL as `?tampilan=grafik` through `replaceState`, so a reload and a shared link keep it, as in the prototype. Kartu is the default and is not written.
- **Right, on Kartu only:** "Per kegunaan · Per rekening" (`aria-label` `accounts.controls.grouping`). It is kept per browser in `rekening-grouping` as today. Blocked storage gives Per kegunaan.

Rejected alternatives:
- Today's `role=tablist` with arrow keys: SPEC segments are buttons with `aria-pressed`, and Tab reaches each.
- Keeping the view in React state: a reload lost Grafik.
- Moving the grouping into the URL: the spec requires "remembered per browser", and other devices keep their own.

### D5. Groups — flagged: owner-directed scope (Utang) and earlier owner choice (Per rekening composition)

Every group is `section.grp` (`aria-labelledby`). Each has:
- a 2 px straight rail `::before` in `--rail`, top and bottom 4, no radius;
- padding-left 16; groups 48 apart;
- a header `.grp-h` (min-h 32, baseline, gap 12):
  - `h2.t-label` in `--ink`;
  - `.count` 12/16 `--muted`;
  - an optional `.gain`;
  - `.total` Inter 14/600 tabular at the right.

The tiles follow (`.tiles`, margin-top 8). The order of groups and of tiles inside each comes from the saved layout, as today (`applyLayout`). Empty groups are hidden and keep their place in the saved order.

**Per kegunaan:**

| Group | Rail | Count | Extra | Total | Tiles |
| --- | --- | --- | --- | --- | --- |
| Uang harian | `--use-harian` | "8 kantong" | — | `assetSum` | leaf cash holdings (`buildHoldings`), a parent's own balance as "saldo di luar kantong" |
| Tabungan | `--use-tabungan` | "2 kantong" | — | `assetSum` | as above |
| Dana investasi | `--use-dana` | "1 rekening" | the note "belum dibelikan" in the count | `assetSum` | as above |
| Investasi | `--use-investasi` | "5 produk" | gain over all products (`gainOf`) | `assetSum` | per-kind lines, then product tiles |
| Utang | `--out-solid` | "1 kredit" | — | `−credit_owed` in `--out-fg` | credit tiles (D7), then the Tagihan row |

- **Investasi per-kind lines** (prototype): a plain list (`.row.is-plain`, min-h 40, padding 0) per kind with products: Reksa dana, Saham, Emas, Lainnya. Each shows its label, the meta "2 produk", `GainText` and the total. The colours are the kind swatches (`--kind-*`) as an 8 px square before the label; the text stays in text tokens (held branch D3).
- **Utang without Tagihan (owner request, plan decision 2 B):**
  - The group lists credit accounts only.
  - When `total_liabilities − credit_owed > 0` (the active obligations, already subtracted by the server), the group ends with one whole-row link (`ListRow`, min-h 56, chevron) to `/goals`: title "Tagihan di Target & Tagihan" (`accounts.debt.billsLink`), meta "{n} tagihan" (`obligations.length`), amount `−Rp …` in `--out-fg`.
  - The group total is `−credit_owed`, so the credit tiles add up to it. The row's amount and the group total add up to `total_liabilities`, which the Rincian shows line by line.
  - When there is no credit account and no obligation, the group is hidden. When there is no credit account but obligations exist, the group shows only the Tagihan row, with the count "0 kredit · {n} tagihan".
- **Per rekening:**
  - one group per top-level account (`.parent-block`), its rail in the account's display colour (`displayColours`);
  - the header count per prototype: "{n} kantong · Uang harian Rp … · Tabungan Rp …", the per-use subtotals of its pockets in role order, or its use label for an account without pockets. A platform's group gain (`group_capital_gain`, else "Modal belum lengkap") sits in `.gain`, and the total is the account's balance;
  - the tiles: its credit tile, else its products, else its pockets, else itself;
  - the "Tagihan" link row follows the last group, under the same rule.
- **`ReconciliationFlag` in headers:** "{n} kantong perlu dicocokkan" (Per rekening) stays as a quiet `.count` suffix in `--warning` text with the alert icon, as a button when `Sesuaikan saldo` is allowed (`account-reconciliation`).

Rejected alternatives:
- The prototype's obligation tiles under Utang and the "Utang lain" row: the owner asked to remove them (2026-11-05).
- A summary "Tagihan" figure (held branch D8): there is no summary on the page any more. The figure belongs to the group whose total it completes.
- The stacked composition bar per rekening (`organize-accounts-by-use`, "per-rekening composition is added"): it repeats the subtotals beside the tiles. The prototype says the same in words, so the composition stays visible. Flagged because it changes an earlier owner choice. Fallback: keep the bar under the header.

### D6. Tiles — flagged: presentation of figures (gain format)

`Tile` (`components/accounts/Tile.tsx`) is a whole `button.tile`:
- min-h 112, padding 16, gap 12, radius 12, 1 px `--line`, `--s0`;
- hover `--s1` with `--line-strong`; pressed `--s2`;
- neutral focus outline, offset 2;
- it opens the detail (D8) and keeps `?akun=<id>`.

Contents:
- **`.tl-top`:** `InstitutionMark` (24 × 24, radius 6, 12/500 initials).
  - The background is the account's display colour (`displayColours`, owner-chosen `color` with the fallback list).
  - The initials are white when white reaches 4.5:1 on that colour, ink otherwise. This is computed with the token test's contrast function and pinned by a unit test.
  - A pocket uses its parent's colour.
- **`.names`:** the name 14/400 and one meta line 12/400 `--muted`:
  - a pocket: "Kantong di {parent}" plus " · bawaan" when it is the default pocket;
  - a top-level account: the type label (Bank, E-wallet, Uang tunai, Rekening dana investasi);
  - a product: "{platform} · {holdingLine}" ("12 lot", "6,25 gram", "dicatat per nominal");
  - a parent's own balance: "saldo di luar kantong";
  - under Per rekening the meta also names the use ("Tabungan · Kantong di Jago").
- **`.tl-bottom`:** the balance in Inter 20/600 tabular, and for a product `GainText`.
- **Reorder mode:** a 16 px grip at the right of `.tl-top` (D12).
- **One optional note line** (12/16, at most one; the order is the priority), below the bottom row:
  1. "Melewati limit" in `--expense` text with the alert icon (credit);
  2. "Perlu dicocokkan" in `--warning` text with the alert icon (`reconciliation_required`). It is a button opening Sesuaikan saldo when allowed. The tile then becomes a `div` with two buttons (open, flag), so no button is nested;
  3. "{n} pindah saldo belum lengkap" in `--muted`.

**`GainText`** (`components/workspace/figures.tsx`), the prototype's `M.gain`:
- "▲ +Rp 396.000 · +8,0%" or "▼ −Rp 164.000 · −10,7%";
- 12/16, weight 500, `--income` or `--expense` text, no fill;
- the sign and the triangle, plus a hidden "untung" or "rugi";
- the percent with one decimal (`lib/format.ts` `percent`);
- the amount from `gain.amount`, the percent from `gain.amount / gain.cost` (today's `gainOf`), or the product's server `capital_gain_pct` rounded to one decimal;
- "Modal belum diisi" in `--muted` when the set has an unknown cost; nothing when the set has no product.

It replaces `GainChip`, which showed no sign, used a pill fill and put the percent in brackets.

Removed:
- the coloured top edge (`--card-accent`, `.cardDebt::before`);
- the use tag pills and provider logo line;
- the "Melewati limit" pill;
- the horizontal card strip, replaced by the wrapping grid.

Rejected alternatives:
- Keeping the coloured edge: the headquarter rule and SPEC §5 forbid a coloured edge on a rounded element.
- Several note lines: one badge per row (audit principle 8). The detail lists every state.
- Two decimals in the percent (server `capital_gain_pct`): the prototype uses one. The value is the same rounded figure, and the change is listed as an intended presentation change in the parity check.

### D7. Credit tile (H8)

`creditFacts(account)` (`components/accounts/credit.ts`) reads the `GET /accounts` row: `credit_owed`, `credit_limit`, `limit_left`, `over_limit`, `next_due {due_date, amount, left_to_pay, status}`, `payment_day` and the overpayment (`balance > 0`, "Lebih bayar"). The tile shows:
- **`.tl-top`:** the mark, the name, and the meta, which is one of:
  - "Sisa limit Rp 2.107.000" (`terms.limitLeft`);
  - "Melewati limit" in `--expense` text;
  - "Tanpa limit".
- **A 4 px `ProgressBar`** of owed over limit in `--m-expense`, only with a limit. It is `aria-hidden`; the value is in text.
- **`.tl-bottom`:**
  - the owed amount "−Rp 893.000" in `--out-fg`, Inter 20/600, with the hidden label "Utang"; or "Lebih bayar Rp 12.000" in `--income` text;
  - the next bill "jatuh tempo 11 Des · Rp 434.000" (`next_due.due_date`, `next_due.left_to_pay`), with "hari ini" or "lewat jatuh tempo" in `--warning` text when `status` is `due_today` or `overdue`;
  - "Tanggal jatuh tempo belum diisi" when the account has no `payment_day`;
  - nothing when there is no open bill.

Rejected alternatives:
- The prototype's "terpakai dari Rp 3.000.000" and "bayar tiap tgl 5": H8 asks for the limit left and the next bill, which answer "can I still use it" and "when do I pay".
- A coloured amount for the limit left: the colour stays on the owed amount and the over-limit word.

### D8. Account detail dialog — flagged: scope addition ("Bayar tagihan")

`AccountDetailDialog` (`components/accounts/AccountDetailDialog.tsx`) is a `ShellDialog` at 560 px (sheet below 768).
- It loads its own data through shared query keys (`queryKeys.accounts` as today, plus keys for the account's recent transactions and its credit overview; the new names are indicative), so phase 9 can open it from Target & Tagihan without the Rekening page.
- On `/accounts` it is driven by `?akun=<id>` as today. A stale id is cleared.
- Focus returns to the tile or row that opened it.

**Header:** the mark and the name (Manrope 20/600), with the meta "Kantong di {parent}" or the type label ("Rekening bank", "E-wallet", "Uang tunai", "Rekening dana investasi", "Akun investasi", "Kredit").

**Body** (blocks 24 apart):
- **Figure:** a `t-meta` label and a 32 px `t-fig-l` (the only figure ≥ 33 px is the page hero):
  - "Saldo" for an account or pocket;
  - "Nilai sekarang" for a product or platform, with `GainText`;
  - "Utang" for credit, with the 8 px `ProgressBar` of owed over limit.
- **`.dl`** (label column 160):
  - **Account or pocket:** Kegunaan, Induk, Kantong utama, "Perlu dicocokkan" when flagged (as a button when Sesuaikan saldo is allowed).
  - **Product:** Modal (or "Belum diisi"), Untung/rugi (`GainText` from `capital_gain` and `capital_gain_pct`), Cara hitung (with "· nilai estimasi"), Jumlah, Harga terakhir, Modal beli rata-rata, Kode, Rekening dana.
  - **Platform:** Produk count, group gain (`group_capital_gain` and `group_cost_basis`, else "Modal belum lengkap").
  - **Credit (H8):**
    - Limit, or "Tanpa limit";
    - Sisa limit, or "Melewati limit";
    - Tagihan berikut: "Rp 434.000 · jatuh tempo hari ini";
    - Tagihan terbit tiap tgl (`statement_day`);
    - Jatuh tempo tiap tgl (`payment_day`);
    - Lebih bayar, when there is one;
    - for the current cycle (`totals`): "Pengeluaran pakai kredit" (`spent_on_credit`), "Tagihan kredit dibayar" (`paid_to_credit`), and "Dilunasi tepat waktu" "{items_settled_on_time} dari {items_settled}".
- **Credit bills (H8)**, `CreditBills.tsx`, under the section header "Tagihan" with the aside "{n} tagihan":
  - one row per bill of `GET /api/credit/accounts/{id}`: title "Jatuh tempo 11 Des"; meta the status word (`terms.billStatus.*`: Terbuka, Akan datang, Jatuh tempo hari ini, Lewat jatuh tempo, Lunas, Lunas terlambat) · "dibayar Rp …"; amount `amount` with "sisa Rp …" (`left_to_pay`) below;
  - each row is a disclosure button (`aria-expanded`, chevron) that opens its `items[]` in place as 12/16 rows indented 16: the item's purchase name or kind ("Biaya kredit", "Tarik tunai", "Saldo awal"), "cicilan 2 dari 3", the amount, then for a settled item "lunas 9 Des · 35 hari · tepat waktu" or "· terlambat", or "sisa Rp …" while open;
  - "Lihat di Transaksi" links to `/ledger?account={id}`;
  - with `due_dates_unavailable`: "Isi tanggal jatuh tempo agar tagihan bisa dihitung." with "Ubah kredit";
  - when the request fails: "Tagihan belum bisa dimuat." with "Coba lagi"; the rest of the dialog stays.
- **"Kantong"** (a parent) or **"Produk"** (a platform): `ListRow`s with the name, the meta (use, "bawaan", or `GainText`) and the amount. Each opens that account's detail in the same dialog, and Back returns.
- **"Juga dilaporkan"** (`AccountLinks`), with "Lepaskan" confirming inline as today.
- **"Transaksi terakhir"** (4 rows): `ListRow` compact with phase 5's `RowAmount` (expense red, owner rule), date and time in the meta, each opening the shared transaction dialog. Empty: "Belum ada transaksi." Error: "Transaksi terakhir belum bisa dimuat."

**Footer** (the audit's "one primary, one secondary, rest in …"): the quiet "Lainnya" menu button at the left, then the secondary and the primary at the right.

| Kind | Primary | Secondary | Lainnya |
| --- | --- | --- | --- |
| Liquid account or pocket (`canCheckBalance`) | Cocokkan saldo | Ubah kantong / Ubah rekening | Sesuaikan saldo (when allowed), Tambah kantong (top-level, non-credit), Buka {induk}, Arsipkan |
| Parent with pockets | Tambah kantong | Ubah rekening | Cocokkan saldo (when allowed), Arsipkan |
| Unit-tracked product | Beli | Jual | Perbarui nilai, Sesuaikan saldo, Ubah ke pelacakan nominal (eligible reksa dana), Ubah kantong, Buka {induk}, Arsipkan |
| Amount-tracked product | Top up | Perbarui nilai | Ubah kantong, Buka {induk}, Arsipkan |
| Platform | Tambah kantong | Ubah rekening | Arsipkan |
| Credit | Bayar tagihan | Ubah kredit | Lihat di Transaksi, Arsipkan |

- "Arsipkan" asks first, in the footer, with today's sentence.
- **"Bayar tagihan" (flagged, scope addition):** it closes the dialog and calls `openMovementCapture({ targetAccountId, amount: next_due.left_to_pay })`. This is a new workspace-context function that extends today's `handleOpenMovement` with an amount, and opens today's Perpindahan form (`QuickCaptureModal` movement mode) with the credit account as "Ke" and the amount filled in. The owner chooses the source and saves; nothing is posted without that. Phase 9 reuses the function for due credit bills; phase 11 rebuilds the form.
- Cocokkan saldo, Sesuaikan saldo and Tambah kantong are never offered for credit (`credit-accounts` "Credit Accounts in Pickers").

Rejected alternatives:
- Today's row of up to eight equal buttons: the audit counted six equal investment buttons.
- Bills as a separate dialog: H8 places them in the credit detail.
- Opening Catat without the amount: H8 and the prototype's "Bayar tagihan" exist to pay the next bill.

### D9. Account form and pocket form (H8 days)

`ShellDialog` at 560, the SPEC form (`.form-stack`, `.form-grid` two columns ≥ 768). Fields stay what they are today, restyled:
- **Nama:** `#account-name`, autofocus, placeholder "Misalnya: Jago · Belanja Bulanan".
- **Jenis:** a radiogroup segmented control (Bank, E-wallet, Tunai, Investasi, Kredit), replacing the long-option select. The options map to today's `type` values. Changing it re-renders the dependent fields as today.
- **Kegunaan** (`RoleField`, a select, with the role rules unchanged) and **Induk** ("Pindahkan ke rekening" on edit).
- **Kantong bawaan:** "Kantong bawaan untuk notifikasi", for a parent.
- **Investasi:** "Model akun investasi" (segmented "Beberapa produk · Satu produk"), "Sumber dana bawaan", the instrument type, the ticker search, units and price.
- **Kredit (H8):**
  - "Utang saat ini (IDR)", "Limit (opsional)";
  - "Tagihan terbit tiap tgl (opsional)" (`statement_day`, 1–31);
  - "Jatuh tempo tiap tgl (opsional)" (`payment_day`, 1–31);
  - the hint under the two days: "Tagihan yang sudah terbit tetap memakai tanggal jatuh temponya.";
  - `statement_day` is sent on create and edit for credit only, as the API requires.
- **Warna:** the app's nine swatches as 28 × 28 squares, radius 6, in a radiogroup:
  - the selected swatch has a 1 px `--ink` outline, offset 2;
  - keyboard focus shows a 2 px `--focus` ring 4 px outside;
  - each is named "Warna {name}".
- **Saldo awal** for non-credit types.
- **Footer:** "Batal" and "Simpan" ("Tambah rekening" on create).

Validation errors stay under their fields, in 12 px `--expense` text with `aria-invalid` and `aria-describedby`. Server refusals render from `errors.<code>` (`credit_fields_not_allowed`, `account_role_type_mismatch`).

The add pocket form ("Tambah kantong di {parent}") uses the same parts.

Rejected alternatives:
- The prototype's six colours: the owner's stored colours must stay selectable.
- One "Tanggal bayar" field as in the prototype: H8 asks for both days.

### D10. Money dialogs

All move from `Modal` to `ShellDialog`. Requests, validation and previews are unchanged.
- **Beli / Jual** (`InvestmentTradeModal`, 520):
  - title "Beli", "Jual" or "Top up"; meta the product name;
  - a `.form-grid` of "Lot" or "Gram" and "Harga per lembar" or "Harga per gram" (with today's "Isi cepat: Pasar · Modal");
  - "Dari rekening" or "Masuk ke", the date and the note;
  - the `.box` "Perkiraan total" with today's position preview (units after, cash direction);
  - the over-sale error under the units field;
  - footer "Batal" and "Beli" or "Jual".
  - The direction toggle leaves the dialog: the product detail offers Beli and Jual separately, as the prototype does.
- **Top up** (`InvestmentTopupModal`, 520): "Nominal (IDR)" as the 56 px money input with the hint "Sama dengan top up terakhir ke produk ini." when prefilled, "Dari rekening", the date and the note; the quiet "Buat rencana bulanan (tidak mencatat)"; footer "Batal" and "Top up". Editing a top-up ("Ubah top up") keeps its delete.
- **Perbarui nilai** (520): "Nilai sekarang", "Modal" and the note, with the gain preview as `GainText`.
- **Sesuaikan saldo** (520): "Saldo sebenarnya (IDR)" and the note, with the difference preview "+Rp …" or "−Rp …" and its words ("uang masuk" or "uang keluar").
- **Ubah ke pelacakan nominal** (520): the carried-over value and cost and the sentence that it cannot be undone.
- **Cocokkan saldo** (`BalanceCheckDialog`, 560, prototype):
  - the field "Saldo di aplikasi bank (IDR)" beside "Tercatat di sini";
  - "Periksa";
  - a result:
    - "Saldo cocok" in a `.box` with "Saldo cocok" (marks the point, as today);
    - or "Hasil pemeriksaan" with the signed difference as `t-fig` and its words, the basis line, and "Kemungkinan penyebab" as plain rows: an icon, the title, the meta and today's links "Buka transaksi" or "Buka di Tinjau";
  - footer "Tutup" and, with a difference, "Sesuaikan saldo" (opens Sesuaikan saldo with the typed amount, as today), plus the quiet "Periksa lagi".

Rejected alternatives:
- Building a fee field into Beli: the trade request has no fee (deviation 8).
- Keeping `Modal`: two dialog systems with different widths, focus and phone behaviour.

### D11. Grafik — flagged: owner preference over the prototype and the dataviz rule

The view has two parts.

**1. "Per kegunaan":**
- A `SectionHeader`, then a two-column block (`200px minmax(0,1fr)`, gap 24; one column below 768 with the ring 160 px centered).
- **The ring:** one SVG ring of the four asset uses (`arcPath`, `segmentAngles`, the existing 2 px surface gap). Its centre reads "Aset" and `total_assets`. The ring is `aria-hidden`; the legend is its accessible form and its table.
- **The legend** (`role="group"`, "Pilih kegunaan"): one row per use, min-h 48, `ws-select-row`, square, 1 px `--line` between rows.
  - Each row shows an 8 px square swatch, the label, the amount (Inter 14/600) and "20% aset" (`--muted`).
  - It is a toggle button (`aria-pressed`). The chosen row carries the straight 2 px full-height bar on `--s2` (held branch D1).
  - A use at 0 is disabled.
  - The default is Investasi when it holds products, otherwise the largest use.
- The previous outer ring of kantong and products is removed; their sizes are the bars below.

**2. The chosen use's contents**, as `.hbar` rows (`200px minmax(0,1fr) 136px 136px`, gap 16, min-h 48; 8 px bar on an `--s2` track, square ends; the label 14/400; the amount Inter 14/600; the fourth column `GainText` or the share):
- **Investasi** (prototype):
  - "Investasi per jenis", with the overall `GainText` in the aside: one bar per kind in `--m-invest`, with its gain;
  - "Per produk": one bar per product, each a whole-row button opening its detail, with its gain.
  - Under Per rekening (the grouping last chosen on Kartu), the products group under each platform with a quiet uppercase label, the platform's colour as an 8 px square, the platform's total and gain, and a 2 px `ws-group-rail` in that colour (held branch D2, D3).
- **Another use:** "Isi {use}", one bar per kantong in the use colour with "{share}% aset", grouped by rekening with the same quiet labels and rails.

Bars scale to the largest value in their section. A negative value has no bar and shows its amount.

Below 768 the `.hbar` stacks: the label and the amount on one line and the bar under them; the fourth column is hidden for shares and kept for gains.

Why the ring stays:
- The owner kept it on 2026-11-05 ("keep both") and chose it as a supporting view.
- The dataviz rule prefers a segmented bar for part-to-whole, and the hero already has that bar. Grafik is the explorer view: there the ring is a selector, and every value is in the legend beside it, so no value is read from an angle alone.

Rejected alternatives:
- The prototype's per-use bars (fidelity and dataviz): they drop the owner's chosen view. This is the recorded fallback.
- Today's two rings: the outer ring of thin slices is unreadable (dataviz: no donut for comparing close values), and the bars show the same sizes.

### D12. Atur urutan — flagged: scope (interaction)

"…" › "Atur urutan" turns reorder mode on, on every width and in both groupings:
- **The SPEC selection bar** (`.sel-bar`, h 48, padding 0 12, radius 6, `--s1`, margin 0 0 24): the one line "Seret kartu dalam baris, atau seret ⋮⋮ untuk memindah baris. Dengan keyboard: Alt + panah. Urutan diingat." (on a phone: "Pakai tombol naik dan turun. Urutan langsung tersimpan.") and the primary "Selesai". The bar stays under the top bar while in the mode (sticky top 56).
- **Tiles:** a 1 px dashed `--line-strong` edge and a 16 px grip. A grabbed tile is solid `--line-strong` on `--s1`. In the mode a tile does not open its detail.
- **Group headers:** a 28 × 28 handle (`grip`) before the label, named "Pindah baris {title}. Alt dan panah atas atau bawah."
- **Pointer** (≥ 768): HTML5 drag and drop as today. Cards move within their row and rows by their handle. The drop target is a 2 px straight bar before the item (`dropBefore`), not a dashed outline.
- **Keyboard:** Alt + ArrowLeft/Right on a tile and Alt + ArrowUp/Down on a handle, with focus kept on the moved element and the status "{name} dipindah ke posisi N dari M." in the live region, as today.
- **Phone:** each tile and handle shows two 44 px move buttons ("Geser ke depan {name}", "Geser ke belakang {name}"; "Naikkan baris {name}", "Turunkan baris {name}"). Boundary moves are disabled.
- **Saving:** through `useWalletLayout` as today, per grouping. A failed save restores the order and announces "Urutan gagal disimpan" as a toast.
- **Leaving the mode:** "Selesai", Esc, or the menu item.

Rejected alternatives:
- Always-on dragging with the hint hidden (today's behaviour): a tile is a button that opens a dialog, so a drag and a click compete. The prototype moves both the drag and its hint into the mode (audit: "drag hint only in Atur urutan mode"). The fallback keeps dragging always on and shows the hint only in the mode.
- `ReorderPanel`'s separate phone list: the mode reorders in place, so the owner sees the real page.

### D13. Phone (below 768 px)

- **Order** (prototype): title block → hero (44 px; 32 below 360) → legend → controls (each segment scrolls inside its box) → body.
- **Uang harian:** a `.wallet` row (`flex`, gap 12, `overflow-x: auto`, `scroll-snap-type: x mandatory`, bleeding to the 16 px gutters). Tiles are 280 px wide, min-h 152, snapping at the start. The group label says "geser ke samping" to assistive technology. The grid copy (`.tiles.is-wallet`) is hidden.
- **Other groups:** one tile per row. Per-kind lines wrap their gain under the label.
- **Dialogs:** sheets with the grab bar. Footer buttons share the width, and "Lainnya" opens as a sheet menu.
- **"Perbarui harga":** in "…" (`phoneOnly`). "Tambah rekening" stays visible.
- **Targets:** 44 px for buttons, segments (40 h items inside a 44 px row), move buttons and menu items.
- **Width:** no horizontal page scroll at 390 and 320. The wallet scrolls inside its own row.

### D14. Loading, failure and empty states

- **Loading:** a hero skeleton (label 96 × 12, figure 240 × 32, line 160 × 12, bar 100 % × 8) and two groups of three tile skeletons (min-h 112, `--s2` bars, radius 6, no pulse), with `aria-busy="true"` on the body.
- **`/accounts` fails:** the SPEC error box (1 px `--line-strong`, radius 12, padding 16 24, alert icon 20) "Daftar rekening gagal dimuat." with "Coba lagi". The hero is not shown.
- **`/dashboard/net-worth` fails:** the hero error box only (D3).
- **Empty** (no active account): the SPEC empty state "Belum ada rekening" with "Tambahkan rekening bank, e-wallet atau tunai untuk mulai." and the secondary "Tambah rekening". There is no hero and no controls.
- **`/review/queue` fails:** the awaiting note is left out, silently, as today.

### D15. "Perbarui harga"

`POST /accounts/sync-prices` as today:
- the button reads "Memperbarui harga…" and is disabled while pending;
- on success, the toast "Harga diperbarui · {n} produk, per {time}" (`role=status`);
- on failure, the toast "Harga belum dapat diperbarui." with the server code's message.

The inline `styles.notice` and its 4 s timer are removed. When a product dialog is open, the price refresh leaves it open.

### D16. Language

- **Namespace `accounts`** in `messages/en.json` and `id.json`, with identical key trees. Sub-namespaces: `page`, `hero`, `rincian`, `controls`, `groups`, `tile`, `credit`, `debt`, `grafik`, `reorder`, `detail`, `actions`, `form`, `pocketForm`, `trade`, `topup`, `valuation`, `adjust`, `amountTracking`, `check`, `links`, `states`, `toast`, `announce`. Appendix A lists the keys with both languages.
- **New `terms`**, shared with phase 9:
  - `debt` ("Utang" / "Debt");
  - `limitLeft` ("Sisa limit" / "Limit left");
  - `overLimit` ("Melewati limit" / "Over limit");
  - `leftToPay` ("Sisa tagihan" / "Left to pay");
  - `everydayMoney` ("Uang harian" / "Everyday money");
  - `billStatus.open|upcoming|dueToday|overdue|paid|paidLate` ("Terbuka · Akan datang · Jatuh tempo hari ini · Lewat jatuh tempo · Lunas · Lunas terlambat" / "Open · Upcoming · Due today · Overdue · Paid · Paid late").
  Existing terms used: `netWorth`, `availableCash`, `investmentCash`, `investments`, `savings` (the Tabungan use), `bills`, `plans`, `negativeBalanceToMatch`, `breakdown`.
- **Use and kind labels:** `accounts.groups.*` for the use names (from `terms` where they exist) and `accounts.kinds.*` (Reksa dana, Saham, Emas, Lainnya / Mutual funds, Stocks, Gold, Other).
- **Indonesian:** today's text for every existing string, with these intended changes, listed in `verification.md`:
  - "Dana likuid" for "Saldo tersedia" in the Rincian (glossary);
  - "Modal belum diisi" kept, and "Modal belum lengkap" for a platform;
  - the form labels in sentence case without the bracketed long options ("Jenis: Bank · E-wallet · Tunai · Investasi · Kredit"; "Kantong bawaan untuk notifikasi");
  - "Perlu dicocokkan" for "Perlu rekonsiliasi" (neutral, non-technical label register; the button opens "Sesuaikan saldo");
  - "Tagihan di Target & Tagihan";
  - "Lainnya" in dialog footers;
  - "Atur urutan" and "Selesai";
  - the toast "Harga diperbarui · N produk, per 14.32".
  New strings follow the neutral register: no "kamu" or "Anda", no slang.
- **English:** plain and firm, US spelling, sentence case. Use names follow the glossary ("Everyday money", "Savings", "Investment cash", "Investments", "Debt"). "Expense", "Outflow" and "Ledger" never appear.
- **Whole ICU sentences:** counts use `plural` (English `one`/`other`, Indonesian `other`). Amounts, dates, times and percentages arrive formatted from `lib/format.ts`. The signs "−" and "+" and the triangles are added by `GainText`, the tile and `RowAmount`, never inside a message.
- **Server text:** errors render from `errors.<code>`. Account, pocket, product, platform, obligation and merchant names, notes and instrument symbols are user data: shown as stored, with no `lang` attribute.
- **Registry and guards:**
  - `/accounts` joins `TRANSLATED_ROUTES`, and its entry leaves `untranslatedPages.ts`;
  - the untranslated-page test example in `workspace.test.tsx` moves from `/accounts` to `/goals`, which phase 9 translates;
  - the copy register's translated roots gain `app/accounts`, `components/accounts` and the four `components/ui` money dialogs;
  - catalogue parity covers `accounts`.
- **Lengths**, measured in both languages at 1440, 1024, 390 and 320 px:
  - tile meta "Mutual fund in Bibit · recorded by amount" (ellipsis);
  - group counts "8 pockets · Everyday money Rp 11.680.520";
  - the `.hbar` 200 px label column ("Investment cash", "Mutual funds");
  - the reorder bar line (wraps on a phone);
  - the credit tile "Limit left Rp 2.107.000" and "due 11 Dec · Rp 434.000".

Rejected alternatives:
- Keeping "Saldo tersedia" beside Beranda's "Dana likuid": two terms for one concept.
- Leaving the money dialogs untranslated (they are shared with Transaksi): their only entry points are this page and the transaction dialog, which phase 5 translated.

### D17. Hidden balances

With "Sembunyikan saldo", every amount renders through `MaskedMoney` as "Rp ••••••" (or "$ ••••••") with the accessible name `common.hidden`. That covers:
- the hero, the Rincian lines, the legend amounts in Grafik, the group totals and the per-kind totals;
- the tiles, the gains, the credit limit left and the next bill;
- the Tagihan row;
- the detail's figure, facts, bills and items, pockets, products and recent transactions;
- the previews in the money dialogs.

Percentages stay visible: shares and gain percents reveal no amount. This is the prototype's rule; today the gain chip hid them, and the change is listed for the owner. Form fields keep what the owner types.

### D18. Shared components and CSS

**Reused as they are:** `ShellDialog`, `PageActions`, `HeroFigure`, `SplitBar`, `SectionHeader`, `ListRow`, `MaskedMoney`, `ExplanationDialog`, `Segmented`, `EmptyState`, `ErrorState`, `Skeleton`, phase 5's `RowAmount`, `.dl` and `.box`.

**New, each with a second named user:**
- `ProgressBar` in `components/workspace/figures.tsx` (`.prog`, h 4 or `is-thick` 8, `--s2` track, square fill, decorative). Users: the credit tile and detail here; phase 9's Ketahanan Dana bar, goal rows and debt rows.
- `GainText` in `figures.tsx`. Users: tiles, groups, Grafik and the detail. A later user is phase 6's investment lines, if any.
- `components/accounts/credit.ts` (`creditFacts`, the bill status labels and their tones) and `AccountDetailDialog` standalone. Users: here, and phase 9's Utang & kredit credit row.
- `openMovementCapture({ sourceAccountId?, targetAccountId?, amount? })` in the workspace context, backed by `AppLayout`'s movement defaults plus an amount. Users: "Bayar tagihan" here, and phase 9's due credit bills in Perlu tindakan and the bell.
- `Tile` and `InstitutionMark` in `components/accounts/` (users: groups and the wallet).
- `CreditBills.tsx`.

**Token names.** Colours in this design use the prototype's names. The code uses phase 1's mapping (`apply-markas-foundation` design D3): `--s0`/`--s1`/`--s2` → `--canvas`/`--surface`/`--surface-2`, `--ink` → `--text`, `--income` → `--in-fg`, `--expense` → `--out-fg`, `--warning` → `--warn-fg`, `--m-expense` → `--out-solid`, `--m-invest` → `--inv-solid`, and the use colours `--use-*`. The checklist compares computed values, so the names never block fidelity.

**CSS** from `styles.css` with the phase 1 token names: `.tiles`, `.tile`, `.tl-top`, `.names`, `.tl-bottom`, `.inst`, `.gain`, `.grp`, `.grp-h`, `.parent-block`, `.reorder`, `.hbar`, `.prog`, `.wallet`, `.tiles.is-wallet`, `.chart-foot` (if phase 3 has none), `.ring-legend`.

**Removed** (no user remains, each checked with `rg` in task 5.11):
- `WalletSummary.tsx` (`Figures`, `MiniRing`, `RekeningComposition`, `lineLabel`, moved into the hero's Rincian mapping);
- `ReorderPanel.tsx`, `GainChip.tsx`, `MoveButtons` (moved into the reorder mode);
- from `wallet.ts`: `Bill`, `BILLS_ROW`, `billsOf`, `billsIn`, and the bills parameter of `isShown`;
- from `WalletRails.tsx`: `BillCard`, `BillsRow`, `RailExtra`'s stacked composition bar, `countText`'s bill branch and the `cardDebt` styles;
- from `WalletChart.tsx`: the outer ring, `focusGroups` and the focus list;
- `AccountDetail.tsx`, replaced by `AccountDetailDialog.tsx`;
- `ExplainableFigure` on Rekening (kept where other pages still use it);
- the `useWidthAtMost(rootRef, 767)` call and `PHONE_WIDTH`;
- `wallet.module.css` rules: `.controls`, `.tabs`, `.panel*`, `.summary*`, `.kpis`, `.billsLink`, `.shareBar`, `.miniRing`, `.rail*`, `.cards`, `.card*`, `.useTag`, `.prov`, `.meter`, `.hint`, `.handle`, `.dragging`, `.dropBefore`, `.railDrop`, `.chart`, `.ring*`, `.focus*`, `.sizeBar`, `.kindTotals`, `.kindFigures`, and the file itself if nothing remains;
- `money.module.css` rules used only by Rekening: `detail*`, `drawer*` where Target & Tagihan no longer uses them, `check*`, `gain*`, `notice` and `alert`. Its Target & Tagihan rules stay for phase 9.

**Kept:** `Modal` (other pages), `useWidthAtMost` (other pages), `.ws-select-row` and `.ws-group-rail` (phase 1), `RecurringRulesModal` (phase 9 rebuilds it), `displayColours`, `useWalletLayout`, `rings.ts` (`arcPath`, `segmentAngles`).

### D19. The held branch `fix/selection-bars-and-tinjau` (`64909ee`)

Its diff against `18a4477` touches the accounts components, `globals.css`, `selectionMarks.test.ts`, the Tinjau files, `GoalDialogs.tsx` and the `refine-selection-and-tinjau-layout` OpenSpec change.

| Part | Here | How |
| --- | --- | --- |
| Tagihan off Rekening: `Bill`, `BILLS_ROW`, `billsOf`, `billsIn`, `BillCard`, `BillsRow` and the obligations mapping removed; `isShown` = "has cards" | **Taken** | D5 and D18, as in the branch |
| Utang = `credit_owed`; "Tagihan" = `total_liabilities − credit_owed`, shown only above 0, linking to `/goals` with the amount in its name | **Taken, moved** | The figure moves from the removed summary into the Utang group's last row (D5); the accessible name "Tagihan −Rp X di Target & Tagihan" is kept |
| Kekayaan bersih remains the server's figure, still subtracting the obligations | **Taken** | D3; the Rincian names each obligation |
| Grafik ring legend on the straight bar (`ws-select-row`, flat rows with 1 px lines, no inset shadow) | **Taken** | D11 legend |
| Grafik focus groups: quiet uppercase labels, the colour once as a dot, muted totals, a straight `ws-group-rail` in the kind, rekening or use colour | **Taken** | D11 bars grouped under quiet labels with rails |
| Investasi per jenis as quiet labels with totals and gains, divided by 1 px lines | **Taken, restyled** | D5 per-kind lines (`.row.is-plain`) |
| Phone focus items with the amount under the name | **Superseded** | `.hbar` stacking below 768 (D11) |
| `accounts-wallet-view` delta of the branch (Utang credit-only, Tagihan link, quiet labels and rails) | **Taken, rewritten** | spec delta "Utang Without Obligations", "Rekening Grafik" |
| Account tests: summary Utang and the Tagihan link, no bill cards, straight-bar class, axe | **Taken, adapted** | task 4.12 |
| `ws-section-label` (11.5 px, 650 weight) | **Superseded** | SPEC `t-label` 12/500 (phase 1 sizes and weights) |
| Straight selection bar, group rail, `selectionMarks.test.ts`, the goal picker rail | Already on `main` (phase 1) | — |
| Tinjau folder list, panes and hints | Phase 4 | — |

The branch is never merged. After this phase merges, the branch is deleted locally and on the remote, and its worktree `cash-flow-tracker-ui-refine` is removed, after checking that each "Taken" row is on `main` (Workflow follow-up).

### D20. What happens to each removed element

| Removed | Destination |
| --- | --- |
| Summary panel and its five figures | Hero + Rincian; group totals; Investasi gain; Utang total + Tagihan row (D3, D5) |
| `MiniRing` on Kartu | The split bar and legend in the hero; the ring in Grafik (D3, D11) |
| `RekeningComposition` and per-rekening stacked bars | Per-use subtotals in each Per rekening header (D5, flagged) |
| Obligation cards and the "Tagihan" row | The Tagihan link row to Target & Tagihan (owner request, D5) |
| Coloured card top edges, use tags, provider logo line | Group rails, the institution mark, the tile meta (D5, D6) |
| "Melewati limit" pill, reconciliation pill | One note line on the tile; the detail's facts (D6, D8) |
| `GainChip` | `GainText` with sign and one-decimal percent (D6) |
| Outer ring and focus list in Grafik | `.hbar` rows of the chosen use (D11) |
| Always-visible drag hint, phone "Ubah urutan" and `ReorderPanel` | Atur urutan mode with its bar and move buttons (D12) |
| `role=tablist` Kartu/Grafik | SPEC segmented control with `?tampilan` (D4) |
| Inline price notice | Toast (D15) |
| Six to eight equal detail buttons | Primary, secondary, "Lainnya" (D8) |
| Three fact tiles in the detail | Figure + `.dl` (D8) |
| Legacy `Modal` dialogs | `ShellDialog` (D8 to D10) |

### D21. Credit hand-off H8 coverage

| H8 requirement (`add-credit-spending-flow` D12) | Here | Fields |
| --- | --- | --- |
| Credit Account Tile: Utang, "Sisa limit {amount}" or "Melewati limit", the next bill "jatuh tempo {date} · {left}" | D7, spec "Credit Account Tile" | `credit_owed`, `limit_left`, `over_limit`, `next_due.due_date`, `next_due.left_to_pay`, `next_due.status` |
| Credit Account Detail: bills per due date with amount, paid, left and status; link to Transaksi filtered by the account | D8, spec "Credit Account Detail" | `GET /api/credit/accounts/{id}` `bills[]` (`due_date`, `amount`, `paid`, `left_to_pay`, `status`), `/ledger?account={id}` |
| Coordinator's H8 detail: instalments, settlement facts, days to settle, paid on time | D8 bill items, same spec | `items[]` (`kind`, `installment {number, count}`, `amount`, `left_to_pay`, `settled_on`, `days_to_settle`, `settled_on_time`), `totals` (`spent_on_credit`, `paid_to_credit`, `items_settled`, `items_settled_on_time`) |
| Credit Days in the Account Form: "Tagihan terbit tiap tgl", "Jatuh tempo tiap tgl", and that past bills keep their due dates | D9, spec "Credit Days in the Account Form" | `statement_day`, `payment_day` on `POST` and `PATCH /accounts` |

`due_dates_unavailable` (a bool in the account overview) shows the "Isi tanggal jatuh tempo" line (D8). The bill-level "paid on time" is the status `paid` versus `paid_late`; the item-level one is `settled_on_time`.

### D22. Verification tooling and gate

In `scripts/verify/markas/` (local only):
- **Scenarios** (`scenarios-app.json`, `scenarios-prototype.json`), each in `en` and `id` for the app:
  - `rekening` (Kartu, Per kegunaan), `rekening-per-rekening`, `rekening-grafik`, `rekening-grafik-tabungan`, `rekening-atur-urutan`;
  - `state-rekening-memuat`, `state-rekening-kosong`, `state-rekening-galat`, `state-rekening-hero-galat`;
  - `dlg-rincian-kekayaan`, `dlg-rekening-detail` (a pocket), `dlg-rekening-induk` (a parent), `dlg-rekening-kredit` (with bills, one opened), `dlg-rekening-kredit-tanpa-tanggal`, `dlg-produk` (unit-tracked), `dlg-produk-nominal`, `dlg-platform`, `dlg-rekening-tambah`, `dlg-rekening-tambah-kredit`, `dlg-kantong-tambah`, `dlg-beli`, `dlg-jual`, `dlg-topup`, `dlg-perbarui-nilai`, `dlg-sesuaikan-saldo`, `dlg-cocokkan` (with a difference), `dlg-cocokkan-cocok`, `dlg-nominal-ubah`, `dlg-menu-rekening`, `dlg-menu-akun`;
  - the deep link `?akun=<id>` and `?tampilan=grafik`.
- **`rekening-checklist.js` for `measure.js`**, generated from D1 to D14 and SPEC §2, §5 to §8 and §13:
  - the first content `y = 192`;
  - one 44 px figure and no other ≥ 33 px (the dialog's 32 px figure passes);
  - the split bar 8 px with 2 px gaps;
  - `.toolbar` margins 32/24; segment items h 30 (40 on a phone);
  - `.grp` padding-left 16, rail 2 px straight, groups 48 apart, `.grp-h` min-h 32;
  - tiles min-h 112, padding 16, gap 12, radius 12, border 1 px `--line`; four columns at 1440 and three at 1024;
  - `.inst` 24 × 24 radius 6; the balance Inter 20/600; `.gain` 12/500;
  - per-kind rows min-h 40;
  - `.hbar` columns `200 / 1fr / 136 / 136` and min-h 48; the legend rows 48;
  - the wallet tiles 280 × ≥ 152 at 390;
  - `.sel-bar` h 48;
  - dialog widths 480, 520 and 560, radius 16, header 24 24 16, body 0 24 24, footer 16 24; `.dl` 160; the sheet at 390;
  - sizes ⊆ {12, 14, 16, 20, 24, 32, 44}, weights ⊆ {400, 500, 600}, ≤ 8 text styles;
  - no border ≥ 2 px and no coloured border except the rails, the bars and focus;
  - no horizontal overflow at 1440, 1280, 1200, 1024, 768, 767, 390 and 320;
  - the institution mark's initials contrast ≥ 4.5:1 for each of the nine colours, in both themes.
- **`rekening-keys.mjs`**, the keyboard flows:
  1. tab order: skip link, sidebar, top bar, the two actions, "…", Rincian, the two segments, each group's tiles;
  2. Rincian: open, Esc, focus back on "Rincian";
  3. a tile: Enter opens the detail, Tab stays inside, "Lainnya" with arrows and Esc, Esc closes with focus on the tile;
  4. a credit tile: open, expand a bill with Enter, "Lihat di Transaksi" reachable, "Bayar tagihan" opens Perpindahan with the account and amount filled, Esc;
  5. Kartu → Grafik by keyboard; the legend toggles with Space; a product bar opens its detail;
  6. Per kegunaan → Per rekening and back;
  7. Atur urutan: "…" › Atur urutan, Alt + ArrowRight on a tile, Alt + ArrowDown on a row handle, the live messages, "Selesai", reload, the order kept per grouping;
  8. the phone reorder with move buttons at 390;
  9. the account form: Jenis by arrows, the credit days, a refused day out of range, Esc;
  10. Beli: the over-sale error on the units field, Batal returns focus;
  11. Cocokkan saldo: type, Periksa, the result announced, "Sesuaikan saldo" opens with the amount;
  12. "Perbarui harga" and its toast announced;
  13. the Tagihan row reaches `/goals`;
  14. no focused element under the top bar or the reorder bar.
- **Parity seed (`seed_parity.py`)** gains, from synthetic data:
  - SPayLater with a limit, `payment_day` 5, `statement_day` 25, a paid bill, an open bill due today with two instalments, one paid late, and a fee;
  - a second credit account without a `payment_day`;
  - a credit account over its limit and one overpaid;
  - two platforms: one with gains and losses, one with a product without cost basis;
  - an amount-tracked reksa dana;
  - two obligations;
  - a pocket with `reconciliation_required`;
  - an awaiting movement leg on a pocket;
  - a parent with an own balance besides its pockets.
  Each addition is listed in the seed report.
- **Stress and USD seed** (phases 4 and 5) for Rekening:
  - the audit seed (17 pockets, several platforms, long names);
  - 40 products;
  - a 60-character pocket name;
  - Rp 1.234.567.890;
  - the USD display currency.
- **Figure parity (`parity.mjs`):** every amount, percent, count and gain on the page and in each detail, read from the DOM, against `main`.
  - The expected differences are named in advance: the gain percent with one decimal, the Utang group total = `−credit_owed`, the new Tagihan row, and the five summary figures leaving.
  - The checks: Utang total + Tagihan row = `total_liabilities`, hero = `net_worth`, and the legend percentages = today's share bar.

The gate (tasks group 5) is the plan's common gate plus the both-language gate of `add-language-system` (its D16).

### D23. Spec reconciliation

`accounts-wallet-view`:
- **Modified:** Wallet Page Layout, Capital Gain Everywhere, Saved Layout per Grouping.
- **Added:** Rekening Hero, Rekening Groups and Tiles, Utang Without Obligations, Rekening Grafik, Atur Urutan Mode, Account Detail Dialog, Rekening Loading, Failure and Empty States, Rekening in Both Languages.

Other capabilities:
- `net-worth-analytics`:
  - Modified: Net Worth Endpoint and Display, Asset Composition on Rekening (the split bar by use replaces the per-account Komposisi aset), Investment Gain in the Account List (the format).
  - Unchanged: Net Worth & Liquidity Aggregation and Cash Flow Runway Forecast.
- `pocket-reordering`:
  - Modified: Sibling Pocket Drag-and-Drop. Pockets reorder as tiles inside their row in Atur urutan, saved by the layout API. `POST /api/accounts/reorder` is kept for API consumers.
  - Removed: Account Card Alignment and Compact Height (two-column cards no longer exist).
- `clean-core-ledger`: Account and Pocket Action Clarity; Parent Balance Aggregation and Grouped Representation (item 4).
- `account-reconciliation`: Reconciliation-Required Flag on the Accounts Page ("Perlu dicocokkan" on the tile and in the detail).
- `credit-accounts`:
  - Modified: Credit Account Type (its first scenario names the tile), written over the `add-credit-spending-flow` version, which merges first. Task 1.2 re-copies it from the merged `main` before archive.
  - Added, H8 verbatim: Credit Account Tile, Credit Account Detail, Credit Days in the Account Form.
- `mobile-native-layout`: Phone Rekening & Kantong Composition. Phone Page Actions and Targets holds as written.
- `account-roles` ("Kas Investasi Outside Siap Pakai" names a figure, not a label on this page), `investment-trade-recording`, `investment-topups`, `investment-amount-conversion`, `interface-language`, `ui-copy-register` and `design-tokens` need no delta.

Legacy "Required behavior and constraints" scenarios in the modified requirements are kept and updated, not split, so every constraint they carried stays in the spec.

### D24. Dependencies and order

- **Merged first, with a stop otherwise (task 1.2):**
  - `add-language-system` and `localize-server-text`;
  - phase 5 `apply-markas-transaksi`, which brings the shared `RowAmount` with the red expense, the dialog conventions, `.dl` and `.box`, and hands over `InvestmentTopupModal`;
  - `add-credit-spending-flow` (H8 fields and `GET /api/credit/accounts/{id}`).
  Phase 4 is merged before phase 5, so it is included.
- **Independent:** phases 6 and 7.
- **Builds on this phase:** phase 9 (`apply-markas-target`) uses `ProgressBar`, `creditFacts`, the new `terms`, `AccountDetailDialog` and `openMovementCapture`.
- **Adapt and record:** if a merged phase's API differs from what is named here (a component name, a prop), this phase adapts to the merged code and records the difference in `verification.md`. It does not stack on unmerged branches.

## Decisions and their defaults

Approved under the owner's standing instruction 'complete all until finished' (2026-11-06, renewed 2026-11-08); decisions take their recommended defaults. **Flag** marks a decision reviewed before implementation (task 1.1), because it goes beyond the prototype or the approved plan, or touches the presentation of money.

| # | Decision | Default (taken) | Rejected alternatives | Flag |
| --- | --- | --- | --- | --- |
| D1 | Layout and breakpoints | One column in 1200; tiles `auto-fill minmax(240,1fr)`; viewport queries | Container measure; context panel | — |
| D2 | Title actions | Perbarui harga, Tambah rekening; "…" Atur urutan, Rencana (in place) | Rencana as a link to `/goals`; visible Atur urutan | — (deviation 9) |
| D3 | Hero | `net_worth` + Rincian (server lines), "dari aset", split bar by use with % | Five-figure summary; "· utang" in the line; dotted figure | **Flag: presentation of figures** (summary figures move to groups and the Rincian; "Dana likuid") |
| D4 | Controls | SPEC segments; Kartu/Grafik in `?tampilan`; grouping per browser | Tablist; React state; grouping in the URL | — |
| D5 | Groups and Utang | Rails, quiet headers, gains; Utang credit-only + Tagihan link row; Per rekening subtotals in words | Obligation tiles and "Utang lain" (prototype); summary Tagihan figure; stacked composition bars | **Flag: owner-directed scope** (Tagihan), **earlier owner choice** (composition bar) |
| D6 | Tiles | Flat tile, institution mark, one note line, `GainText` "▲ +Rp … · +8,0%" | Coloured edges; several pills; two-decimal percent | **Flag: presentation of figures** (one-decimal percent) |
| D7 | Credit tile | Owed, Sisa limit or Melewati limit, next bill | Prototype "terpakai dari", "bayar tiap tgl" | — (H8; deviation 3) |
| D8 | Detail | 560 standalone dialog; figure + `.dl`; credit bills with items; primary/secondary/Lainnya; "Bayar tagihan" prefill | Eight equal buttons; bills in a separate dialog; Catat without amount | **Flag: scope addition** (`openMovementCapture`) |
| D9 | Forms | SPEC form; Jenis segmented; both credit days with the hint; nine colours | Prototype six colours; one day field | — (H8; deviation 7) |
| D10 | Money dialogs | `ShellDialog`, unchanged requests; Beli and Jual separate | Fee field; keep `Modal` | — (deviation 8) |
| D11 | Grafik | Ring of uses as selector + legend; chosen use as `.hbar` rows with gains; rails per platform | Prototype per-use bars; two rings | **Flag: owner preference over prototype and dataviz** |
| D12 | Reorder | Atur urutan mode only; drag, Alt + arrows, phone move buttons | Always-on drag; separate phone list | **Flag: scope (interaction)** |
| D13 | Phone | Wallet for Uang harian; one tile per row; sheets | Grid on phone | — |
| D14 | States | SPEC skeleton, error boxes, empty state | Grey blocks | — |
| D15 | Prices | Toast | Inline notice | — |
| D16 | Language | `accounts` namespace, new shared `terms`, `/accounts` translated, "Dana likuid", "Perlu dicocokkan" | Two terms for one concept; untranslated dialogs | — |
| D17 | Hidden balances | `MaskedMoney` on every amount; percentages visible | Hide percentages | — |
| D18 | Shared pieces | `ProgressBar`, `GainText`, `credit.ts`, standalone detail, `openMovementCapture` | Page-only copies | — |
| D19 | Held branch | Fold the Rekening parts; delete after this phase | Merge it first | — |
| D20 | Removed elements | As the table | — | — |
| D21 | H8 | As the table | — | — |
| D22 | Tooling | Scenarios, checklist, key flows, extended seeds, parity | Existing seed only | — |
| D23 | Specs | As listed | — | — |
| D24 | Dependencies | Phase 5, both language changes and the credit flow merged first; stop otherwise | Stacking on unmerged branches | — |

No default changes money behaviour, a request, a total's value or what net worth counts. The flagged D3, D5 and D6 change where and how figures are shown. D8 adds a client function that prefills a form. D11 and D12 keep owner choices or change an interaction.

## Risks / Trade-offs

- **[Utang total changes meaning]** The Utang group total was credit plus obligations; it becomes credit only. Mitigation:
  - the Tagihan row sits directly under the credit tiles with the rest;
  - the Rincian lists both;
  - the parity check proves that Utang + Tagihan = `total_liabilities`.
- **[Reorder behind a menu]** The owner reorders "based on my usual usage"; one more step. Mitigation: the item is the first in "…", the mode is visible, and the fallback (always-on drag) is recorded.
- **[Ring versus dataviz]** Mitigation: the ring is a selector with a full legend, the hero carries the bar, and the fallback is the prototype's bars.
- **[Standalone detail]** Phase 9 opens it outside `/accounts`. Mitigation: it loads its own data by query key, and the keyboard and focus flows run from both pages.
- **[Initials contrast on owner colours]** Mitigation: a computed choice of white or ink, pinned by a test for the nine colours in both themes.
- **[Credit API drift]** The credit fields are on an unmerged branch at planning time. Mitigation: task 1.2's stop condition, and the adapt-and-record rule (D24).
- **[Shared money dialogs]** Transaksi and Tinjau also open the trade and top-up dialogs. Mitigation: their scenarios are captured before and after (task 1.3), and their requests are compared in the tests.
- **[Copy volume]** About 260 strings move to keys. The Indonesian text is checked against today's by a rendered-text snapshot before and after (task 3.4), with the intended changes of D16 listed.

## Migration and rollback

No data, schema or API change and no migration. The saved layouts (`account_layouts`) are read and written as today, so an order saved after this phase is valid for the previous release and the other way round. Rollback is a revert of the merge commit; `?tampilan=grafik` is ignored by the previous release. The catalogue keys revert with the commit.

## Open Questions

None. The flagged decisions take their defaults under the owner's standing instruction and are reviewed in task 1.1. Their fallbacks are recorded above.

## Appendix A. Rekening copy as message keys (`accounts` namespace; Indonesian · English)

`{…}` are ICU arguments; `#` is the plural count. Amounts, dates and percentages arrive formatted.

### `page`, `hero`, `rincian`, `controls`, `toast`, `states`

| Key | Indonesian | English |
| --- | --- | --- |
| `page.subtitle` | Saldo rekening, kantong, investasi dan utang | Balances of accounts, pockets, investments and debts |
| `page.updatePrices` · `page.updatingPrices` | Perbarui harga · Memperbarui harga… | Update prices · Updating prices… |
| `page.addAccount` | Tambah rekening | Add account |
| `page.arrange` · `page.arrangeDone` | Atur urutan · Selesai mengatur urutan | Arrange · Finish arranging |
| `page.plans` | Rencana | Plans |
| `hero.fromAssets` | dari aset {amount} | of {amount} in assets |
| `hero.splitLabel` | Pembagian aset: {parts} | Assets by use: {parts} |
| `hero.share` | {use} {percent} | {use} {percent} |
| `hero.failed` | Kekayaan bersih belum dapat dihitung. | Net worth cannot be calculated yet. |
| `rincian.rule` | Dana likuid, Dana investasi dan Investasi dikurangi utang, tagihan dan saldo minus yang perlu dicocokkan. Saldo kantong dihitung sekali. | Available cash, investment cash and investments, less debts, bills and negative balances to match. Pocket balances count once. |
| `rincian.billLine` · `rincian.debtLine` | Tagihan · {name} · Utang · {name} | Bill · {name} · Debt · {name} |
| `rincian.openGoals` | Buka Target & Tagihan | Open Goals & Bills |
| `controls.view` · `controls.cards` · `controls.chart` | Tampilan · Kartu · Grafik | View · Cards · Chart |
| `controls.grouping` · `controls.byUse` · `controls.byAccount` | Kelompokkan · Per kegunaan · Per rekening | Group · By use · By account |
| `toast.pricesUpdated` | Harga diperbarui · {count, plural, other {# produk}}, per {time} | Prices updated · {count, plural, one {# product} other {# products}}, as of {time} |
| `toast.pricesFailed` | Harga belum dapat diperbarui. | Prices could not be updated. |
| `toast.orderFailed` | Urutan gagal disimpan. | The order could not be saved. |
| `states.loadFailed` | Daftar rekening gagal dimuat. | Accounts could not be loaded. |
| `states.emptyTitle` · `states.emptyLine` | Belum ada rekening · Tambahkan rekening bank, e-wallet atau tunai untuk mulai. | No accounts yet · Add a bank account, e-wallet or cash to start. |

### `groups`, `kinds`, `tile`, `credit`, `debt`

| Key | Indonesian | English |
| --- | --- | --- |
| `groups.harian` · `groups.tabungan` · `groups.dana` · `groups.investasi` · `groups.utang` | Uang harian · Tabungan · Dana investasi · Investasi · Utang | Everyday money · Savings · Investment cash · Investments · Debt |
| `groups.danaNote` | belum dibelikan | not yet invested |
| `groups.pockets` · `groups.products` · `groups.accounts` · `groups.credit` | {count, plural, other {# kantong}} · {count, plural, other {# produk}} · {count, plural, other {# rekening}} · {count, plural, other {# kredit}} | {count, plural, one {# pocket} other {# pockets}} · {count, plural, one {# product} other {# products}} · {count, plural, one {# account} other {# accounts}} · {count, plural, one {# credit account} other {# credit accounts}} |
| `groups.subtotal` | {use} {amount} | {use} {amount} |
| `groups.needCheck` | {count, plural, other {# kantong perlu dicocokkan}} | {count, plural, one {# pocket needs a balance check} other {# pockets need a balance check}} |
| `kinds.emas` · `kinds.reksaDana` · `kinds.saham` · `kinds.lainnya` | Emas · Reksa dana · Saham · Lainnya | Gold · Mutual funds · Stocks · Other |
| `tile.pocketOf` · `tile.default` | Kantong di {parent} · bawaan | Pocket in {parent} · default |
| `tile.outsidePockets` | saldo di luar kantong | balance outside pockets |
| `tile.type.bank` · `cash` · `ewallet` · `brokerCash` · `broker` · `credit` | Bank · Uang tunai · E-wallet · Rekening dana investasi · Akun investasi · Kredit | Bank · Cash · E-wallet · Investment cash account · Investment account · Credit |
| `tile.byAmount` · `tile.lots` · `tile.grams` · `tile.units` | dicatat per nominal · {count} lot · {count} gram · {count} unit | recorded by amount · {count} lots · {count} grams · {count} units |
| `tile.needsCheck` | Perlu dicocokkan | Needs a balance check |
| `tile.awaiting` | {count, plural, other {# pindah saldo belum lengkap}} | {count, plural, one {# transfer not complete} other {# transfers not complete}} |
| `tile.gainUp` · `tile.gainDown` · `tile.costMissing` · `tile.costIncomplete` | untung · rugi · Modal belum diisi · Modal belum lengkap | gain · loss · Cost not entered · Cost incomplete |
| `credit.limitLeft` | Sisa limit {amount} | Limit left {amount} |
| `credit.noLimit` | Tanpa limit | No limit |
| `credit.nextBill` | jatuh tempo {date} · {amount} | due {date} · {amount} |
| `credit.dueToday` · `credit.overdue` | hari ini · lewat jatuh tempo | today · overdue |
| `credit.noDueDay` | Tanggal jatuh tempo belum diisi | No due day yet |
| `credit.overpaid` | Lebih bayar {amount} | Overpaid {amount} |
| `debt.billsLink` · `debt.billsCount` | Tagihan di Target & Tagihan · {count, plural, other {# tagihan}} | Bills in Goals & Bills · {count, plural, one {# bill} other {# bills}} |
| `debt.billsName` | Tagihan {amount} di Target & Tagihan | Bills {amount} in Goals & Bills |

### `grafik`, `reorder`, `announce`

| Key | Indonesian | English |
| --- | --- | --- |
| `grafik.byUse` · `grafik.pick` · `grafik.assets` | Per kegunaan · Pilih kegunaan · Aset | By use · Choose a use · Assets |
| `grafik.shareOfAssets` | {percent} aset | {percent} of assets |
| `grafik.perKind` · `grafik.perProduct` · `grafik.contents` | Investasi per jenis · Per produk · Isi {use} | Investments by kind · By product · In {use} |
| `reorder.hint` | Seret kartu dalam baris, atau seret ⋮⋮ untuk memindah baris. Dengan keyboard: Alt + panah. Urutan diingat. | Drag a card within its row, or drag ⋮⋮ to move a row. With the keyboard: Alt + arrows. The order is kept. |
| `reorder.hintPhone` | Pakai tombol naik dan turun. Urutan langsung tersimpan. | Use the up and down buttons. The order is saved right away. |
| `reorder.done` | Selesai | Done |
| `reorder.rowHandle` | Pindah baris {title}. Alt dan panah atas atau bawah. | Move row {title}. Alt and the up or down arrow. |
| `reorder.cardEarlier` · `reorder.cardLater` · `reorder.rowUp` · `reorder.rowDown` | Geser ke depan {name} · Geser ke belakang {name} · Naikkan baris {name} · Turunkan baris {name} | Move {name} earlier · Move {name} later · Move row {name} up · Move row {name} down |
| `announce.cardMoved` · `announce.rowMoved` | {name} dipindah ke posisi {position} dari {total}. · Baris {title} dipindah ke posisi {position} dari {total}. | {name} moved to position {position} of {total}. · Row {title} moved to position {position} of {total}. |

### `detail`, `actions`, `links`

| Key | Indonesian | English |
| --- | --- | --- |
| `detail.balance` · `detail.value` · `detail.debt` | Saldo · Nilai sekarang · Utang | Balance · Current value · Debt |
| `detail.use` · `detail.parent` · `detail.mainPocket` | Kegunaan · Induk · Kantong utama | Use · Parent · Main pocket |
| `detail.cost` · `detail.gain` · `detail.method` · `detail.amount` · `detail.lastPrice` · `detail.avgPrice` · `detail.code` · `detail.fundingAccount` | Modal · Untung/rugi · Cara hitung · Jumlah · Harga terakhir · Modal beli rata-rata · Kode · Rekening dana | Cost · Gain or loss · How it is valued · Holding · Last price · Average buy price · Code · Funding account |
| `detail.estimated` | nilai estimasi | estimated value |
| `detail.limit` · `detail.nextBill` · `detail.statementDay` · `detail.paymentDay` | Limit · Tagihan berikut · Tagihan terbit tiap tgl · Jatuh tempo tiap tgl | Limit · Next bill · Bill issued on day · Due on day |
| `detail.dayOfMonth` | tiap tanggal {day} | every {day, selectordinal, one {#st} two {#nd} few {#rd} other {#th}} |
| `detail.spentOnCredit` · `detail.paidToCredit` · `detail.onTime` | Pengeluaran pakai kredit · Tagihan kredit dibayar · Dilunasi tepat waktu | Spending on credit · Credit bills paid · Paid on time |
| `detail.onTimeValue` | {onTime} dari {settled} | {onTime} of {settled} |
| `detail.bills` · `detail.billsCount` · `detail.billDue` · `detail.billPaid` · `detail.billLeft` | Tagihan · {count, plural, other {# tagihan}} · Jatuh tempo {date} · dibayar {amount} · sisa {amount} | Bills · {count, plural, one {# bill} other {# bills}} · Due {date} · paid {amount} · {amount} left |
| `detail.item.fee` · `detail.item.cashAdvance` · `detail.item.opening` | Biaya kredit · Tarik tunai · Saldo awal | Credit fee · Cash advance · Opening balance |
| `detail.item.installment` | cicilan {number} dari {count} | installment {number} of {count} |
| `detail.item.settled` | lunas {date} · {days, plural, other {# hari}} · {onTime, select, true {tepat waktu} other {terlambat}} | paid {date} · {days, plural, one {# day} other {# days}} · {onTime, select, true {on time} other {late}} |
| `detail.noDueDates` | Isi tanggal jatuh tempo agar tagihan bisa dihitung. | Add the due day so the bills can be worked out. |
| `detail.billsFailed` | Tagihan belum bisa dimuat. | Bills could not be loaded. |
| `detail.viewInLedger` | Lihat di Transaksi | View in Transactions |
| `detail.pockets` · `detail.products` · `detail.recent` | Kantong · Produk · Transaksi terakhir | Pockets · Products · Recent transactions |
| `detail.noRecent` · `detail.recentFailed` | Belum ada transaksi. · Transaksi terakhir belum bisa dimuat. | No transactions yet. · Recent transactions could not be loaded. |
| `actions.more` | Lainnya | More |
| `actions.checkBalance` · `actions.adjustBalance` | Cocokkan saldo · Sesuaikan saldo | Check balance · Adjust balance |
| `actions.editPocket` · `actions.editAccount` · `actions.editCredit` · `actions.addPocket` | Ubah kantong · Ubah rekening · Ubah kredit · Tambah kantong | Edit pocket · Edit account · Edit credit · Add pocket |
| `actions.buy` · `actions.sell` · `actions.topUp` · `actions.revalue` · `actions.toAmount` | Beli · Jual · Top up · Perbarui nilai · Ubah ke pelacakan nominal | Buy · Sell · Top up · Update value · Switch to tracking by amount |
| `actions.payBill` · `actions.openParent` · `actions.archive` | Bayar tagihan · Buka {parent} · Arsipkan | Pay bill · Open {parent} · Archive |
| `actions.archiveConfirm` | Arsipkan {kind, select, pocket {kantong} other {rekening}} "{name}"{withPockets, select, true { beserta seluruh kantong di dalamnya} other {}}? | Archive {kind, select, pocket {pocket} other {account}} "{name}"{withPockets, select, true { with all its pockets} other {}}? |
| `links.title` · `links.remove` · `links.confirm` · `links.removing` | Juga dilaporkan · Lepaskan · Ya, lepaskan · Melepas… | Also reported by · Remove · Yes, remove · Removing… |

### `form`, `pocketForm`, `trade`, `topup`, `valuation`, `adjust`, `amountTracking`, `check`

| Key | Indonesian | English |
| --- | --- | --- |
| `form.titleNew` · `form.titleEditAccount` · `form.titleEditPocket` | Tambah rekening · Ubah rekening · Ubah kantong | Add account · Edit account · Edit pocket |
| `form.subtitleNew` | Rekening, e-wallet, kantong atau kredit | Account, e-wallet, pocket or credit |
| `form.name` · `form.namePlaceholder` | Nama · Misalnya: Jago · Belanja Bulanan | Name · For example: Jago · Monthly shopping |
| `form.type` · `form.typeBank` · `form.typeEwallet` · `form.typeCash` · `form.typeInvestment` · `form.typeCredit` | Jenis · Bank · E-wallet · Tunai · Investasi · Kredit | Type · Bank · E-wallet · Cash · Investment · Credit |
| `form.role` · `form.parent` · `form.noParent` · `form.moveUnder` | Kegunaan · Induk (opsional) · Tanpa induk · Pindahkan ke rekening | Use · Parent (optional) · No parent · Move under account |
| `form.defaultPocket` | Kantong bawaan untuk notifikasi | Default pocket for notifications |
| `form.model` · `form.modelMulti` · `form.modelSingle` · `form.funding` | Model akun investasi · Beberapa produk · Satu produk · Sumber dana bawaan | Investment account model · Several products · One product · Default funding account |
| `form.instrumentType` · `form.ticker` | Jenis instrumen · Kode (Yahoo Finance) | Instrument type · Code (Yahoo Finance) |
| `form.owedNow` · `form.limit` | Utang saat ini (IDR) · Limit (opsional) | Owed now (IDR) · Limit (optional) |
| `form.statementDay` · `form.paymentDay` | Tagihan terbit tiap tgl (opsional) · Jatuh tempo tiap tgl (opsional) | Bill issued on day (optional) · Due on day (optional) |
| `form.daysHint` | Tagihan yang sudah terbit tetap memakai tanggal jatuh temponya. | Bills already issued keep their due dates. |
| `form.dayRange` | Isi tanggal 1 sampai 31. | Enter a day from 1 to 31. |
| `form.colour` · `form.colourName` | Warna · Warna {name} | Colour · Colour {name} |
| `form.opening` | Saldo awal (IDR) | Opening balance (IDR) |
| `form.cancel` · `form.save` · `form.create` · `form.saving` | Batal · Simpan · Tambah rekening · Menyimpan… | Cancel · Save · Add account · Saving… |
| `pocketForm.title` · `pocketForm.create` | Tambah kantong di {parent} · Buat kantong | Add a pocket in {parent} · Create pocket |
| `trade.buy` · `trade.sell` · `trade.lots` · `trade.grams` · `trade.pricePerShare` · `trade.pricePerGram` | Beli · Jual · Lot · Gram · Harga per lembar · Harga per gram | Buy · Sell · Lots · Grams · Price per share · Price per gram |
| `trade.quickFill` | Isi cepat: Pasar {market} · Modal {cost} | Quick fill: Market {market} · Cost {cost} |
| `trade.from` · `trade.into` · `trade.estimate` | Dari rekening · Masuk ke · Perkiraan total | From account · Into · Estimated total |
| `trade.tooMany` | Jumlah melebihi yang dimiliki ({owned}). | More than the holding ({owned}). |
| `topup.title` · `topup.editTitle` · `topup.amount` · `topup.sameAsLast` · `topup.plan` | Top up · Ubah top up · Nominal (IDR) · Sama dengan top up terakhir ke produk ini. · Buat rencana bulanan (tidak mencatat) | Top up · Edit top-up · Amount (IDR) · Same as the last top-up to this product. · Make a monthly plan (records nothing) |
| `valuation.title` · `valuation.value` · `valuation.cost` | Perbarui nilai · Nilai sekarang (IDR) · Modal (IDR) | Update value · Current value (IDR) · Cost (IDR) |
| `adjust.title` · `adjust.actual` · `adjust.in` · `adjust.out` | Sesuaikan saldo · Saldo sebenarnya (IDR) · uang masuk · uang keluar | Adjust balance · Actual balance (IDR) · money in · money out |
| `amountTracking.title` · `amountTracking.note` | Ubah ke pelacakan nominal · Perubahan ini tidak bisa dibatalkan. | Switch to tracking by amount · This change cannot be undone. |
| `check.title` · `check.subtitle` | Cocokkan saldo · Bandingkan {name} dengan saldo di aplikasi bank | Check balance · Compare {name} with the balance in the bank app |
| `check.bankBalance` · `check.recorded` · `check.run` · `check.again` | Saldo di aplikasi bank (IDR) · Tercatat di sini · Periksa · Periksa lagi | Balance in the bank app (IDR) · Recorded here · Check · Check again |
| `check.matched` · `check.matchedLine` | Saldo cocok · Catatan sama dengan aplikasi bank. | Balance matches · The record equals the bank app. |
| `check.result` · `check.more` · `check.less` · `check.causes` | Hasil pemeriksaan · selisih: aplikasi bank lebih banyak · selisih: aplikasi bank lebih sedikit · Kemungkinan penyebab | Check result · difference: the bank app shows more · difference: the bank app shows less · Possible causes |
| `check.close` · `check.adjust` | Tutup · Sesuaikan saldo | Close · Adjust balance |

## Appendix B. Rekening glossary (Indonesian → English)

| Indonesian | English | Note |
| --- | --- | --- |
| Rekening & Kantong | Accounts & Pockets | `nav.accounts` |
| Rekening · Kantong · Produk | Account · Pocket · Product | |
| Kegunaan | Use | the role |
| Uang harian | Everyday money | `terms.everydayMoney` |
| Tabungan | Savings | `terms.savings` |
| Dana investasi | Investment cash | `terms.investmentCash`; never "Kas investasi" |
| Investasi | Investments | `terms.investments` |
| Utang | Debt | `terms.debt` |
| Tagihan | Bills | `terms.bills`; obligations, and credit bills in the detail |
| Kekayaan bersih | Net worth | `terms.netWorth` |
| Dana likuid | Available cash | `terms.availableCash`; replaces "Saldo tersedia" |
| Aset | Assets | |
| Untung/rugi · untung · rugi | Gain or loss · gain · loss | |
| Modal · Modal belum diisi · Modal belum lengkap | Cost · Cost not entered · Cost incomplete | |
| Kartu · Grafik | Cards · Chart | |
| Per kegunaan · Per rekening | By use · By account | |
| Atur urutan | Arrange | |
| Perbarui harga · Perbarui nilai | Update prices · Update value | |
| Cocokkan saldo · Sesuaikan saldo · Perlu dicocokkan | Check balance · Adjust balance · Needs a balance check | |
| Beli · Jual · Top up | Buy · Sell · Top up | |
| Dicatat per nominal · Pelacakan nominal | Recorded by amount · Tracking by amount | |
| Kredit | Credit | |
| Sisa limit · Melewati limit · Tanpa limit | Limit left · Over limit · No limit | `terms.limitLeft`, `terms.overLimit` |
| Lebih bayar | Overpaid | |
| Tagihan terbit tiap tgl · Jatuh tempo tiap tgl | Bill issued on day · Due on day | |
| Tagihan berikut · Sisa tagihan | Next bill · Left to pay | `terms.leftToPay` |
| Terbuka · Akan datang · Jatuh tempo hari ini · Lewat jatuh tempo · Lunas · Lunas terlambat | Open · Upcoming · Due today · Overdue · Paid · Paid late | `terms.billStatus.*` |
| Cicilan {k} dari {n} | Installment {k} of {n} | |
| Pengeluaran pakai kredit · Tagihan kredit dibayar | Spending on credit · Credit bills paid | |
| Bayar tagihan | Pay bill | |
| Juga dilaporkan | Also reported by | |
| Bawaan | Default | |
| Saldo di luar kantong | Balance outside pockets | |
| Belum dibelikan | Not yet invested | |
