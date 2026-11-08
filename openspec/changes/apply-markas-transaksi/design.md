## Context

Transaksi (`/ledger`) on `main` `5eff424`, inside the phase 2 shell (frame 1040 px, title block, `PageActions`):
- `app/ledger/page.tsx` (673 lines) holds the URL filters, the paged query, consolidation, day grouping, the merge selection, the keys and every handler. It renders:
  - the shared `PeriodStepper` in `PageActions` when a period is set;
  - three `KpiCard`s (Masuk, Pengeluaran with its reimbursement or saving caption, Bersih);
  - one card with a toolbar row: type tabs with counts, search, a `Disclosure` popover "Filter" (period segment, four single-select chips, account, category, pillar and status selects, "Atur ulang filter") and a `Disclosure` "…" (Gabungkan dua transaksi, Rutin);
  - removable active-filter chips, the heading "Daftar transaksi · N transaksi", and `LedgerTable` or `LedgerFeed`;
  - "Muat 50 lagi", a viewport-fixed merge bar, `TransactionDrawer`, the "Nominal tidak sama" `Modal` and `RecurringRulesModal`.
- The table or feed is chosen by `useWidthAtMost(listRegionRef, 767)`, the width of the list card itself. At 1024 to about 1100 px the card is below 767 px (1024 − 240 sidebar − 96 margins = 688), so a laptop gets the phone feed. **This is the cause of the known 1024–1100 px issue.**
- `components/transactions/LedgerList.tsx`: `LedgerTable` (columns Transaksi, Jam, Jenis, Pilar, Kategori, Jumlah, with a 32 px avatar, a two-line title cell, a type `Pill` and coloured day totals) and `LedgerFeed` (two-line phone rows with avatars and long-press selection). Amounts come from `RowAmount` in `components/beranda/activity.tsx`, which colours expenses coral.
- `components/transactions/TransactionDrawer.tsx`: a `Drawer` (centered from 768 px, a sheet below) with the amount and type pill, the Tinjau banner, a facts list, the original text, the inline `TransactionEditForm`, the delete confirmation and three dynamically loaded modals: `ExpenseTopupConversionModal`, `InternalMovementModal` (movement edit, split and delete) and `InvestmentTopupModal`. Beranda, Kalender and Peta Aliran open the same drawer.
- Data, unchanged by this phase:
  - `GET /transactions?<filters>&include=daily_totals,type_counts&limit=50&offset=N` with `logical_movements=true` when `listsLogicalMovements` allows, returning `total`, `transactions`, `summary.{cycle,cumulative,period}`, `daily_totals` and `type_counts`;
  - `GET /transactions/{id}`, `/accounts`, `/categories`, `/goals`, `/obligations`;
  - writes: `PATCH /transactions/{id}`, `POST /transactions/{id}/receipt`, `DELETE /transactions/{id}`, `/movements/{id}` and `/investment-topups/{id}`, `PATCH /movements/{id}`, `POST /movements/{id}/split`, `POST /movements/merge`, `POST /review/bulk` (accept), the top-up conversion and `POST /review/undo` through `useTransactionUndo`.
- URL contract (`lib/ledgerFilters.ts`): `period`, `offset`, `category`, `kakeibo`, `type`, `account`, `review`, `from`, `to`, `q`, `dup`, `source`, `waiting`, plus `edit`, `return=review`, `folder` and `fokus=cari` on the page.

The prototype (`option-6/js/pages.js` `PAGES.transaksi`, `M.txRow`, `dayHead`, `dayTotals`; `js/dialogs.js` `transaksi`, `transaksi-ubah`, `filter` and the `transaksi` menu; `styles.css` §Rows, §Rail, §Transaksi, §States, §Layers, Tablet, Phone) is:
- a title block with the stepper and "…";
- a hero with two supporting figures beside it;
- `200 | 1fr` with a sticky rail, a toolbar, chips, an inline selection bar, day groups and one-line rows;
- centered dialogs for the detail (560), the edit (600) and the filter (640);
- on a phone, the hero, two figures, a scrolling type control, search and Filter, and two-line rows.

Inputs this phase builds on:
- **Phase 1:** tokens, type roles, the straight selection bar (`ws-select-row`, `selectionMarks.test.ts`), buttons, fields, segmented control, menu, dialog and chip primitives.
- **Phase 2:** the title block, `PageActions` with its "…" menu, `ShellDialog`, the frame width 1040 on `/ledger`, `fokus=cari`, the bell carrying due plans.
- **Phase 3:** `HeroFigure`, `FigureBlock`, `ListRow`, `DayHeading`, `SectionHeader`, `MaskedMoney`, the skeleton and error rules, the Beranda row helpers.
- **Phase 4 (`apply-markas-tinjau`):** `Rail`, `RailItem` and `RailRule`; `HeroFigure` with an optional mark; `ListRow`'s `current` and `rowKey` (`data-row`); the `.raw` box; the removal of `ChipGroup`; the untranslated-page test example moved to Transaksi.
- **`add-language-system`:** next-intl with `messages/{en,id}.json`, `terms`, `common` (`common.period.*`, `common.rowMarks.*`), `lib/format.ts`, `lib/i18n/translatedRoutes.ts`, the copy register per language, `renderWithIntl`, the both-language gate and the en-versus-id figure check.
- **`localize-server-text`:** every error response is `{code, params, message}`, including the plain-string movement errors, and stored generated notes are written in the account's language.

Owner feedback of 2026-11-06, after looking at production (proposal "Owner feedback"):
1. expense amounts red on Transaksi, in the dialog and on Beranda (D29);
2. transfer rows titled by their note, with the route as meta (D11);
3. the saving direction instead of "Tidak dihitung" for transfers, classified like Beranda's "Ditabung & investasi" (D27);
4. a "Tabungan & investasi" type whose cycle total equals Beranda's figure (D28);
5. room for `add-credit-spending-flow` without building it (D30).
One role per account stays; the owner rejected several roles and tags.

How the server counts "Ditabung & investasi" today (`routers/dashboard.py` `get_kakeibo_breakdown`, `services/cycle_summary.py` `savings_change`), which D27 and D28 follow:
- `total = masuk − diambil − belanja + pilar_tabungan`, the Kakeibo saving bucket;
- a **savings destination** is an account with role `tabungan`, an investment product (`type = 'investment'` or an `instrument_type`), or an account linked to a goal (`goal_accounts`). `dana_investasi`, `harian` and `utang` are not;
- `masuk`: durably linked movements from a non-destination into a destination; `diambil`: from a destination into a non-destination; a move between two destinations, or two non-destinations, counts in neither;
- a movement touching an investment product counts only as an investment top-up (or a legacy row stored with the saving pillar), so a Beli/Jual trade is outside;
- `belanja`: reportable expenses paid from a `tabungan` or goal-linked account; `pilar_tabungan`: reportable expenses whose effective pillar is saving, net of reimbursement offsets per pillar;
- roles and goal links are read as they are now, so a later role change also applies to earlier movements. Movement legs sit on leaf pockets (a parent is resolved to its effective pocket when recording), so only leaves count.

Constraints:
- No money logic or existing API field changes. Every figure is a server field formatted by `lib/format.ts`. The only client arithmetic is today's: consolidation of movement legs, day grouping, the merge checks and the "N lagi" counts. The backend additions of D27 and D28 are read-only and reuse the server's own savings rule.
- Every filter, the URL contract, every deep link, the keys, the history entry of the phone sheet and every request keep their behaviour and tests.
- The held branch `fix/selection-bars-and-tinjau` (`64909ee`) has no Transaksi part. Its parts went to phases 1, 4 and 8.

## Goals / Non-Goals

**Goals:**
- Transaksi and its dialogs match the prototype at 1440, 1100, 1024 and 390 px in both themes and both languages, measured against `styles.css`. The only exceptions are the recorded deviations.
- One row amount for Beranda, Transaksi and the Rekening detail, with the expense in the expense red everywhere it carries "−" (owner feedback 1).
- Every transfer row says what it did to savings by the same rule as Beranda's "Ditabung & investasi", and "Tabungan & investasi" adds up to that figure.
- No width from 768 px up shows the phone layout.
- Transaksi's copy lives in the `transactions` namespace in English and Indonesian, and `/ledger` leaves the untranslated set.
- Nothing obsolete remains: `KpiCard`, the table and feed, the avatars, the popovers, `InternalMovementModal`, the dead row marks and the Transaksi-only CSS.

**Non-Goals:**
- Backend changes beyond the read-only additions of D27 and D28; data, schema or companion changes; a delete undo; a duplicate flag per row; a receipt preview.
- Changing what Beranda counts as "Ditabung & investasi"; a second role per account; tags; the credit spending flow (`add-credit-spending-flow`).
- Catat (phase 11), the Rekening detail and `InvestmentTopupModal` (phase 8), the Analisis and Laporan pages (phases 6 and 7).
- Changing what a filter matches or how rows are paged, grouped or consolidated.

## Decisions

### D1. Page structure and breakpoints

At 1440 × 900 the frame is 1040 px (phase 2), so the list column is 808 px.

| Viewport | Layout |
| --- | --- |
| ≥ 1200 px | `.tx`: `grid-template-columns: 200px minmax(0, 1fr)`, gap 32, `align-items: start`. The rail is sticky at `top: calc(var(--topbar-h) + 24px)`. |
| 768–1199 px | One column. The rail is replaced by the type control (D6) above the toolbar, and the period mode is in Filter (D8). Rows keep the desktop one-line layout. |
| < 768 px | Phone: hero, two supporting figures, the type control, search and Filter, two-line rows (SPEC "Phone row"), dialogs as bottom sheets. |

- **The switches are viewport media queries** (`PHONE_QUERY` and a 1200 px query), shared with the shell. `useWidthAtMost(listRegionRef, 767)`, `PHONE_LIST_WIDTH` and `RESIZE_SETTLE_MS` leave Transaksi. Other users of `useWidthAtMost` (Kalender, Rekening, Laporan, Tinjau until phase 4) are not touched.
- **Why 1200 and not `styles.css`'s 1024 (deviation 1).** The full row needs time 40 + category 168 + pillar 112 + amount 120 + four 16 px gaps = 504 px before the title and meta. With the 200 px rail and the 32 px gap, the list column is 456 px at 1024, 532 px at 1100 and 632 px at 1200. So the prototype's own grid overflows or leaves no title below about 1176 px. SPEC §6 already names 1200 as the two-column breakpoint. With the 136 px pillar column of deviation 17 the row needs 528 px; at 1024 without the rail the list is 688 px and the title keeps 160 px, and at 1200 with the rail it keeps 104 px.
- **DOM order:** title block → hero → `.tx` (rail, then the list column: type control below 1200, toolbar, chips, selection bar, list). The rail is a `nav`. The list column is the shortcut region (D18).

Rejected:
- `styles.css`'s rail down to 1024 px: the rows overflow (arithmetic above), or the category column would have to vanish at a width where a laptop is normal.
- A container query on the list width: it is the mechanism that caused the phone feed on a laptop, and the page would change layout when the sidebar collapses.
- The rail stacked above the list (the prototype's 768–1023 rule): eight 36 px items push the first row about 400 px down. The type control carries the same choices in one 32 px line, as on the prototype's phone.

### D2. Title block and "…" menu — flagged: scope

- The title "Transaksi" and its subtitle come from phase 2 (`navigation.ts`, now `nav.*` keys).
- Actions through `PageActions`: the stepper (D3) when `period !== "all"`, and one "…" (`role="menu"`, 260 px, labelled "Tindakan lain"):
  1. "Gabungkan dua transaksi" (icon `swap`), or "Batal gabungkan" while selecting (D9);
  2. "Filter transaksi" (icon `filter`), which opens the dialog of D8, as the toolbar button does.
- **"Rutin" leaves the menu (flag: scope).** The prototype's menu has the two items above. Plans stay reachable from Target & Tagihan's "Rencana", the bell's due plans and Beranda's Terjadwal. `RecurringRulesModal` keeps its other users.

Rejected:
- Keeping "Rutin" (today): a third item that is not about transactions, and the same dialog is one step away elsewhere. If the owner rejects the flag, the fallback is a third item "Rencana" that opens the same `RecurringRulesModal`.
- A visible "Gabungkan" button: rarely used, and the prototype keeps it in "…".

### D3. Period stepper (shared)

`components/ui/PeriodStepper.tsx` is restyled to SPEC `.stepper`:
- a `div role="group"` labelled "Periode";
- two icon buttons (36 × 36, 44 × 44 on a phone) with `chevron-left` and `chevron-right` and their `common.period.*` names;
- the label 14/500, `min-width: 168px`, centered, `aria-live="polite"`;
- no box, border or fill.

Rules:
- **The label** is `common.period.cycleRange` ("Siklus 23 Okt – 24 Nov" / "Cycle 23 Oct – 24 Nov") or the month label from the language system. The range comes from the response's `summary.period.start` and `end` through `useFormat()`, as the language system's stepper does. While loading it reads `common.loading`.
- **Next is disabled at offset 0**, previous at the lower bound (−36), as today.
- **The reset stays (deviation 4).** After stepping back, a quiet small button ("Siklus ini" / "Bulan ini", h 28) follows the stepper. `monthly-navigation-stepper` requires it, and without it a far step back needs many presses.
- **Shared:** Analisis and Laporan use the same component, so their headers take the SPEC look now. Their pages are otherwise unchanged until phases 6 and 7, as phase 3 accepted for other shared drawing.

Rejected:
- A second, Transaksi-only stepper: two components for one control.
- Dropping the reset as in the prototype: removes required behaviour.

### D4. Hero and supporting figures — flagged: money presentation (rejected alternative)

Markup (prototype `section.hero` with flex): `HeroFigure` gains an `aside` slot and an optional `line`, and renders `.hero.is-split`:
- `display: flex; flex-wrap: wrap; align-items: flex-end; gap: 24px 48px; margin-bottom: 32px`;
- the figure part `flex: 1 1 320px`;
- the aside `flex: 1 1 320px` with `.supports` at `--n: 2` and `margin: 0`.

| Part | Content | Source |
| --- | --- | --- |
| Label (`t-label`, `h2`) | "Pengeluaran", "Pengeluaran · semua waktu" without a period, "Pengeluaran · {range}" for a `from`/`to` link | `terms.spending` |
| Figure (Manrope 44/52/600) | `spending`, or "Belum tersedia" in words for an older API | `summary.period` with a period or range, else `summary.cumulative`, as today |
| Line (14/20, muted), only when it applies | "sudah dikurangi penggantian {amount}" when `reimbursement_offset > 0`, else "tanpa {amount} yang ditabung" when `saving > 0` | today's caption (deviation 2) |
| Supporting 1 | "Masuk", `inflow` with "+" when above zero, in `--income` | `terms.income` |
| Supporting 2 | "Bersih · masuk − pengeluaran", `spending_net` with its sign ("+" above zero, "−" below), in the ink | `terms.net` (new) |

- **Supporting figures have no colour bar.** `FigureBlock` gains `bar={false}`, which leaves out the `sw-bar` slot, because the prototype's Transaksi blocks are `fig-block` with a `t-meta` label only. The legend on Beranda keeps its bars.
- **No colour mark above the hero label** (phase 4's optional `mark`).
- **Signs:** the "+" on Masuk and the sign on Bersih are presentation. The values are the server's, unchanged. The figure-parity check compares the numbers and lists the two sign changes as intended string differences.
- **Phone:** the hero, then the two figures side by side (`.supports` at 2 columns, gap 16). Below 360 px the hero is 32 px (SPEC).
- **Loading:** the label, a 44 px skeleton bar (240 px wide) and two 20 px skeleton figures, inside `aria-busy` (D13, deviation 3).
- **Hidden balances:** all three figures through `MaskedMoney`.
- **With the type "Tabungan & investasi" (D28):** the label is `terms.savedInvested` ("Ditabung & investasi"), the figure is the summary's `saving_net`, and the line is Beranda's "Tabungan bertambah {amount}" or "Tabungan berkurang {amount}" (`home` keys, reused). "Masuk" and "Bersih" are hidden, because they describe income and spending, not this list. When another row filter is active, `saving_net` is `null` and the figure reads "Tidak tersedia dengan filter lain" in words; no number is computed in the browser.

**Flag, rejected alternative (money presentation):** the prototype computes the hero as the sum of every expense row and "Bersih" as income less that sum. The app's hero is the server's `spending`, which leaves out saving-pillar expenses and nets applied reimbursements, and equals Beranda's "Pengeluaran" ("Ledger Spending Figure", "Equal to Beranda"). Taking the prototype's arithmetic would change the figure the owner reads and break that equality. It is rejected and recorded so it is not reopened.

Rejected:
- No line, as in the prototype: the figure would silently differ from the expense rows below it without saying why.
- Three `KpiCard`s (today): cards and dots that the prototype replaces with one hero.

### D5. Rail (≥ 1200 px)

Phase 4's `Rail`, `RailItem` and `RailRule` in a `nav` labelled "Jenis dan periode":
- `t-label` "JENIS", then six items: Semua, Pengeluaran, Masuk, Pindah saldo, Investasi, Tabungan & investasi (owner feedback 4, D28). They come from one ordered list (`LEDGER_TYPES` with its label key and count key), which `add-credit-spending-flow` extends with "Kredit" without changing the rail, the type control or the URL parser beyond the new value (D30). Each shows its count from `type_counts` (12/400 muted, right) with hidden text ("59 transaksi"). The counts are hidden while loading.
- `RailRule`, then `t-label` "PERIODE" and three items: Siklus gaji, Bulan kalender, Semua waktu.
- Items are buttons (h 36, padding 0 12, gap 12, radius 6, 14/500 muted). The chosen item has `aria-pressed="true"` and `aria-current="true"`, the `--s1` fill (the app's token of phase 1 D3) with `--ink`, and the straight 2 × 20 px bar 8 px left of the fill. `RailItem` gains a `pressed` prop for toggle buttons, because these items filter the list rather than navigate.
- A type item calls `updateFilters({ type })`. A period item calls `updateFilters({ period, offset: 0 })`, which still shares the period with Analisis (`setTimeframe`, `setCycleOffset`). Both keep focus on the pressed item.

Rejected:
- Links with `aria-current="page"`: the URL is written with `replaceSearch`, not navigated, and the items are toggles.
- Hiding zero counts: a 0 says the type is empty for the current filters.

### D6. Type control below 1200 px

The prototype's `.tx-types` (a `.seg` group labelled "Jenis", horizontal scroll, `margin-bottom: 12px`):
- six buttons from the same list as the rail, with `aria-pressed`, each "Semua <count>" with the count in muted 14/400;
- items h 30 from 768 to 1199 px and h 40 on a phone;
- the same handler as the rail.

The period mode below 1200 px is in the Filter dialog (D8). The stepper stays in the title block.

### D7. Toolbar and active filters

- **Toolbar** (`.toolbar`: flex, gap 8, wrap):
  - the search field: `.search` with the 16 px icon, `flex: 1`, `min-width: 200px`, `--s0` background and `--line-strong` edge, placeholder and accessible name "Cari catatan, merchant, kategori", `id = LEDGER_SEARCH_ID`, the 300 ms delay and `fokus=cari` as today;
  - the secondary button "Filter" with the `filter` icon, reading "Filter · 2" when filters are active, with the accessible name "Filter, 2 aktif".
- **Active chips** (`.chips`, gap 8, `margin-top: 12px`, a group labelled "Filter aktif"): SPEC chips (h 28, padding 0 4 0 12, 1 px `--line-strong`, radius 6, 12/500) with a 24 × 24 remove button "Hapus filter {label}". One chip per active filter: the account name, the category name or "Tanpa kategori", the pillar, the status, the source ("Dari HP", "Lewat Catat"), "Menunggu jawaban", "Kemungkinan duplikat", the range "1 Nov – 5 Nov" and the search text in quotes ("“kopi”").
  - Labels are the value only, as in the prototype; the remove button names it in full.
  - Removing a chip clears that filter only. Focus moves to the next chip, or to the search field when none is left.
  - The count on "Filter" leaves out the search text, as in the prototype.

Rejected:
- "Rekening: BCA" style labels (today): longer, and the remove button's name already says what it clears.
- The search outside the chips (today): the prototype shows it, and a chip is the one visible way to clear a search that came from a link.

### D8. Filter dialog — flagged: scope (two items)

`ShellDialog` "Filter transaksi", 640 px, a bottom sheet below 768 px. It edits a draft. "Terapkan" writes all filters with one `replaceSearch`, and "Batal", Esc and the close button discard the draft. Focus returns to the control that opened it.

| Field | Control | URL key |
| --- | --- | --- |
| Rekening | select: "Semua rekening" and `AccountSelectOptions` (parents selectable, credit included) | `account` |
| Kategori | select: "Semua kategori", "Tanpa kategori", then optgroups "Pengeluaran" and "Masuk" | `category` |
| Pilar | select: "Semua pilar", the five pillars, "Belum diklasifikasi" | `kakeibo` |
| Status | segmented radio group: Semua status · Perlu ditinjau · Belum dicek · Sudah dicek | `review` |
| Sumber | segmented radio group: Semua sumber · Dari HP · Lewat Catat | `source` |
| Menunggu jawaban | checkbox "Hanya yang menunggu jawaban di Tinjau" | `waiting` |
| Kemungkinan duplikat | checkbox, shown only while `dup=1` is set, so it can be cleared | `dup` |
| Periode | segmented radio group: Siklus gaji · Bulan kalender · Semua waktu (a change resets `offset`) | `period`, `offset` |

- **Footer:** the quiet "Atur ulang" at the left (clears every draft field except the period, as today's "Atur ulang filter"), then "Batal" (secondary) and "Terapkan" (primary).
- **Layout:** fields 24 px apart, labels 12/500 muted, selects h 40, segmented items h 30 (40 on a phone) and scrolling when too wide.

**Flag 1 (scope, filter behaviour).** Today the four chips (Menunggu jawaban, Belum dicek, Dari HP, Lewat Catat) are single-select (`chipOf`/`withChip`, `redesign-transactions-workspace` D2). Here they become three independent controls, so for example "Dari HP" with "Belum dicek" can be chosen together. The URL and `ledgerApiParams` already accept and send these combinations, so no request changes shape. The prototype has a Status segment and no chips. Fallback if rejected: one single-select segment "Tampilkan" with Semua · Menunggu jawaban · Belum dicek · Dari HP · Lewat Catat, mapped as `withChip` maps today.

**Flag 2 (differs from the plan text).** The plan lists "the period inside Filter" as removed. The prototype's dialog has "Periode", and below 1200 px, where the rail is hidden, it is the only control for the period mode. The default keeps it at every width. Fallback: show it only below 1200 px.

**Deviation 5:** one select for account, category and pillar instead of the prototype's checkbox lists. The URL and API take one value each. Multiple values would need a backend change.

Rejected:
- Applying each control at once (today's popover): with eight fields the list would reload several times, and the prototype's dialog applies once.
- A popover: the prototype uses a centered dialog.

### D9. Merge selection

- **Start:** "…" › "Gabungkan dua transaksi", or, on a phone, holding a row for 600 ms (today's `useHold`, kept, deviation 12). "Pindah saldo" in the edit dialog's Jenis control also starts it with that row selected (today's `onPickPair`).
- **Rows while selecting:** `ListRow`'s selectable mode renders the row as a `label` with a 16 px checkbox before the time (`accent-color: --accent-text`), so the whole row toggles it. Movement rows and a third row are disabled (`disabled` on the checkbox, 45 % opacity). Rows keep `data-row`. A checked row has `data-selected` (the `--s2` fill and the 2 px bar).
- **Selection bar** (`.sel-bar`: h 48, padding 0 12, `margin-top: 12px`, `--s1`, radius 6, region "Gabungkan dua transaksi"):
  - "{n} dari 2 dipilih" (`grow`);
  - the quiet "Batal gabungkan";
  - the primary "Gabungkan jadi pindah saldo", disabled until two rows are chosen.
  - While selecting it is sticky at `top: var(--topbar-h)` (56 px on a phone), and the day headings stick 60 px lower, so the action stays in view (deviation 6; "Consolidated Internal Movement Ledger Representation" item 9).
  - An error is a 12 px `--expense` line inside the bar's region with `role="alert"`.
- **Action:** unchanged. It fetches both details, requires one expense and one income, opens "Nominal tidak sama" when the amounts differ, rejects linked rows or one account, and posts `POST /movements/merge`. The toast reads "Digabung sebagai pindah saldo." with "Urungkan".
- **"Nominal tidak sama":** `ShellDialog` 480 px, one sentence naming both amounts, and a footer with the single primary "Tutup" that takes focus.
- **Copy change:** "Jadikan Pindah Saldo" becomes "Gabungkan jadi pindah saldo" (prototype), and the pick-pair toast says "Pilih pasangannya di daftar, lalu Gabungkan jadi pindah saldo."

Rejected:
- The bar inline as in the prototype: it scrolls away while the owner looks for the second row, and the spec asks for the action to stay reachable.
- A viewport-fixed bottom bar (today): it covers rows and the toast, and the prototype places the bar under the toolbar.

### D10. Day groups and headings

- `groupByDay` and the merged `daily_totals` are unchanged.
- Each group is `.day-group` (24 px apart) with phase 3's `DayHeading`: h 40, sticky under the top bar, `--s0`, 1 px bottom line, `h3` label 12/500 uppercase `--ink` ("KAMIS, 5 NOVEMBER · HARI INI", "RABU, 4 NOVEMBER · KEMARIN"). The date comes from `useFormat().longDate` and the relative day from `common`.
- **Totals** (12/400 `--muted`, right, ellipsis): "Masuk {amount} · Pengeluaran {amount} · Pindah saldo {amount} · {n} menunggu jawaban". Parts without a value are left out. Only the waiting count has a colour (`--warning`). Masuk is `inflow`, Pengeluaran `spending`, Pindah saldo `movements` and the count `waiting_count`.
- **Order:** the prototype's (Masuk first). Today Pengeluaran comes first.
- **Colour:** the amounts become muted. "Ledger Day Heading Colours" is removed (D25), because the prototype colours no day total and the kind is named in words.
- On the phone the heading sticks at 56 px. If an English heading overflows at 390 px, the weekday may shorten (`longDate` short weekday), as the language research allows.

Rejected:
- Coloured day amounts (today): five colours in one muted line, against SPEC "Day heading".

### D11. Rows — flagged: correction of the plan (duplicate badge)

The shared `ListRow`, full variant (`.row`, min-h 56, padding 0 12 with the −12 bleed, gap 16, 1 px bottom line; hover `--s1`, pressed `--s2`, selected `--s2` plus the 2 px bar; neutral focus outline inset 2 px). Each row is one `button` that opens the dialog. Enter opens it.

| Column | Content |
| --- | --- |
| `r-time` 40 | the time, `useFormat().time` ("12.37" / "12.37" in `en-ID`, "12:37 PM" in `en-US`) |
| `r-title` | `rowTitle`: the merchant, notes, category or "Tanpa keterangan". A transfer or top-up is titled by its note (owner feedback 2), and by its route "Jago · Tabungan → Jago · Bayar Kredit" ("?" for the unknown side) when it has no note. A top-up without a note reads "Top up {product}", or "Top up Investasi" when the product is not known (deviation 11) |
| `r-meta` (desktop) | the account with its bank. A transfer or top-up titled by its note shows its route here; one titled by its route shows nothing more. An unpaired leg adds "Pasangan belum ditemukan" in `--warning`. In a list filtered to one account, the meta is "Masuk dari …" or "Keluar ke …" (today's `movementDirection`). A credit purchase keeps its provider account here (D30) |
| `r-cat` 168 | the category name; "Pindah saldo" for a transfer; "Top up investasi" for a top-up; "Belum ada kategori" for a waiting row without one; "Tanpa kategori" otherwise |
| `r-pillar` 136 | an 8 px dot and the pillar name (the unclassified dot dashed, "Belum diklasifikasi"); "Menunggu jawaban" in `--warning` for a waiting row; "Uang masuk" for income; for a transfer or top-up, the saving direction of D27 ("Ditabung", "Diambil dari tabungan", "Investasi", "Bayar utang") as text without a dot, or "—" (owner feedback 3; "Tidak dihitung" is no longer used) |
| `r-amt` ≥ 120 | expense "−Rp 30.000" in `--out-fg`, the expense red (owner feedback 1, D29); income "+Rp 18.400" in `--income`; transfer "Rp 200.000" in `--transfer`; top-up "Rp 112.590" in `--invest`; all `t-amt` (14/600 tabular) through `MaskedMoney` |

- **The movement note:** `notes` as stored, whether the owner wrote it or the server generated it ("Pindah saldo ke GoPay"). The server's placeholder for "no note" (`"Internal Movement"`, and the English default `localize-server-text` writes, "Transfer") counts as no note, so it never becomes a title. The placeholder list is one constant in `rows.tsx`, with a test that reads the backend default in `routers/movements.py`, so the two cannot drift.

- **Pillar column width (deviation 17):** 136 px instead of SPEC's 112, in the shared row, so the owner's saving-direction labels fit on one line in both languages. The full row still needs 528 px plus the title, which D1's 1200 px breakpoint leaves room for.
- **Phone (< 768 px):** two lines (min-h 64): the title, then "{time} · {status}", using `activityPhoneStatus` with the waiting text in `--warning`. A transfer titled by its note shows "{time} · {route}" (ellipsis), and one titled by its route shows "{time} · {saving direction}" or "{time} · Pindah saldo". The amount is at the right. Time, category and pillar columns are hidden.
- **Removed from rows:** the avatar, the type pill, the Jenis column, the second line with the account, " · belum dicek" (the check state is in the dialog and the Status filter), coloured expense amounts and the transfer's `repeat` icon.
- **The expense colour (known issue, settled by owner feedback 1):** the expense red `--out-fg`, on Transaksi and Beranda alike. Transaksi, Beranda (Aktivitas terbaru and Perlu tindakan) and the Rekening detail now use one `RowAmount`: the SPEC variant moves from `ActivitySection`'s `ActivityAmount`, with the expense class changed to `c-expense`, and replaces the old `RowAmount` and Perlu tindakan's own amount (D22, D29).
- **Beranda follows:** Aktivitas terbaru reads the same helpers, so its transfers also show the note first and the saving direction instead of "Tidak dihitung".
- **Shared helpers:** `rowKind`, `rowTitle`, `rowStatus`, `rowCategory`, `movementRoute`, `movementDirection`, `activityMeta`, `activityPhoneStatus`, `RowAmount` and `RowStatus` move from `components/beranda/activity.tsx` to `components/transactions/rows.tsx`. Their keys move with Indonesian unchanged (D20). Beranda imports them from there.
- **Consolidation unchanged:** one row per movement (`consolidateLedgerMovements`, `listsLogicalMovements`), inferred pairs stay one row, and account views list each leg with its direction.
- **Selection:** the row whose dialog is open has `data-selected` (today's `data-open`).

**Flag (correction of the plan):** the plan expects a "Kemungkinan duplikat" badge on rows (12/500, `--s2`, `--warning`). The row payload carries no duplicate flag. Duplicates are a filter (`dup=1`, `possible_duplicates=true`) that Laporan links to. So no badge is drawn, and "Kemungkinan duplikat" stays a chip and a dialog checkbox (D7, D8). A row flag would be another backend field, outside the owner's feedback, and is not added.

Rejected:
- The prototype's "Tidak dihitung" for income, transfers and top-ups: income is counted in Masuk, and the owner asked for the saving direction on transfers (feedback 3).
- The route as a transfer's title (prototype): the owner asked for the note first (feedback 2).
- The ink for expenses (prototype, phase 3): the owner asked for red (feedback 1).
- A table (today): the whole row cannot be one button inside a table row, and the prototype has no header row. Each row button's accessible name reads its cells in order.
- Keeping the Beranda helpers in `components/beranda`: Transaksi, Kalender, the dialog and Rekening import them. One shared home avoids a second copy.

### D12. Loading more and the end of the list

- Pages of 50 and the `offset` paging are unchanged.
- Inside a day with unloaded rows, a centered quiet button "{n} lagi di hari ini · Muat lagi" (`.list-foot`) loads the next page. It is a whole-width row, which fixes the phone wrap.
- After the last group: the secondary "Muat {k} lagi" (`k = min(50, total − loaded)`), or "Memuat…" while loading. When every row is loaded, the muted line "{total} transaksi · akhir daftar" appears.
- The polite live message "{n} transaksi lagi dimuat." is kept. Focus moves to the first new row when the last page arrives, as today.
- A failed page keeps the loaded rows and shows "Gagal memuat transaksi berikutnya." with "Coba lagi" beside the button.

### D13. Loading, failure and empty states

- **Loading:** six SPEC skeleton rows (56 h: a 40 × 12 bar, a 30–52 % bar and a 96 × 12 bar at the right, `--s2`, radius 6, no pulse) in a `role="status"` with `aria-busy="true"` and the name "Memuat transaksi". The hero shows skeleton figures, and the rail and type control hide their counts (deviation 3).
- **Failure** (first page): no hero figures, and the SPEC error box in the list column: `alert` icon 20, "Transaksi belum bisa dimuat" (14/500), "Server tidak menjawab. Data yang tersimpan tetap aman." (12/400) and the secondary "Coba lagi", `role="alert"`.
- **Empty** (SPEC empty, padding 48 24, centered):
  - no row at all, with no filter, search or period: "Belum ada transaksi", "Transaksi dari HP dan yang dicatat manual muncul di sini." and the secondary "Catat transaksi", which opens Catat;
  - otherwise: "Tidak ada transaksi yang cocok", "Ubah pencarian atau filter." and "Hapus semua filter" when a chip is active (it keeps the period, as today's `clearAll`), or "Catat transaksi" when none is.
- **The list heading:** a visually hidden `h2` "Daftar transaksi, {total} transaksi" stays as the focus target after a delete (D18).

### D14. Transaction dialog — flagged: correction of the plan (delete undo)

`TransactionDrawer` becomes `TransactionDialog` on `ShellDialog`, 560 px (`dialogs.js`; the plan's 480 is superseded, deviation 13), a bottom sheet below 768 px. Transaksi, Beranda, Kalender and Peta Aliran open it with the same props. The `Drawer` primitive keeps its other users (the due-plans view, Peta Aliran's node detail).

- **Header:** the title `rowTitle` (Manrope 20/600), so a transfer with a note is titled by its note and its route is in "Dari" and "Ke". The meta line is "{longDate} pukul {time}", plus " · {category}" for a categorized expense ("Rabu, 4 November pukul 09.12" / "Wednesday, 4 November at 09.12").
- **Amount:** "Jumlah" (12/400 muted), then the amount in `t-fig-l` (32/40/600) with the row's sign and colour, so an expense is "−Rp 120.000" in the expense red (D29).
- **Label and value list** (`.dl`: `160px minmax(0,1fr)`, gap 12 16; `dt` 12/20 muted; one column on a phone):

| Kind | Rows, in order |
| --- | --- |
| Expense, income | Jenis (Pengeluaran / Masuk) · Rekening · Kategori ("Menunggu jawaban" in `--warning` for a waiting row without one, else the name or "Tanpa kategori") · Kakeibo (expense: the pillar or "Belum diklasifikasi") · Merchant ("—" when none) · Biaya untuk (a fee row: `common.rowMarks.transferFeeRoute`) · Catatan ("—") · Dicatat · Diklasifikasikan · Kebiasaan (when habit-recorded) · Target (when linked) · Tagihan (one row per allocation, "{name} · {amount}") · Dicek · Bukti |
| Transfer, top-up | Dari · Ke ("Pasangan belum ditemukan" in `--warning` when unknown) · Biaya (when `movement_fee`: "{amount} · Lihat transaksi biaya", a link button that opens the fee row in the same dialog) · Status pasangan ("Lengkap" or "Pasangan belum ditemukan", deviation 10) · Tabungan & investasi (the D27 label, with "—" when the move changes nothing) · Catatan ("—") · Dicatat · Diklasifikasikan · Dicek |

  - "Dicatat" keeps today's provenance wording (`sourceLabel`, deviation 9). "Diklasifikasikan" uses `classifierLabel`, or "—".
  - "Dicek" reads "Sudah dicek", or "Belum dicek · Tandai sudah dicek" with a link button when `checkable` (automatic rows, not waiting). A waiting row reads "Belum dicek" without the link.
  - "Bukti" reads "Tersimpan" when `receipt_path` is set, else "Belum ada bukti". There is no preview, because the API has no receipt read endpoint.
- **Original text:** the `.raw` box from phase 4, captioned "Notifikasi asli · {app}" or "Teks yang diketik", with the text as stored and no `lang` attribute.
- **Waiting box:** a waiting row shows `.box` (1 px `--line`, radius 12, padding 16) with the `help` icon 20 in `--warning`, "Transaksi ini menunggu jawaban di Tinjau." and the secondary link "Jawab di Tinjau" to `/review?item=<id>`.
- **Footer:**
  - at rest: the quiet destructive "Hapus" (`trash`, `--expense` text) at the left, then the secondary "Ubah" (`edit`) at the right;
  - while confirming a delete: "Hapus transaksi {amount}?" (14/400, `grow`), "Batal" (secondary, focused) and "Hapus" (`--danger` fill). The body gains a `.box` above the footer listing today's `deleteEffects` (balances, debts, goals, the top-up) and "Penghapusan tidak dapat dibatalkan." (deviation 8).
- **States:** loading shows two skeleton blocks with `aria-busy`. A missing row shows the title "Transaksi tidak ditemukan" and "Transaksi ini sudah tidak ada. Daftarnya sudah dimuat ulang.", and the lists reload. A failure shows the error box with "Coba lagi".
- **Requests, unchanged:** `GET /transactions/{id}`; "Tandai sudah dicek" → `POST /review/bulk {action: "accept"}` with the Urungkan toast "Ditandai sudah dicek."; delete through `deletePath` (top-up, movement or transaction path), then the toast "Transaksi dihapus." and the list refresh.
- **Focus and history:** see D18 and D19.

**Flag (correction of the plan, money behaviour):** the plan's "Must not change" lists "delete with Urungkan". Delete has no undo today, and the prototype's undo toast after a delete is not built. Restoring a deleted row, its balances, debts and goals would be a new money operation and a backend change. Delete keeps its confirmation, effects list and "Penghapusan tidak dapat dibatalkan.".

Rejected:
- Keeping the `Drawer` look: the prototype's dialog parts (`.dl`, footer actions) replace it, and one dialog primitive serves every page.
- The prototype's one-sentence delete confirmation: it drops the effects that "Delete asks first" requires to be named.

### D15. "Ubah transaksi" dialog

"Ubah" opens a second `ShellDialog` in place of the detail ("Ubah transaksi", meta = the row's title, 600 px; a bottom sheet on a phone). The detail closes and the edit opens on the same `?edit=` URL, without a new URL parameter.
- **"Batal", Esc and the close button** return to the detail, with focus on "Ubah". **"Simpan"** sends the request, returns to the detail with "Perubahan disimpan." and refreshes the lists.
- **Fields** (`.form-grid` 2 columns, gap 16 24; one column on a phone), all from `TransactionEditForm` with today's rules and the same `PATCH /transactions/{id}` and receipt upload:
  - Jenis: a segmented radio group: Pengeluaran · Masuk · Pindah saldo · Top up Investasi (the last only when eligible, with the reason when not). "Pindah saldo" closes the dialog and starts the merge with this row (D9). "Top up Investasi" opens D17.
  - Nominal (IDR), read-only as "Total bayar (IDR)" when debt allocations set it · Tanggal dan waktu (with seconds);
  - Bayar dari / Masuk ke · Kategori;
  - Mengurangi pengeluaran (a reimbursement income, as today);
  - Pilar Kakeibo: a segmented radio group with the five pillars, none checked when unclassified, and one hint line under it: the chosen pillar's help, or "Pilih pilar agar transaksi ini masuk ke ringkasan Kakeibo." This replaces the `InfoHelp` icon.
  - Kaitkan ke target (opsional) · Bayar tagihan / utang (opsional) with today's multi-debt allocation rows and checks;
  - Merchant atau tempat (opsional) · Catatan;
  - Ganti atau tambahkan bukti, with today's note and the receipt retry.
- **Footer:** "Batal" (secondary) and "Simpan" (primary, "Menyimpan…" while pending). An error is shown above the form as `role="alert"`, and focus moves to the first invalid field.

Rejected:
- The inline edit inside the detail (today): the prototype uses its own dialog, and the detail stays readable.
- A URL parameter for the edit mode: a new URL contract for a transient state.

### D16. Movement edit (the Transaksi path of the English error)

"Ubah" on a transfer opens the same dialog in movement mode ("Ubah pindah saldo", meta = the route):
- no Jenis control;
- fields: Nominal (IDR), Tanggal dan waktu (seconds), Dari, Ke (the liquid and credit accounts `InternalMovementModal` offers today), Catatan;
- footer: the quiet "Pisahkan jadi dua transaksi" at the left, which confirms in the footer ("Pisahkan jadi dua transaksi? Kedua catatan tetap ada." with "Batal" and "Ya, pisahkan"), then "Batal" and "Simpan".
- **Requests unchanged:** `PATCH /movements/{id}` (both legs atomically) and `POST /movements/{id}/split`. Delete stays in the detail's footer (D14), so the duplicate delete inside `InternalMovementModal` goes.
- **Validation unchanged:** `movementFormSchema` (its messages become keys) and the credit-on-both-sides check.
- **Server errors through codes.** "Source and target accounts must be different" from `PATCH /movements/{id}` (and `ledger_mutations`) arrives as `{code, params, message}` after `localize-server-text`. The dialog renders `errors.<code>` in the interface language and falls back to `message`. It never shows a raw English string in an Indonesian interface. A test plants the code's response and asserts both languages.
- **Ownership:** the same server error on Catat's Perpindahan (`POST /movements`) is phase 11. The code itself belongs to `localize-server-text`. If that change gave the error no code (a gap), phase 5 stops and reports instead of mapping English text in the web.
- `InternalMovementModal.tsx` and its test are removed: the transaction dialog was its only user.
- A top-up row's "Ubah" still opens `InvestmentTopupModal` (Rekening's, phase 8). In an English interface it carries `lang="id"` and the untranslated notice in its header, per `add-language-system` D10.

Rejected:
- Keeping `InternalMovementModal`: a second dialog style for one action, the source of the untranslated error, and a duplicate delete.
- Mapping the English message text to Indonesian in the web: fragile, and the code contract exists.

### D17. "Catat sebagai top up investasi"

`ExpenseTopupConversionModal`, opened only from the edit dialog's Jenis control, moves onto `ShellDialog` (520 px) with SPEC fields: the product select, the amount line and the explanation as one hint, and "Batal" / "Catat sebagai top up". The conversion request, its eligibility and the Urungkan toast "Dicatat sebagai top up investasi." are unchanged. Its copy moves to `transactions.topupConversion`.

### D18. Keys and focus

- **Keys:** J and K move focus between rows (`[data-row]` inside the list column), across day headings. Enter opens the dialog, and Z undoes the newest check, conversion or merge. They are scoped to the list region, skipped while typing, listed in "Bantuan & pintasan" under "Di Transaksi", and follow the off switch, as today. While selecting, J and K move between the checkboxes and Space toggles.
- **Focus:**
  - a dialog takes focus on its first control;
  - closing returns focus to the row (`data-row`) after the list settles;
  - closing the edit returns focus to "Ubah";
  - after a delete, focus moves to the next row, the previous one, or the hidden list heading;
  - after "Terapkan", focus returns to "Filter";
  - after removing a chip, focus moves to the next chip or the search field.
- **Not obscured (2.4.11):** `scroll-padding-top` covers the top bar, a sticky day heading, and the selection bar while selecting.

### D19. URL, deep links and history

Unchanged:
- every filter key of `lib/ledgerFilters.ts`, parsed and written as today, with invalid values falling back;
- `edit=<uuid>`: it opens the dialog on load, and an invalid id is ignored;
- `return=review&folder=<valid>`: closing goes to `/review?item=<id>&folder=…`, and a tampered value stays on the ledger;
- `fokus=cari`: it focuses the search once and drops the parameter;
- on a phone, opening a row pushes a history entry (`ledgerSheet`), so back closes the sheet. Above 768 px the URL is replaced.

Links into Transaksi from Beranda (Kakeibo rows, "Semua transaksi", Rincian), Analisis (category, Kalender day, Peta Aliran node), Laporan (`from`/`to`, `q`, `dup=1`), Tinjau (`edit`, `return`) and the Rekening balance check (`edit`) are listed in the gate's deep-link scenarios (D24).

### D20. Language

- **Namespace `transactions`** in `messages/en.json` and `id.json`, with identical key trees. Sub-namespaces: `page`, `hero`, `rail`, `types`, `periods`, `toolbar`, `chips`, `filter`, `merge`, `list`, `day`, `row`, `more`, `states`, `dialog`, `detail`, `edit`, `movement`, `topupConversion`, `delete`, `toast`, `announce`, `keys`. Appendix A lists the keys with both languages.
- **Terms:**
  - figure and concept names come from `terms` (`spending`, `income`, `transfer`, `investments`, `investmentTopUp`, `payCycle`, the pillars, `noPillarYet`, `noCategory`, `noDescription`);
  - this phase adds `terms.net` ("Bersih" / "Net"), `terms.calendarMonth` ("Bulan kalender" / "Calendar month") and `terms.savingsAndInvestments` ("Tabungan & investasi" / "Savings & investments"), the type that `terms.savedInvested` ("Ditabung & investasi") sums;
  - the saving-direction labels are `transactions.flow.*`: "Ditabung" / "Saved", "Diambil dari tabungan" / "Taken from savings", "Investasi" / "Invested", "Bayar utang" / "Debt payment" (owner feedback 3);
  - `nav.ledger` gives the title.
- **Shared row keys:** the row helpers' strings that the language system put under `home` (`statusIncome`, `statusWaiting`, `inFrom`, `outTo`, `pairNotFound`) move to `transactions.row` with Indonesian unchanged; `statusNotCounted` is removed with its last use, since the helpers move to `components/transactions/rows.tsx` (D11). Beranda reads them from there.
- **Dead keys:** `common.rowMarks.needsReview` and `common.rowMarks.automatic` go with the dead `ReviewStatusMark`. The fee keys stay for the dialog.
- **Indonesian:** today's text for every existing string, with these intended changes, listed in `verification.md`:
  - "Gabungkan jadi pindah saldo" (was "Jadikan Pindah Saldo");
  - "Filter transaksi" in the menu;
  - "Tidak ada transaksi yang cocok", "Ubah pencarian atau filter." and "Hapus semua filter" (were "Tidak ditemukan" and "Hapus filter");
  - "Belum ada transaksi" and its line;
  - "{total} transaksi · akhir daftar";
  - "Bersih · masuk − pengeluaran" as one label (was "Bersih" with the caption "masuk − pengeluaran");
  - "Status pasangan", "Lengkap", "Lihat transaksi biaya", "Tersimpan", "Belum ada bukti";
  - "Ubah pindah saldo", "Pisahkan jadi dua transaksi";
  - "Sumber", "Semua sumber" and "Hanya yang menunggu jawaban di Tinjau";
  - the chip labels without their "Rekening:", "Kategori:", "Pilar:" and "Rentang:" prefixes.
  New strings follow the neutral register: no "kamu" or "Anda", no slang, and "Pengeluaran" for spending.
- **English:** plain and firm, US spelling, sentence case, "Spending" only. No "you/your" in labels. "Expense", "Spent", "Outflow", "Inflow" and "Ledger" never appear.
- **Whole ICU sentences:** the 27 interpolated Transaksi strings become messages with named arguments. Counts use `plural` (English `one`/`other`, Indonesian `other`). Amounts, dates, times and ranges arrive formatted from `lib/format.ts` (`money`, `longDate`, `shortDate`, `time`, `dateRange`). The amount signs "−" and "+" are added by `RowAmount` and the hero, never inside a message.
- **Formatting:** the `id-ID` formatters in `app/ledger/page.tsx` (`SHORT_DATE`), `LedgerList.tsx` (`TIME`), `TransactionDrawer.tsx` (`WHEN`) and the row helpers (`JAKARTA_TIME`, `SHORT_DAY`) move to `useFormat()`. `jakartaDay` (an `en-CA` day key, not display) stays.
- **Server text:** error responses render from `errors.<code>`. The merge's client errors ("Pilih satu transaksi keluar dan satu transaksi masuk.", "Pilih transaksi yang belum tertaut dari dua rekening berbeda.") and the movement schema messages become `transactions.*` keys. Merchant, notes, account and category names, the notification and typed text, and stored notes are user data: shown as stored, with no `lang` attribute.
- **Registry and guards:**
  - `/ledger` joins `lib/i18n/translatedRoutes.ts`, so the untranslated notice and `lang="id"` leave Transaksi and its dialogs;
  - the untranslated-page test example, which phase 4 moved to Transaksi, moves to Analisis (`/insights`), which stays untranslated until phase 6;
  - the copy register's translated roots gain `app/ledger`, `components/transactions`, `components/ui/ExpenseTopupConversionModal.tsx`, `lib/ledgerFilters.ts`, `lib/ledgerDays.ts` and `lib/movementFormSchema.ts`;
  - catalogue parity covers `transactions`.
- **Lengths** (measured in both languages at 1440, 1100, 1024, 390 and 320 px):
  - the pillar column: "Menunggu jawaban" (16 characters) and the English "Needs an answer" (15). "Waiting for an answer" (21) would overflow, so the shorter glossary form is used here. The owner's "Diambil dari tabungan" (21) and "Taken from savings" (18) need about 128 px at 12/400, so the shared pillar column widens from SPEC's 112 to 136 px on Transaksi and Beranda (deviation 17). At 1024 px the title keeps 160 px;
  - the 168 px category column: "No category yet", "Investment top-up";
  - the rail: "Calendar month", "Investments";
  - the type control: "Spending 43" scrolls inside its box on the phone;
  - the day heading: "WEDNESDAY, 4 NOVEMBER · YESTERDAY" with totals that ellipsize.

Rejected:
- Leaving the row keys in `home`: Transaksi would read Beranda's namespace for its own rows.
- English that follows the prototype's Indonesian word order: messages are whole sentences per language.

### D21. Hidden balances

With "Sembunyikan saldo", every Transaksi amount renders through `MaskedMoney` as "Rp ••••••" (or "$ ••••••") with the accessible name `common.hidden`. That covers the hero, supporting figures, day totals, row amounts, the dialog's amount, fee, allocations and delete question, the merge mismatch dialog, and the conversion dialog. Edit fields keep the values the owner types, because they are inputs.

### D22. Shared components and CSS

**Reused as they are:** `ShellDialog`, `PageActions`, `DayHeading`, `MaskedMoney`, `Rail`/`RailRule` (phase 4), the `.raw` box (phase 4), `Segmented` (phase 1 `.seg`), `EmptyState`, `ErrorState`, `Skeleton`.

**Extended:**
- `ListRow`: `selectable` (renders a `label` with a checkbox, `checked`, `disabled`, `onToggle`) for the merge. It uses phase 4's `rowKey` (`data-row`) and `selected`. Phase 4's stacked layout gets no Transaksi user: search results are ordinary rows. Its users stay Tinjau and, later, Target.
- `RailItem`: `pressed` (toggle buttons; the same look as `current`).
- `HeroFigure`: `aside` and an optional `line` (`.hero.is-split`).
- `FigureBlock`: `bar={false}`.
- `PeriodStepper`: the SPEC look (D3).
- `RowAmount` and `RowStatus`: one version in `components/transactions/rows.tsx`, used by Beranda (Aktivitas terbaru and Perlu tindakan), Transaksi, the dialog and the Rekening detail (`AccountDetail.tsx`). Its expense variant is `c-expense` (`--out-fg`), by owner feedback (D29). `RowStatus` renders the pillar or, for a transfer, the D27 label.
- The pillar column (`.r-pillar`): 136 px (deviation 17).

**New:** `components/transactions/TransactionDialog.tsx`, `TransactionEditDialog.tsx` (with `MovementEditFields.tsx`), `LedgerFilterDialog.tsx`, `rows.tsx`; `app/ledger/` parts `LedgerHero`, `LedgerRail`, `LedgerToolbar` and `MergeBar`. CSS from `styles.css` with the phase 1 token names: `.tx`, `.tx-rail`, `.tx-types`, `.toolbar`, `.search`, `.chips`, `.chip`, `.sel-bar`, `.stepper`, `.dl`, `.box`, `.form-grid` (if phase 1 has none), `.hero.is-split`, and `.c-expense { color: var(--out-fg) }` beside the existing `.c-income`. The load-more row reuses Beranda's `.list-foot`, because the app's `.more-row` already names the Lainnya sheet row.

**Removed** (no user remains, checked with `rg` in task 5.11):
- `KpiCard` (`primitives.tsx`) and `.ws-kcards`, `.ws-kcard*`;
- `LedgerTable`, `LedgerFeed`, `MoreOfDay`, `DayTotals`, `subLine`, `feedStatus` and the old `SelectBox` (the hold logic moves to the row);
- both `KIND_PILL` maps, `RowAvatar` and `AVATAR`; `.ws-ava` and `.ws-tone-*` once Tinjau (phase 4) has also dropped them;
- the old coral `RowAmount` and `ActivityAmount`, merged into one;
- `FilterPopover`, `LEDGER_CHIPS`, `chipOf`, `withChip` (and their tests, rewritten for the dialog);
- `components/ledger/LedgerRowMarks.tsx`: `ReviewStatusMark` and `HabitMark` have no user, and `feeLinkText` is replaced by the dialog's fee rows. The `FeeLinkFields` type moves to `components/transactions/types.ts`;
- `InternalMovementModal.tsx` and `InternalMovementModal.test.tsx`;
- `TransactionDrawer.tsx` (renamed and rebuilt) and the inline `TransactionEditForm` mode;
- `PHONE_LIST_WIDTH`, `RESIZE_SETTLE_MS` and Transaksi's `useWidthAtMost`;
- CSS: `.ws-tx-toolbar`, `.ws-tx-tabs`, `.ws-tx-search`, `.ws-tx-more`, `.ws-tx-filter`, `.ws-tx-chips`, `.ws-tx-chip-static`, `.ws-tx-chip-x`, `.ws-tx-tbl`, `.ws-tx-col-*`, `.ws-tx-category`, `.ws-tx-day*`, `.ws-tx-dname`, `.ws-tx-daytot`, `.ws-tx-waiting`, `.ws-tx-feed*`, `.ws-tx-row`, `.ws-tx-check`, `.ws-tx-loadmore`, `.ws-tx-selectbar`, `.ws-tx-amount*`, `.ws-tx-facts`, `.ws-tx-quote`, `.ws-tx-confirm`, `.ws-tx-danger`, `.ws-tx-kind`, `.ws-tx-edit`, and `.ws-kk`, `.ws-ph-list`, `.ws-ph-row`, `.ws-amt-*` where no user remains.

**Kept:**
- `.ws-tx-error` and `.ws-tx-actions` (used by `CatatDialog`, until phase 11);
- `Pill`, `Disclosure`, `Modal`, `Drawer`, `useWidthAtMost`, `.ws-chip`, `.ws-tbl`, `.ws-who` and `.ws-seg`, because other pages still use them;
- `RecurringRulesModal`.

### D23. What happens to each removed element

| Removed | Destination |
| --- | --- |
| Three KPI cards | Hero "Pengeluaran" and two supporting figures (D4) |
| KPI captions | The hero's one line (D4); "masuk − pengeluaran" in Bersih's label |
| Type tabs in the toolbar | Rail JENIS (≥ 1200), type control (< 1200) (D5, D6) |
| Period segment in the popover | Rail PERIODE (≥ 1200), Filter › Periode (all widths), stepper (D3, D5, D8) |
| Filter popover and four chips | Filter dialog (D8) |
| "…" › Rutin | Target & Tagihan › Rencana, the bell, Beranda (D2, flagged) |
| "Daftar transaksi · N transaksi" heading | Hidden heading; "N transaksi · akhir daftar"; the rail's Semua count (D13, D12) |
| Table header, Jenis column, type pills | The amount's sign and colour, the category column's words (D11) |
| Avatars, second line with the account | The meta beside the title (D11) |
| " · belum dicek" on rows | The dialog's "Dicek" row and the Status filter (D11, D14) |
| "Tidak dihitung" on transfers and top-ups | The saving direction of D27, or "—" (owner feedback 3) |
| The route as a transfer's title | The note as title, the route as meta (owner feedback 2) |
| Coloured day totals | Muted totals with the waiting count in warning (D10); expense amounts stay red by owner feedback (D29) |
| Viewport-fixed merge bar | Sticky SPEC selection bar under the toolbar (D9) |
| Drawer with pill and banner | Transaction dialog with the `.box` Tinjau note (D14) |
| Inline edit form, `InfoHelp` icons | "Ubah transaksi" dialog with one-line hints (D15) |
| `InternalMovementModal` | The edit dialog's movement mode (D16) |
| Dead `ReviewStatusMark`, `HabitMark`, `feeLinkText` | None needed; fee rows in the dialog (D14) |

### D24. Verification tooling and gate

In `scripts/verify/markas/` (local only):
- **Scenarios** (`scenarios-app.json`, `scenarios-prototype.json`), each in `en` and `id` for the app:
  - `transaksi` (cycle, Semua), `transaksi-pengeluaran`, `transaksi-bulan`, `transaksi-semua-waktu`, `transaksi-rekening` (one account, movement directions), `transaksi-cari` (search chip), `transaksi-filter-aktif` (three chips), `transaksi-gabung` (selection with one row chosen);
  - `state-transaksi-memuat`, `state-transaksi-kosong`, `state-transaksi-tidak-cocok`, `state-transaksi-galat`;
  - `dlg-menu-transaksi`, `dlg-filter`, `dlg-transaksi-detail` (waiting expense), `dlg-transaksi-detail-masuk`, `dlg-transaksi-perpindahan` (with a fee), `dlg-transaksi-topup`, `dlg-transaksi-hapus` (delete confirmation), `dlg-transaksi-ubah`, `dlg-transaksi-ubah-perpindahan`, `dlg-catat-topup`, `dlg-nominal-beda`;
  - the deep links: `?edit=`, `?edit=&return=review&folder=jawab`, `?fokus=cari`, `?type=expense&kakeibo=unclassified`, `?from=&to=&q=`, `?dup=1`, `?waiting=1`, `?period=month&offset=-1`.
- **`transaksi-checklist.js` for `measure.js`**, generated from D1 to D15 and SPEC §2, §5 to §7 and §13:
  - `.tx` columns and gap at 1440 (200 / 808, 32), the rail sticky top 80, one column at 1199, 1100 and 1024 with the type control;
  - the first content `y = 192`; the hero flex gap 24 48 and margin-bottom 32; one 44 px figure and no other ≥ 33 px (the dialog's 32 px amount passes);
  - rail item 36, padding 12, radius 6, the bar 2 × 20 at −8/8, the rule 1 px;
  - toolbar: search h 36 and min-width 200, Filter h 36; chip h 28, padding 0 4 0 12, radius 6, remove 24;
  - selection bar h 48, padding 0 12, radius 6;
  - day heading h 40 and sticky top 56;
  - row min-h 56 (64 on a phone), padding 0 12, gap 16, time 40, category 168, pillar 136 (deviation 17), amount ≥ 120;
  - the amount colour per kind in both themes;
  - dialog widths 560, 600, 640, 520 and 480, radius 16, header 24 24 16, body 0 24 24, footer 16 24; `.dl` 160 px; the sheet at 390 px;
  - stepper buttons 36 and the label min-width 168;
  - sizes ⊆ {12, 14, 16, 20, 24, 32, 44}, weights ⊆ {400, 500, 600}, ≤ 8 text styles;
  - no border ≥ 2 px and no coloured border except the bars and focus;
  - no horizontal overflow at 1440, 1280, 1200, 1199, 1100, 1024, 768, 767, 390 and 320.
- **`transaksi-keys.mjs`**, the keyboard flows:
  1. tab order: skip link, sidebar, top bar, stepper, "…", rail, toolbar, chips, the first row;
  2. J/K across a day heading, Enter opens, Esc closes with focus on the row;
  3. "j" typed in the search field;
  4. the rail and the type control by keyboard;
  5. "…" with arrows and Esc;
  6. Filter: Tab stays inside, a segment by arrows, "Terapkan", focus back on "Filter";
  7. removing a chip by keyboard;
  8. merge by keyboard: "…" › Gabungkan, Space on two checkboxes, the action, Z;
  9. detail → Ubah → Batal (focus on "Ubah") → Ubah → Simpan;
  10. "Tandai sudah dicek", then Z;
  11. delete: "Hapus", "Batal" focused, Esc;
  12. the phone sheet: open, back closes with focus on the row;
  13. the `return=review` deep link;
  14. the single-key switch off;
  15. no focused element under the top bar, a day heading or the selection bar.
- **Parity seed (`seed_parity.py`)** gains, from synthetic data without double booking:
  - a transfer with a linked fee row;
  - a converted top-up;
  - a reimbursement income with an offset;
  - a habit-recorded row;
  - a row with a receipt path;
  - a day with more than 50 rows, so a day splits across pages;
  - one transfer for each D27 value: harian → tabungan, tabungan → harian, harian → goal-linked harian pocket, RDN cash → product top-up, harian → dana_investasi, tabungan → product top-up, harian → credit, tabungan → credit, a Beli trade, harian → harian;
  - an expense paid from a tabungan pocket, a saving-pillar expense, and a reimbursement that offsets the saving pillar.
  Each addition is listed in the seed report.
- **Savings parity (`parity.mjs`, owner feedback 4):** for the open cycle, the previous cycle and the current month, the hero of `/ledger?type=saving&period=…` equals Beranda's "Ditabung & investasi" (`cycle_summary` `tabungan.total`) byte for byte, in both languages and with hidden balances off; the `saving` count equals the number of listed rows; and the API test of D28 proves the same equality on the seed.
- **Backend suite:** because D27 and D28 touch `backend/`, the full backend suite runs on migrated disposable PostgreSQL, and the Kakeibo, cycle-summary, dashboard and ledger tests pass unchanged.
- **Stress and USD seed:**
  - the audit seed (17 pockets, long pocket routes);
  - more than 1,000 rows;
  - 60-character merchants and notes;
  - Rp 1.234.567.890;
  - the USD display currency.
- **Scroll:** with 200 rows loaded at 1440 px, a CDP performance trace of a full scroll shows no main-thread task over 50 ms.
- **Colour:** `tokens.test.ts` pins `--out-fg` at ≥ 4.5:1 on `--canvas`, `--window`, `--surface`, `--surface-2` and `--dialog` in both themes (D29), and the checklist reads the computed colour of every expense amount on Transaksi, the dialog and Beranda as `--out-fg`.

The gate (tasks group 6) is the plan's common gate plus the both-language gate of `add-language-system` (D16 there).

### D25. Spec reconciliation

`transaction-ledger-management`:
- **Modified:** Searchable & Filterable Ledger View, Transaction Detail & Inline Editing, Consolidated Internal Movement Ledger Representation (items 3, 8 and 9: the row presentation, the selection start and the bar), Loading More Ledger Rows, Confirming a Row With Undo, Ledger Type Switch, Transaksi Keys, Return to the Tinjau Item, Ledger Table Columns.
- **Removed:** Ledger Day Heading Colours (reason and migration in the delta).
- **Also modified (API, additive, owner feedback 4):** Ledger Day Totals and Type Counts (`type_counts.saving`), Ledger Type and Waiting Filters (`type=saving` and the summary's `saving_net`).
- **Added:** Transaksi Hero Figures, Transaksi Type and Period Rail, Ledger Filter Dialog, Ledger Day Headings, Ledger Loading, Failure and Empty States, Transaksi in Both Languages, Ledger Saving Flow.
- **Unchanged** (behaviour, neutral to presentation): Recurring Rule Origin Tracking, Flexible Transaction Creation Contract, Complete Canonical Transaction Capture, Internal Movement Capture Mode, Complete Ledger Editing, Consistent Post-Mutation Refresh, Ledger Spending Figure.

Other capabilities:
- `mobile-native-layout`: Mobile Date-Grouped Transaction Ledger.
- `monthly-navigation-stepper`: Month-and-Year Stepper Navigation, written over `add-language-system`'s version of the requirement, which merges first.
- `tactile-pulse-ui`: Semantic Colour Meaning (expense red wherever it carries "−", owner feedback 1); No Decorative Effects.
- `beranda-cycle-summary`: Recent Activity on Beranda (the red expense, and a transfer by its note, route and saving direction).
- `kakeibo-reconciliation` ("savings from the economic direction of a durably linked movement") and `account-roles` need no delta: D27 moves their SQL into one helper without changing what it counts, which the existing tests prove.
- `transaction-provenance`: Provenance in the Ledger Edit Dialog. Its old wording ("Tandai sudah ditinjau", "Otomatis, belum ditinjau", `review_status = reviewed`) no longer matched the drawer, which shows "Dicek" and sends `POST /review/bulk` (as "Confirming a Row With Undo" says).
- `executive-sidebar-layout` needs no delta: its Transaksi scenarios (frame 1040, search, the tab bar, old links) hold as written.
- `interface-language` and `ui-copy-register` need no delta (proposal, Capabilities).

Legacy "Required behavior and constraints" scenarios in the modified requirements are kept and updated, not split, so every constraint they carried stays in the spec.

### D26. Dependencies and order

- **Before task 2.1 starts,** `apply-markas-tinjau` (phase 4), `add-language-system` and `localize-server-text` must be merged into `main`, and `feat/markas-transaksi` is updated from `origin/main` (task 1.2). If one is missing, the phase stops and reports. It never stacks on an unmerged branch.
- **From phase 4:** `Rail`, `RailItem`, `RailRule`, `HeroFigure`'s optional mark, `ListRow`'s `rowKey`, `current` and `selected`, the `.raw` box, and the untranslated-test example at Transaksi.
  - If phase 4 merged a different API for one of them, this phase adapts to what is on `main` and records the difference in `verification.md`.
  - If phase 4 dropped a piece, this phase builds it in the shared place named in D22, not locally.
- **From the language changes:** the catalogues, `terms`, `common.period.*`, `common.rowMarks.*`, `lib/format.ts`, the registry, the copy register, `renderWithIntl`, and the error codes of the movement and merge endpoints.
- **Conflicts with phase 6:** Analisis's Kalender and Peta Aliran import `TransactionDrawer` today. This phase renames the import to `TransactionDialog`, and phase 6 starts from that.
- **Follow-up: `add-credit-spending-flow`** (planned separately) builds on this phase and must start from it, or rebase on it: it adds "Kredit" to `LEDGER_TYPES`, a `type` value and its own row labels (D30).

### D27. Saving flow per row — flags decided 2026-11-06

Owner feedback 3 asks for the saving direction in the pillar column, classified as Beranda's "Ditabung & investasi" is. The server has the rule only as aggregate SQL inside `get_kakeibo_breakdown`; no row carries it, and the browser has neither the roles of both legs' accounts nor the goal links on a row. So the server adds one read-only field.

**Field:** every row of `GET /transactions` and `GET /transactions/{id}` gains `saving_flow`:

| Value | When (the server's rule, as `get_kakeibo_breakdown` counts) | Label (id / en) | In "Tabungan & investasi" |
| --- | --- | --- | --- |
| `saved` | a counted movement from a non-destination into a savings destination that is not a product (role `tabungan`, or a goal-linked account) | Ditabung / Saved | yes (+) |
| `invested` | a counted movement into an investment product: a top-up from a non-destination, or a legacy row stored with the saving pillar | Investasi / Invested | yes (+) |
| `taken` | a counted movement from a savings destination into a non-destination, including into a credit account | Diambil dari tabungan / Taken from savings | yes (−) |
| `paid_from_savings` | a reportable expense paid from a `tabungan` or goal-linked account | the row's pillar, as today | yes (−) |
| `saving_pillar` | a reportable expense whose effective pillar is saving, not paid from savings | the row's pillar (Tabungan) | yes (+) |
| `debt_payment` | a movement into a credit (`utang`) account from a non-destination. Beranda's rule leaves it outside the figure (neither side is a savings destination), so the label is informational only | Bayar utang / Debt payment | no |
| `null` | every other row: harian → harian, a move into a `dana_investasi` account, a top-up between two savings destinations (such as `tabungan` → product), a Beli/Jual trade | — | no |

- **Precedence:** counted classes first (`saved`, `invested`, `taken`), then `debt_payment`, then `null`. An expense both paid from savings and with the saving pillar is `paid_from_savings`; its two effects cancel in the server's total, as today.
- **Consolidated rows:** the movement's outbound leg carries the value, and both legs carry the same one, so an account view shows the same label on either side.
- **One definition:** `_SAVINGS_DESTINATION_SQL`, `_PRODUCT_SQL` and the top-up/legacy rule move from `routers/dashboard.py` into `services/saving_flow.py`. `get_kakeibo_breakdown` and the ledger query both read it. This is a move, not a change: the existing Kakeibo, cycle-summary and dashboard tests pass unchanged, and a new test asserts that for the seeded cycle the sum of `saved` and `invested` amounts equals `saving_into` and the sum of `taken` amounts equals `saving_out`.
- **Read-only:** no write path, schema, migration or existing field changes. The companion does not read the field. It is computed per request from roles and goal links as they are now, the same way the aggregate reads them.

- **Rule for labels (decided 2026-11-06):** a saving label appears only when the move changes Beranda's "Ditabung & investasi", so no label suggests that something counts when it does not. The one exception is "Bayar utang", which is informational: Beranda's rule counts a payment from a non-saving account into a credit account in neither direction, and the label only names what the move was. The enum is kept minimal: no value exists for a move that changes nothing.

**Flag (backend addition) — decided 2026-11-06: default taken.** This phase touches `backend/` (gate item 6 of the plan applies: the full backend suite on migrated disposable PostgreSQL). The rejected fallbacks were a separate backend change merged before this phase, and classifying in the browser from `/accounts` roles and `/goals` links (a second copy of a money rule, rejected for drift).

**Flag (owner rule reconciled) — decided 2026-11-06.** Three of the owner's cases differ from what Beranda counts. Under the owner's standing instruction the coordinator decided that a label never suggests counting that does not happen:
1. **Into `dana_investasi`:** `null`, shown as "—"; the route already names the target. Investment cash is not a savings destination (`kakeibo-reconciliation`, `account-roles` "Kas Investasi Outside Siap Pakai"); the later top-up into a product counts as `invested`.
2. **Between two savings destinations** (a top-up from a `tabungan` pocket into a product): `null`, shown as "—", because it moves money inside savings and investments and changes nothing. The optional dialog line "Dari tabungan ke investasi · tidak mengubah angka" is not built, because it would need an enum value that only this line reads; the dialog's "Dari" and "Ke" already say it.
3. **From `tabungan` into a credit account:** `taken`, "Diambil dari tabungan", matching Beranda.
Rejected: the owner's literal "Investasi" for cases 1 and 2 (it would suggest counting), a `to_investment` value (unused once those cases show "—"), and changing what Beranda counts (a money-logic change for its own plan, never a UI phase).

Rejected:
- Labels by the owner's wording without the server rule: the filter would list rows whose sum differs from Beranda's figure, and the label "Ditabung" could appear on a move Beranda does not count.
- Several roles per account or tags (rejected by the owner on 2026-11-06, because they double count).

### D28. "Tabungan & investasi" filter — flags decided 2026-11-06

Owner feedback 4: a sixth type in the rail and the type control, "Tabungan & investasi" ("Savings & investments"), whose total for the cycle equals Beranda's "Ditabung & investasi".

**API (additive, read-only):**
- `GET /transactions` accepts `type=saving`: rows whose `saving_flow` is `saved`, `invested`, `taken`, `paid_from_savings` or `saving_pillar`, with the same period, range, search and row filters as any type.
- `type_counts` gains `saving`. It overlaps the other tabs (a top-up counts under `investment` and `saving`), so the tab counts no longer add up to "Semua"; the rail shows each as it is.
- The summary (`period`, `cumulative`) gains `saving_net`: `savings_change(get_kakeibo_breakdown(window))["total"]` for the same window, the function Beranda's figure comes from, so the two are equal by construction, reimbursement offsets included. It is set only for `type=saving` without any other row filter (account, category, pillar, status, source, waiting, duplicates or search), and `null` otherwise, because the aggregate has no per-filter form.
- `listsLogicalMovements` keeps movements consolidated for `type=saving`, as for "Pindah saldo" and "Investasi".
- The web: `LEDGER_TYPES` and the URL parser accept `saving`; an unknown value still falls back to "all".

**Page:** the hero of D4 shows "Ditabung & investasi" with `saving_net`. Rows keep their columns: transfers show their D27 label, and expenses show their pillar with the meta "Dibayar dari tabungan" for `paid_from_savings`. The day totals are the server's `daily_totals` for the filtered rows, as for any type.

**Acceptance (gate D24):** for the open cycle, the previous cycle and the current month, the hero of `/ledger?type=saving&period=…` equals Beranda's figure byte for byte, in both languages; a backend test proves `saving_net == cycle_summary tabungan.total` on the parity seed.

**Flag (backend addition) — decided 2026-11-06: default taken,** as D27. **Flag (content beyond the owner's list) — decided 2026-11-06: default taken.** The filter lists every part of Beranda's figure: moves counted into or out of savings, counted top-ups, spending from a savings account (`belanja`) and Tabungan-pillar expenses (`pilar_tabungan`). The rows then add up to the figure, and each row shows its own label ("Ditabung", "Diambil dari tabungan", "Investasi", the meta "Dibayar dari tabungan", or the Tabungan pillar), so it is clear why it is listed. The rejected fallback was movements and top-ups only.

Rejected:
- Summing the listed rows in the browser: new client money arithmetic, and it cannot reproduce the per-pillar reimbursement offset.
- Showing `saving_net` with other filters by summing per-row effects: the aggregate is defined for the whole window only.

### D29. Expense colour — owner-directed deviation

Owner feedback 1: "expense value in transactions still in plain white color, better make it red".
- **Token:** `--out-fg` (the app's expense text ink, the prototype's `--expense`), unchanged in value: `#B8142D` in light and `#FF6B78` in dark. Measured contrast:

| Theme | `--canvas` | `--surface` (hover) | `--surface-2` (selected, pressed) | `--dialog` |
| --- | --- | --- | --- | --- |
| Light | 6.17:1 | 5.76:1 | 5.16:1 | 6.17:1 |
| Dark | 6.62:1 | 6.12:1 | 5.42:1 | 6.12:1 |

  No value needs adjusting.
- **Where:** every amount that carries "−" for an expense: Transaksi rows, the transaction dialog's amount, Beranda's Aktivitas terbaru and Perlu tindakan, and the Rekening detail's rows, through the one `RowAmount` (D22). Perlu tindakan stops painting its own `t-amt` and uses the shared component.
- **Not changed:** income stays `--in-fg` green; transfers sky, top-ups lavender; the hero "Pengeluaran", the day totals and Bersih stay in the ink or muted, because they are figures, not signed expense amounts.
- **Non-colour cue:** the "−" sign and the row's pillar or kind words stay, so the kind is never carried by colour alone (1.4.1).
- **Token test:** `tokens.test.ts` adds `--canvas` to the `--out-fg` pairs (today it checks `--window`, `--surface`, `--surface-2`, `--dialog` and `--out-bg`) and a test that the `c-expense` rule and `RowAmount`'s expense variant resolve to `var(--out-fg)`.
- **Specs:** `tactile-pulse-ui` "Semantic Colour Meaning" and `beranda-cycle-summary` "Recent Activity on Beranda" change from the ink to the expense red.

Rejected:
- The prototype's and phase 3's ink: overridden by the owner.
- A new, brighter red token: `--out-fg` already passes on every surface, and one expense ink is simpler.

### D30. Room for credit spending

`add-credit-spending-flow` (per-provider credit purchases and settlements, 0 % installments, "no cash out until settlement" labelling) is planned separately and is not built here. This phase leaves room for it:
- **Types:** the rail, the type control, the URL parser and `type_counts` read one ordered list (`LEDGER_TYPES` with its label, count and API value). "Kredit" is one more entry there and one more `type` value on the server.
- **Rows:** a credit purchase (an expense paid from a credit account) keeps its provider account in the meta ("SPayLater"), its category and pillar as today, and the expense red. A payment into a credit account is labelled "Bayar utang" (D27), which that change may refine for settlements.
- **No labels or rules for installments or settlement here;** the follow-up change owns them, and its plan starts from this phase's merged state.

Rejected:
- A placeholder "Kredit" item now: an empty filter with no behaviour.

## Decisions and their defaults

Approved 2026-11-06 under the owner's standing instruction "complete all until finished". Every decision takes its recommended default. **Flag** marks a decision that is reviewed before implementation (task 1.1), because it goes beyond the approved behaviour or plan, or touches money behaviour or presentation.

| # | Decision | Default (taken) | Rejected alternatives | Flag |
| --- | --- | --- | --- | --- |
| D1 | Layout and breakpoints | `200 \| 1fr` from 1200; one column with the type control from 768 to 1199; phone below 768; viewport queries | `styles.css` rail from 1024 (rows overflow); container query (caused the phone feed); stacked rail | — (deviation 1) |
| D2 | Title actions | Stepper + "…" with Gabungkan and Filter | Keep "Rutin"; visible Gabungkan | **Flag: scope** ("Rutin" removed) |
| D3 | Stepper | Shared component restyled to SPEC; reset kept | Transaksi-only stepper; no reset | — (deviation 4) |
| D4 | Hero | `spending` with a qualifier line; Masuk and Bersih beside it with signs | Prototype's sum of every expense and its Bersih; no line; KPI cards | **Flag: money presentation** (rejected alternative recorded) |
| D5 | Rail | Phase 4 rail with toggle items and counts | Links; hidden zero counts | — |
| D6 | Type control < 1200 | Prototype `.tx-types` with counts | Tabs; select | — |
| D7 | Toolbar and chips | SPEC search, "Filter · N", value-only chips including the search text | Prefixed chips; search outside the chips | — |
| D8 | Filter dialog | 640 dialog, draft and "Terapkan"; Status, Sumber, waiting check, Periode | Single-select chips; live popover; multi-select checkboxes | **Flag: scope** (independent controls; Periode kept against the plan text) |
| D9 | Merge | Selectable rows, SPEC bar sticky while selecting; long-press kept | Inline bar; viewport-fixed bottom bar | — (deviations 6, 12) |
| D10 | Day headings | Shared `DayHeading`, muted totals, waiting in warning | Coloured totals | — |
| D11 | Rows | Shared `ListRow`, one `RowAmount` (expense red), note-first transfer titles, the saving direction in a 136 px pillar column, "Uang masuk" for income | Table; "Tidak dihitung"; the route as title; helpers left in Beranda | **Flag: plan correction** (no duplicate badge without a row flag) |
| D12 | Loading more | "N lagi di hari ini · Muat lagi", "Muat 50 lagi", end line | — | — |
| D13 | States | SPEC skeleton, error box, two empty states | Grey blocks; one empty state | — (deviation 3) |
| D14 | Transaction dialog | 560 `ShellDialog`, `.dl` per kind, effects listed on delete, no delete undo | Drawer look; one-sentence confirmation; a delete undo (new money operation) | **Flag: plan correction, money behaviour** (no Urungkan for delete) |
| D15 | Edit dialog | 600 dialog in place of the detail, segmented Jenis and Pilar, hints | Inline edit; URL edit mode | — |
| D16 | Movement edit | Edit dialog's movement mode; errors by code; `InternalMovementModal` removed | Keep the modal; map English text | — |
| D17 | Top-up conversion | On `ShellDialog`, same request | Leave it as is | — |
| D18 | Keys and focus | Unchanged keys; focus rules per dialog and chip | — | — |
| D19 | URL and history | Unchanged | — | — |
| D20 | Language | `transactions` namespace, `terms.net`, `terms.calendarMonth`, row keys moved, `/ledger` translated, the test example moved to `/insights` | Row keys left in `home`; prototype word order | — |
| D21 | Hidden balances | `MaskedMoney` on every amount | — | — |
| D22 | Shared pieces | Extend `ListRow`, `RailItem`, `HeroFigure`, `FigureBlock`, `PeriodStepper`; one `RowAmount` | Transaksi-only copies | — |
| D23 | Removed elements | As the table | — | — |
| D24 | Tooling | Scenarios, checklist, key flows, extended seeds, scroll trace | Existing seed only | — |
| D25 | Specs | As listed | — | — |
| D26 | Dependencies | Phase 4 and both language changes merged first; stop otherwise; credit flow follows | Stacking on unmerged branches | — |
| D27 | Saving flow per row | Server field `saving_flow` from one shared savings helper; labels follow the server rule | Browser classification from roles; the owner's literal wording for `dana_investasi`, saving-to-product and tabungan-to-credit; several roles or tags | **Flags decided 2026-11-06:** backend addition taken; labels only where Beranda counts (`dana_investasi` and saving-to-product "—", tabungan-to-credit "Diambil dari tabungan", "Bayar utang" informational) |
| D28 | "Tabungan & investasi" | `type=saving`, `type_counts.saving`, `saving_net` from Beranda's function; lists the two expense parts too | Browser sums; movements and top-ups only | **Flags decided 2026-11-06:** defaults taken (backend addition; all four parts listed) |
| D29 | Expense colour | `--out-fg` on every "−" expense amount, Transaksi, dialog and Beranda; token test | Ink (prototype, phase 3); a new red token | — (owner-directed deviation 14) |
| D30 | Credit preparation | One type list that takes "Kredit"; provider account in the meta | A placeholder item | — |

No default changes money behaviour, an existing request, a count or a figure's value. The flagged D4, D14 and D27 record alternatives that would have. D27 and D28 add read-only fields; every existing figure, including Beranda's, is unchanged and tested.

## Flag decisions (2026-11-06)

Decided by the coordinator under the owner's standing instruction "complete all until finished":
- **D2** "Rutin" leaves the menu; **D4** the prototype's hero arithmetic stays rejected; **D8** independent filter controls, and Periode kept in Filter at every width; **D11** no duplicate badge, and the 136 px pillar column (deviation 17); **D14** no delete undo: the recommended defaults, accepted.
- **D27** the backend field, accepted. Labels appear only when a move changes Beranda's "Ditabung & investasi": a move into `dana_investasi` and a top-up from a `tabungan` pocket into a product show "—"; a payment from `tabungan` into a credit account stays "Diambil dari tabungan"; "Bayar utang" stays as an informational label for credit payments from non-saving accounts, which Beranda's rule leaves outside the figure. `to_investment` is removed, and the enum is `saved`, `invested`, `taken`, `paid_from_savings`, `saving_pillar`, `debt_payment` or `null`.
- **D28** the default: the backend filter, and all four parts of Beranda's figure listed, so the rows add up to it.

## Risks / Trade-offs

- [The rail only from 1200 px] Laptops at 1024 to 1199 px get the type control instead of the rail, which differs from the prototype's CSS. Mitigation: deviation 1 with the arithmetic, captures at 1100 and 1024, and the owner's end review.
- [Shared drawing changes on other pages] The restyled stepper (Analisis, Laporan), the shared `RowAmount` (Rekening detail) and the dialog (Beranda, Kalender, Peta Aliran) change before their phases. Mitigation: those pages are captured before and after (task 6.1). Their figures are compared in the parity run, and the changes are the intended presentation.
- [Independent filter controls] New combinations become reachable (flag D8). They are combinations the API already serves, and `listsLogicalMovements` still decides consolidation, so a combination lists both legs as today when any row filter is set.
- [Sticky selection bar and day headings] Two sticky layers. Mitigation: the day headings shift by the bar's height only while selecting, and keyboard flow 15 checks that nothing focused is covered.
- [English lengths in fixed columns] The pillar (136) and category (168) columns. Mitigation: the short glossary forms of D20, ellipsis on the category, and measurements in both languages.
- [Phase 4 API drift] Phase 4 is planned, not merged. Mitigation: D26's adapt-and-record rule and task 1.2's stop condition.
- [Backend in a UI phase] D27 and D28 move money SQL into a shared helper. Mitigation: a pure move, the existing Kakeibo, cycle-summary, dashboard and ledger tests unchanged, the new equality tests (`saved` + `invested` = `saving_into`, `taken` = `saving_out`, `saving_net` = `tabungan.total`), and the full backend suite on migrated disposable PostgreSQL.
- [The informational "Bayar utang"] It is the one label outside Beranda's figure (D27). Mitigation: it is not listed under "Tabungan & investasi", and the design names it as informational.
- [Query cost] `saving_flow` joins both legs' accounts and goal links per page of 50, and `saving_net` runs the Kakeibo aggregate once for `type=saving`. Mitigation: the existing indexes on `movement_id` and `account_id`; the ledger request time on the stress seed is recorded before and after, and a rise over 50 ms at the 95th percentile is reported.
- [Overlapping tab counts] `saving` overlaps the other types. Mitigation: each count is labelled by its type, and "Semua" stays the total.
- [Copy volume] About 182 strings move to keys. The Indonesian text is checked byte for byte against today's by a rendered-text snapshot before and after (task 4.4).

## Migration and rollback

No data or schema change, and no migration; the API gains read-only fields and one filter value. Rollback is a revert of the merge commit, which drops them without data effects; the web of the previous release ignores them. The URL contract is unchanged, so links from Beranda, Analisis, Laporan, Tinjau, Rekening and the bell work across a rollback. The catalogue keys moved from `home` to `transactions.row` revert with the commit.

## Open Questions

None. Every flagged item was decided on 2026-11-06 (see "Flag decisions" below).

## Appendix A. Transaksi copy as message keys (`transactions` namespace; Indonesian · English)

Arguments in braces arrive formatted (D20). `{count}` selects plurals. Existing Indonesian text is kept byte for byte, and keys marked "new" are new strings or intended changes. The remaining existing strings (receipt messages, allocation checks, top-up eligibility reasons, delete effects) move under the same sub-namespaces with their Indonesian unchanged and English written with Appendix B. Task 3.1 produces their full list and task 4.4 reviews it.

### `page`, `hero`, `rail`, `types`, `periods`

| Key | Indonesian | English |
| --- | --- | --- |
| `page.subtitle` | Semua uang masuk, pengeluaran, dan pindah saldo | All income, spending and transfers |
| `page.more` | Tindakan lain | More actions |
| `page.mergeStart` | Gabungkan dua transaksi | Merge two transactions |
| `page.mergeStop` | Batal gabungkan | Cancel merge |
| `page.filter` (new) | Filter transaksi | Filter transactions |
| `hero.allTime` (new) | {label} · semua waktu | {label} · all time |
| `hero.range` (new) | {label} · {range} | {label} · {range} |
| `hero.reimbursed` | sudah dikurangi penggantian {amount} | after {amount} in reimbursements |
| `hero.withoutSaved` | tanpa {amount} yang ditabung | not counting {amount} saved |
| `hero.netLabel` (new) | Bersih · masuk − pengeluaran | Net · income − spending |
| `hero.unavailable` | Belum tersedia | Not available yet |
| `rail.label` (new) | Jenis dan periode | Type and period |
| `rail.types` (new) | Jenis | Type |
| `rail.periods` (new) | Periode | Period |
| `rail.count` (new) | {count, plural, other {# transaksi}} | {count, plural, one {# transaction} other {# transactions}} |
| `types.all` | Semua | All |
| `types.expense` | `terms.spending` | `terms.spending` (Spending) |
| `types.income` | `terms.income` | `terms.income` (Income) |
| `types.movement` | `terms.transfer` | `terms.transfer` (Transfer) |
| `types.investment` | `terms.investments` | `terms.investments` (Investments) |
| `types.saving` (new) | `terms.savingsAndInvestments` (Tabungan & investasi) | Savings & investments |
| `hero.savingUp` · `hero.savingDown` | `home` keys reused: Tabungan bertambah {amount} · Tabungan berkurang {amount} | Savings went up {amount} · Savings went down {amount} |
| `hero.savingUnavailable` (new) | Tidak tersedia dengan filter lain | Not available with other filters |
| `periods.cycle` | `terms.payCycle` | `terms.payCycle` (Pay cycle) |
| `periods.month` | `terms.calendarMonth` (new) | Calendar month |
| `periods.all` | Semua waktu | All time |

### `toolbar`, `chips`, `filter`

| Key | Indonesian | English |
| --- | --- | --- |
| `toolbar.search` | Cari catatan, merchant, kategori | Search notes, merchants, categories |
| `toolbar.filter` | Filter | Filter |
| `toolbar.filterCount` (new) | Filter · {count} | Filter · {count} |
| `toolbar.filterCountName` | {count, plural, other {Filter, # aktif}} | {count, plural, one {Filter, # active} other {Filter, # active}} |
| `chips.label` | Filter aktif | Active filters |
| `chips.remove` | Hapus filter {label} | Remove filter {label} |
| `chips.query` (new) | “{text}” | “{text}” |
| `chips.waiting` | Menunggu jawaban | Needs an answer |
| `chips.phone` | Dari HP | From phone |
| `chips.typed` | Lewat Catat | Added in app |
| `chips.duplicates` | Kemungkinan duplikat | Possible duplicates |
| `chips.range` (new) | {range} | {range} |
| `filter.title` | Filter transaksi | Filter transactions |
| `filter.account` · `filter.allAccounts` | Rekening · Semua rekening | Account · All accounts |
| `filter.category` · `filter.allCategories` | Kategori · Semua kategori | Category · All categories |
| `filter.pillar` · `filter.allPillars` | Pilar Kakeibo · Semua pilar | Kakeibo pillar · All pillars |
| `filter.status` | Status | Status |
| `filter.statusAll` · `needsReview` · `auto` · `reviewed` | Semua status · Perlu ditinjau · Belum dicek · Sudah dicek | All statuses · Needs review · Not checked · Checked |
| `filter.source` (new) · `sourceAll` (new) | Sumber · Semua sumber | Source · All sources |
| `filter.waitingOnly` (new) | Hanya yang menunggu jawaban di Tinjau | Only rows that need an answer in Review |
| `filter.duplicates` | Kemungkinan duplikat | Possible duplicates |
| `filter.period` | Periode | Period |
| `filter.reset` | Atur ulang | Reset |
| `filter.apply` (new) | Terapkan | Apply |

### `merge`, `list`, `day`, `row`, `more`, `states`, `announce`

| Key | Indonesian | English |
| --- | --- | --- |
| `merge.region` | Gabungkan dua transaksi | Merge two transactions |
| `merge.count` | {count} dari 2 dipilih | {count} of 2 selected |
| `merge.action` (new) | Gabungkan jadi pindah saldo | Merge into a transfer |
| `merge.saving` | Menyimpan… | Saving… |
| `merge.select` | Pilih {title} {time} | Select {title} {time} |
| `merge.needOneEach` | Pilih satu transaksi keluar dan satu transaksi masuk. | Choose one money-out and one money-in transaction. |
| `merge.needUnlinked` | Pilih transaksi yang belum tertaut dari dua rekening berbeda. | Choose unlinked transactions from two different accounts. |
| `merge.pickPair` (new) | Pilih pasangannya di daftar, lalu Gabungkan jadi pindah saldo. | Choose its other side in the list, then Merge into a transfer. |
| `merge.mismatchTitle` | Nominal tidak sama | Amounts differ |
| `merge.mismatchBody` | Nominal keluar ({out}) berbeda dengan nominal masuk ({in}). Pindah saldo hanya dapat dibuat jika nominalnya sama. | The money out ({out}) differs from the money in ({in}). A transfer needs equal amounts. |
| `list.heading` | Daftar transaksi, {count, plural, other {# transaksi}} | Transaction list, {count, plural, one {# transaction} other {# transactions}} |
| `day.today` · `day.yesterday` | {day} · hari ini · {day} · kemarin | {day} · today · {day} · yesterday |
| `day.income` · `day.spending` · `day.transfer` | Masuk {amount} · Pengeluaran {amount} · Pindah saldo {amount} | Income {amount} · Spending {amount} · Transfer {amount} |
| `day.waiting` | {count, plural, other {# menunggu jawaban}} | {count, plural, one {# needs an answer} other {# need an answer}} |
| `row.waiting` (moved) | Menunggu jawaban | Needs an answer |
| `row.income` (moved) | Uang masuk | Money in |
| `row.inFrom` · `row.outTo` (moved) | Masuk dari {other} · Keluar ke {other} | In from {other} · Out to {other} |
| `row.pairNotFound` (moved) | Pasangan belum ditemukan | Other side not found yet |
| `row.categoryTransfer` (new) | Pindah saldo | Transfer |
| `row.categoryTopup` (new) | Top up investasi | Investment top-up |
| `row.noCategoryYet` (new) | Belum ada kategori | No category yet |
| `row.topupTitle` (new) | Top up {product} | Top-up {product} |
| `row.paidFromSavings` (new) | Dibayar dari tabungan | Paid from savings |
| `flow.saved` (new) | Ditabung | Saved |
| `flow.taken` (new) | Diambil dari tabungan | Taken from savings |
| `flow.invested` (new) | Investasi | Invested |
| `flow.debtPayment` (new) | Bayar utang | Debt payment |
| `flow.none` (new) | — | — |
| `more.inDay` | {count} lagi di hari ini | {count} more on this day |
| `more.loadDay` | Muat lagi | Load more |
| `more.load` | Muat {count} lagi | Load {count} more |
| `more.loading` | Memuat… | Loading… |
| `more.end` (new) | {count, plural, other {# transaksi}} · akhir daftar | {count, plural, one {# transaction} other {# transactions}} · end of list |
| `more.failed` | Gagal memuat transaksi berikutnya. | The next transactions could not be loaded. |
| `states.loading` (new) | Memuat transaksi | Loading transactions |
| `states.errorTitle` (new) | Transaksi belum bisa dimuat | Transactions could not be loaded |
| `states.errorLine` (new) | Server tidak menjawab. Data yang tersimpan tetap aman. | The server did not respond. Saved data is safe. |
| `states.emptyTitle` (new) | Belum ada transaksi | No transactions yet |
| `states.emptyLine` (new) | Transaksi dari HP dan yang dicatat manual muncul di sini. | Transactions from the phone and those added manually appear here. |
| `states.noMatchTitle` (new) | Tidak ada transaksi yang cocok | No matching transactions |
| `states.noMatchLine` (new) | Ubah pencarian atau filter. | Change the search or filters. |
| `states.clearAll` (new) | Hapus semua filter | Clear all filters |
| `states.add` | Catat transaksi | Add transaction |
| `announce.loaded` | {count} transaksi lagi dimuat. | {count, plural, one {# more transaction loaded.} other {# more transactions loaded.}} |

### `dialog`, `detail`, `delete`, `toast`

| Key | Indonesian | English |
| --- | --- | --- |
| `dialog.title` | Detail transaksi | Transaction details |
| `dialog.close` | Tutup detail | Close details |
| `dialog.when` (new) | {date} pukul {time} | {date} at {time} |
| `dialog.amount` | Jumlah | Amount |
| `dialog.missingTitle` | Transaksi tidak ditemukan | Transaction not found |
| `dialog.missing` | Transaksi ini sudah tidak ada. Daftarnya sudah dimuat ulang. | This transaction no longer exists. The list has been reloaded. |
| `dialog.loadError` | Detail transaksi belum bisa dimuat. | The transaction details could not be loaded. |
| `detail.type` · `account` · `category` · `kakeibo` | Jenis · Rekening · Kategori · Kakeibo | Type · Account · Category · Kakeibo |
| `detail.merchant` · `notes` · `none` | Merchant · Catatan · — | Merchant · Notes · — |
| `detail.from` · `to` · `fee` · `feeFor` | Dari · Ke · Biaya · Biaya untuk | From · To · Fee · Fee for |
| `detail.feeOpen` (new) | Lihat transaksi biaya | View fee transaction |
| `detail.pairStatus` (new) · `pairComplete` (new) | Status pasangan · Lengkap | Pair status · Complete |
| `detail.savingFlow` (new) | Tabungan & investasi | Savings & investments |
| `detail.recorded` · `classified` · `habit` | Dicatat · Diklasifikasikan · Kebiasaan | Recorded · Classified by · Habit |
| `detail.goal` · `bill` | Target · Tagihan | Goal · Bill |
| `detail.checked` · `checkedYes` · `checkedNo` | Dicek · Sudah dicek · Belum dicek | Checked · Checked · Not checked |
| `detail.markChecked` · `marking` | Tandai sudah dicek · Menandai… | Mark as checked · Marking… |
| `detail.receipt` · `receiptSaved` (new) · `receiptNone` (new) | Bukti · Tersimpan · Belum ada bukti | Receipt · Saved · No receipt |
| `detail.originNotification` · `originTyped` | Notifikasi asli · Teks yang diketik | Original notification · Typed text |
| `detail.waiting` (new) | Transaksi ini menunggu jawaban di Tinjau. | This transaction needs an answer in Review. |
| `detail.answer` | Jawab di Tinjau | Answer in Review |
| `detail.edit` · `delete` | Ubah · Hapus | Edit · Delete |
| `delete.question` | Hapus transaksi {amount}? | Delete transaction {amount}? |
| `delete.final` | Penghapusan tidak dapat dibatalkan. | Deleting cannot be undone. |
| `delete.deleting` | Menghapus… | Deleting… |
| `toast.checked` | Ditandai sudah dicek. | Marked as checked. |
| `toast.saved` | Perubahan disimpan. | Changes saved. |
| `toast.deleted` | Transaksi dihapus. | Transaction deleted. |
| `toast.merged` | Digabung sebagai pindah saldo. | Merged into a transfer. |
| `toast.topup` | Dicatat sebagai top up investasi. | Recorded as an investment top-up. |

### `edit`, `movement`, `topupConversion`, `keys`

| Key | Indonesian | English |
| --- | --- | --- |
| `edit.title` | Ubah transaksi | Edit transaction |
| `edit.type` | Jenis | Type |
| `edit.amount` · `edit.total` | Nominal (IDR) · Total bayar (IDR) | Amount (IDR) · Total paid (IDR) |
| `edit.date` | Tanggal dan waktu | Date and time |
| `edit.payFrom` · `edit.paidInto` | Bayar dari · Masuk ke | Paid from · Paid into |
| `edit.offset` | Mengurangi pengeluaran | Reduces spending |
| `edit.pillar` · `edit.pillarChoose` | Pilar Kakeibo · Pilih pilar agar transaksi ini masuk ke ringkasan Kakeibo. | Kakeibo pillar · Choose a pillar to count this transaction in the Kakeibo summary. |
| `edit.goal` · `edit.goalNone` | Kaitkan ke target (opsional) · Tidak dikaitkan | Link to a goal (optional) · Not linked |
| `edit.bill` · `edit.billNone` | Bayar tagihan / utang (opsional) · Bukan pembayaran utang | Pay a bill or debt (optional) · Not a debt payment |
| `edit.merchant` | Merchant atau tempat (opsional) | Merchant or place (optional) |
| `edit.receipt` | Ganti atau tambahkan bukti | Replace or add a receipt |
| `edit.save` · `edit.saving` | Simpan · Menyimpan… | Save · Saving… |
| `movement.title` (new) | Ubah pindah saldo | Edit transfer |
| `movement.from` · `movement.to` | Dari rekening · Ke rekening | From account · To account |
| `movement.split` (new) | Pisahkan jadi dua transaksi | Split into two transactions |
| `movement.splitQuestion` (new) | Pisahkan jadi dua transaksi? Kedua catatan tetap ada. | Split into two transactions? Both records stay. |
| `movement.splitConfirm` | Ya, pisahkan | Yes, split |
| `movement.sameAccount` | Rekening sumber dan tujuan tidak boleh sama | The source and target accounts must differ |
| `movement.creditBothSides` | Salah satu sisi harus rekening bank, e-wallet, atau tunai. | One side must be a bank, e-wallet or cash account. |
| `topupConversion.title` | Catat sebagai top up investasi | Record as an investment top-up |
| `topupConversion.product` | Produk investasi | Investment product |
| `topupConversion.action` | Catat sebagai top up | Record as top-up |
| `keys.group` | Di Transaksi | On Transactions |
| `keys.move` | Pindah ke transaksi berikutnya atau sebelumnya | Move to the next or previous transaction |
| `keys.open` | Buka detail transaksi | Open transaction details |
| `keys.undo` | Urungkan tanda sudah dicek atau top up terakhir | Undo the last check or top-up |

## Appendix B. Transaksi glossary (Indonesian → English)

Consistent with `research/2026-11-06-language-system.md` §7 and phase 4's Appendix B. Figure and concept names are `terms.*` and are spelled the same everywhere.

| Indonesian | English | Note |
| --- | --- | --- |
| Transaksi | Transactions | page title (`nav.ledger`); never "Ledger" |
| Pengeluaran | Spending | the only English name of the figure, the type and the filter (`terms.spending`); never "Expense(s)", "Spent" or "Outflow" |
| Masuk (figure) · Pemasukan · Uang masuk | Income · Income · Money in | "Money in" only in row text |
| Bersih | Net | `terms.net`; the label reads "Net · income − spending" |
| Pindah saldo | Transfer | between own accounts only; money sent to others is Spending |
| Gabungkan jadi pindah saldo | Merge into a transfer | |
| Investasi · Top up Investasi | Investments · Investment top-up | top-up (noun), top up (verb) |
| Siklus gaji · Bulan kalender · Semua waktu | Pay cycle · Calendar month · All time | |
| Siklus ini · Bulan ini | This cycle · This month | stepper reset |
| Jenis · Periode | Type · Period | rail labels |
| Rekening · Kantong | Account · Pocket | |
| Kategori · Tanpa kategori · Belum ada kategori | Category · No category · No category yet | |
| Pilar Kakeibo · Belum diklasifikasi | Kakeibo pillar · No pillar yet | |
| Kebutuhan · Keinginan · Budaya · Tak Terduga · Tabungan | Needs · Wants · Culture · Unexpected · Savings | `terms.*` |
| Menunggu jawaban | Needs an answer | short form for the pillar column and day totals |
| Perlu ditinjau · Belum dicek · Sudah dicek | Needs review · Not checked · Checked | |
| Tandai sudah dicek | Mark as checked | |
| Dari HP · Lewat Catat | From phone · Added in app | sources |
| Kemungkinan duplikat | Possible duplicates | not "Duplicates" |
| Pasangan belum ditemukan · Status pasangan · Lengkap | Other side not found yet · Pair status · Complete | |
| Tidak dihitung | — | removed: transfers and top-ups show their saving direction (owner feedback 3), and its keys `home.statusNotCounted` and `row.notCounted` go |
| Tabungan & investasi · Ditabung & investasi | Savings & investments · Saved & invested | the type · the figure it sums (`terms.*`) |
| Ditabung · Diambil dari tabungan · Investasi · Bayar utang | Saved · Taken from savings · Invested · Debt payment | saving directions (owner feedback 3) |
| Dibayar dari tabungan | Paid from savings | meta of an expense paid from a savings account |
| Bukti | Receipt | |
| Dicatat · Diklasifikasikan | Recorded · Classified by | |
| Merchant · Catatan | Merchant · Notes | |
| Hapus · Ubah · Simpan · Batal · Terapkan · Atur ulang | Delete · Edit · Save · Cancel · Apply · Reset | |
| Urungkan | Undo | shell term |
| Muat lagi · akhir daftar | Load more · end of list | |
