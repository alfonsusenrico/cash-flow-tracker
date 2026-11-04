# Proposal

## Why

The owner used the finished six-phase web redesign on their own account on 2026-11-04 and found it hard to read, saying they were "starting to give up" and might ask for a rollback. Every check had passed, but it ran on small, tidy synthetic data. With 13 pockets in one account, two investment platforms and real text lengths, the problems became obvious:
- prose summaries instead of figures;
- text-heavy cards;
- a three-line Transaksi toolbar;
- wasted space next to tiny cards;
- account rows that are hard to tell apart and that only open from the name;
- a detail panel squeezed to the side after an automatic scroll;
- investments without their gain in the list;
- a colour scheme the redesign changed without asking.

A local preview answering each point was built and compared with the pre-redesign app on data shaped like the owner's account. The owner approved it.

## Expected Outcome

On the owner's real account, at desktop and phone widths in both themes:
- every page this change touches reads at a glance, like the approved preview;
- the colours are the pre-redesign ones;
- the sidebar is short;
- Beranda leads with figures, not sentences;
- Rekening & Kantong shows its account cards with pockets inside and gain or loss on every investment, and its details open in the middle of the screen.

No money figure changes value. Every figure that is renamed or split states exactly which existing figure it shows.

## Approval

1. **Owner feedback, 2026-11-04.** The owner sent 11 production screenshots with the points listed under Why. They added "you shouldn't change the color scheme" and "remove "HP terhubung" and "Bantuan" put it all inside settings is enough". These points are binding, as recorded in the agent's memory notes `owner-design-preferences` and `ui-label-register`.
2. **Preview review, 2026-11-04.** On the first preview the owner asked for:
   - the safe amount for the whole period instead of per day: "better for me to see how much value safe to use in that period instead of calculating it by daily or weekly";
   - the old Rekening cards: "I still like the previous cards design that can dragged and the pockets is inside the card and draggable as well, combine with your kpi cards design is fine. Adding the "Komposisi Aset" is also good, no need to include the utang there".

   They also said "other than that can use your recommendation". That covers:
   - "Pengeluaran" as spending without savings, with "Ditabung & investasi" shown separately;
   - a centered modal for details;
   - Tanya removed from the navigation, with Catat kept.
3. **Final preview, 2026-11-04.** The owner reviewed `cash-flow-tracker-prototypes/2026-11-04-workspace-refinement/preview.html` and `compare.html` and said: "ok use that window, approved, create the plan". The preview is the visual and interaction target of this change. Where it conflicts with an approved money rule, the rule wins, and design.md records each such conflict.
4. **Still binding from the redesign phases:**
   - the copy register (`ui-copy-register`);
   - the review undo;
   - the shared transaction drawer;
   - the URL helper;
   - the one net worth helper (OD57);
   - the unchanged-writes rule for forms (phase 5 D17);
   - WCAG 2.2 AA verification (phase 1 D17).
5. **Owner testing, 2026-11-03.** The owner said "I'm not going to test anything until all phases done implemented properly and approved". This change is a correction of finished work, and the owner will look at it. The agent's evidence must still be complete, including data shaped like the owner's account.

**Plan approval, 2026-11-04.** After a summary of both refinement plans (`refine-workspace-layout` and `compare-whole-months`), the owner said "approved, defaults ok". OD80–OD87 take their recommended defaults (design.md, open decisions).

**Acceptance, 2026-11-04.** After the combined summary of both refinement changes, with the comparison page, the deviations and OD96–OD98, the owner said "accepted, defaults ok". OD96–OD98 keep their applied defaults.

## What Changes

- **Palette and type:**
  - the pre-redesign "Coral Terminal" colours in both themes, mapped onto the existing semantic tokens, with no alias layer returning;
  - Plus Jakarta Sans for interface text, self-hosted;
  - Geist Mono stays for code-like text.
- **Navigation:**
  - a short sidebar of Beranda, Tinjau, Transaksi, Analisis, Laporan, Rekening & Kantong and Target & Tagihan, with Pengaturan and the account card at the foot;
  - no group headings;
  - no Tanya, "HP terhubung" or "Bantuan";
  - the phone's status and the help move into Pengaturan, and help still opens with `?`;
  - the phone's Lainnya sheet loses Tanya and Bantuan.
- **Tanya:** removed. `/tanya` redirects to `/`. The intent code is deleted (D10). Catat stays the recording entry point.
- **Beranda:**
  - five KPI cards: Aman dipakai sampai gajian, Masuk, Pengeluaran, Ditabung & investasi, Kekayaan bersih;
  - no prose summary;
  - no insight banner;
  - no same-date chips;
  - balanced columns, with a full-size Ketahanan Dana card;
  - "Uang siklus ini" split into Pengeluaran, Ditabung & investasi, Terjadwal and Aman dipakai.
- **Spending terms:**
  - "Pengeluaran" replaces "Keluar" and "Belanja" as the label for spending. It never contains savings.
  - "Ditabung & investasi" is the saving bucket.
  - The cycle summary, the ledger summary and the day totals gain additive `spending` and `saving` fields, so the client computes no money value (D4).
- **Transaksi:** one toolbar row with:
  - type tabs with counts;
  - search;
  - one "Filter" popover holding the period mode, the chips and the account, category and pillar filters;
  - a "⋯" menu for Gabungkan and Rutin.
- **Rekening & Kantong:**
  - KPI cards: Total aset, Siap pakai, Investasi, Kekayaan bersih;
  - a full-width "Komposisi aset" card without debt;
  - account cards in two columns, each with its pockets inside;
  - drag reorder of cards and of pockets inside a card, plus move buttons, through the existing `POST /api/accounts/reorder`;
  - gain or loss per product and per platform in the list;
  - account and pocket details in a centered modal (a bottom sheet on phones), with no automatic scroll;
  - each account keeps its own colour, and accounts still on the default blue get distinct hues.
- **Target & Tagihan:** three equal cards (Ketahanan Dana, Utang, Tagihan 30 hari), then the goals at full width.
- **Pengaturan:** HP & perangkat shows the phone status that left the sidebar. Tampilan gains "Bantuan & pintasan".

## Non-goals

- The whole-month comparison logic, the Analisis Ringkasan and Laporan restructure, and "Yang bisa diperbaiki". These belong to the separate change `compare-whole-months`.
- **Tanya:** any answering of questions, and the later `add-tanya-llm-answers`.
- **Companion app:** any change to the Android companion. Its label sweep stays a separate follow-up.
- **Tinjau:** a new layout. Tinjau changes only through the palette and the terms.
- **Database:** a migration. None is needed.
- **Production:** any production action outside the merge and the `deploy.yml` pipeline. Local verification never grants production authority; the post-deploy check is read-only.

## Capabilities

**New capabilities:** none.

**Modified capabilities:**
- `design-tokens`: pre-redesign palette values.
- `tactile-pulse-ui`: navigation without groups or Tanya, and the coral accent with Plus Jakarta Sans.
- `web-font-hosting`: Plus Jakarta Sans and Geist Mono.
- `executive-sidebar-layout`: short sidebar, phone Lainnya list, companion status moved.
- `tanya-conversation`: retired. `/tanya` redirects and every other requirement is removed.
- `beranda-cycle-summary`: KPI cards instead of the written summary, the period safe amount, the Pengeluaran and Ditabung split.
- `observed-insights`: Beranda insight requirements removed.
- `ui-copy-register`: spending terms.
- `transaction-ledger-management`: one-row toolbar, Pengeluaran labels, `spending` in the day totals.
- `clean-core-ledger`: account cards, centered detail modal.
- `pocket-reordering`: cards with pockets inside, drag reorder.
- `net-worth-analytics`: Komposisi aset, platform gain.
- `mobile-native-layout`: phone Rekening composition.
- `financial-goals-tracker`: Target & Tagihan layout.
- `currency-conversion-and-settings`: help and phone status in Pengaturan.

**Project capabilities:** unchanged. The change touches `money` figures (labels and an additive split, no new arithmetic rule), `multi-user` (owner-scoped fields), `api-consumers` (additive fields only, and the companion uses none of the changed endpoints) and `deployed`. No new capability is added.


## Ordering

This change ships first, on `feat/workspace-refinement`. `compare-whole-months` is on its own branch, `feat/whole-month-comparisons`, from the same base `2f66187`, and becomes a second pull request. It uses this change's spending terms and `spending` fields, so it rebases onto `main` after this change merges. The two can be built in parallel, because they touch different pages, but this one must merge first.

## Impact

- **Frontend:**
  - `frontend/tokens.css`, `tailwind.config.js`, `app/layout.tsx` and fonts;
  - `components/workspace/*` (sidebar, Lainnya, navigation);
  - `components/beranda/*`, `app/ledger/page.tsx` and the ledger toolbar;
  - `components/accounts/*` and `app/accounts`;
  - `components/goals/*` and `app/goals`;
  - `app/settings/*`;
  - removal of `app/tanya`, `components/tanya` and `lib/tanya`;
  - the copy guard;
  - `tokens.test.ts` and the contrast pairs.
- **Backend (additive):**
  - `services/cycle_summary.py` (`spending`, `fresh_saving`);
  - `routers/transactions.py` summary and `daily_totals` (`spending`, `saving`);
  - `routers/accounts.py` (platform `group_capital_gain`, `group_cost_basis`).
- **Data:** no migration and no stored data change.
- **Docs:**
  - `AGENTS.md` (palette, terms, Tanya removed);
  - `docs/NEW_USER_GUIDE.md`.
- **Rollback:** revert the merge commit. No data needs restoring.
