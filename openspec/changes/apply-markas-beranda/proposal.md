## Why

On 2026-11-05 the owner chose design option 6 "Markas+" and expects the app to match it exactly:
- "I want 3, with combinated stylings (especially fonts) from 4 & 5, but the layout totally 3"
- "for any borders that colored and thick like border on add new transaction make it simple and clean"
- on the resulting prototype: "ok accepted"

Phase 1 (`apply-markas-foundation`) is merged. Phase 2 (`apply-markas-shell`) was approved with "merged, continue", is archived and pushed on `feat/markas-shell` (`539b367`), and awaits the owner's merge. This change is based on it.

Beranda is the first page inside the new shell, and the audit (`design-options/audit/AUDIT.md`) measured why it tires the owner:
- five KPI cards compete, with seven figures within 75 % of the hero's size;
- "Uang siklus ini" repeats four KPI figures, and "Kakeibo siklus ini" repeats Pengeluaran and Tabungan again, so 13 of 34 money figures are repeats ("Ditabung & investasi" appears five times);
- 20 dotted "explain" underlines, so nothing reads as plain data.

Phase 2's evidence adds one defect: between 1024 and about 1100 px the full sidebar narrows the page and the Kekayaan bersih KPI figure overflows its card by 24 px.

The phased plan is `research/2026-11-05-markas-implementation-plan.md` in the prototypes repository (phase 3 of twelve, owner decisions at their defaults). The fidelity source is `design-options/option-6/` (`styles.css`, `js/pages.js` Beranda, `js/dialogs.js` Rincian, `SPEC.md` §2, §4 to §8, §11 and §13). Where `SPEC.md` and `styles.css` differ, `styles.css` wins.

## Expected Outcome

- At 1440 px Beranda reads top to bottom as in the prototype:
  - the date as title ("Kamis, 5 November" in the prototype) with the cycle line ("Siklus gaji 23 Okt – 24 Nov · hari ke-14");
  - one hero "Aman dipakai sampai gajian" in 44 px with a single "Rincian" button, a line with the days left to the salary, a thin split bar, and Masuk, Pengeluaran, Ditabung & investasi and Terjadwal as four quiet supporting figures under it;
  - "Perlu tindakan" with at most three whole-row items and "Buka Tinjau · N", or one calm line when nothing waits;
  - "Aktivitas terbaru": today's and yesterday's rows under day headings, one line each, and "Semua transaksi";
  - at the right, a 352 px panel with Kekayaan bersih, Ketahanan Dana in one line, and Kakeibo siklus ini in five rows.
- Every money figure appears once on the page. "Rincian" opens one 480 px dialog with the calculation; its lines open in place to show what each is made of.
- Between 1024 and 1199 px the page is one column and the panel follows the reading column, so no figure leaves its block at any width from 320 px up.
- On a phone: hero, then Pengeluaran and Ditabung & investasi, then Perlu tindakan, Aktivitas terbaru and the panel.
- With "Sembunyikan saldo" every Beranda figure reads "Rp ••••••" and is announced as "Disembunyikan".
- Every figure the server provides and the prototype shows keeps its value. The numbers do not change; only where and how often they appear.

## Approval

Approved by the owner on 2026-11-06 ("approved"), with every recommended default. The thirteen owner decisions in `design.md` are decided as follows:
1. Per-day amount: (B) left out; the hero line reads "20 hari lagi sampai gajian 25 Nov".
2. Subtitle span: "Siklus gaji 23 Okt – 24 Nov · hari ke-14" with the server's window end, "Siklus gaji sejak 23 Okt" when the salary date is unknown.
3. Breakdowns inside Rincian: (A) the four lines open their server lines in place, plus the "Menunggu jawaban" disclosure.
4. "N dicatat otomatis hari ini": removed.
5. Waiting rows in Aktivitas terbaru: left out; they are in Perlu tindakan and Tinjau.
6. Status column: income "Uang masuk", a top-up "Tabungan" with the saving dot.
7. Kakeibo labels: each keeps its ledger link, plus "Perlu dipilih di Tinjau" when unclassified spending exists.
8. Ketahanan Dana target: "target 6 bulan" (`target_months`).
9. Ditabung & investasi caption: "Tabungan bertambah" or "Tabungan berkurang" without the amount.
10. Terjadwal caption: "tagihan sampai 24 Nov", and with goal set-asides "termasuk setoran · 24 Nov" (revised by the owner on 2026-11-06; the first wording was "tagihan & setoran sampai 24 Nov").
11. Masked figure markup: `role="img"` with `aria-label="Disembunyikan"`.
12. Top-up row title: "Top up Investasi".
13. Legend at 1024 px: the prototype's four columns, with a measured two-column fallback recorded as a deviation only if the stress figure overflows.

Three fixes approved by the owner on 2026-11-06 ("ok") after the first verification:
1. The Terjadwal caption reads "termasuk setoran · 23 Nov" when goal set-asides are included (was "tagihan & setoran sampai 23 Nov", which wrapped), so every caption stays on one line; without set-asides it stays "tagihan sampai 23 Nov". The breakdown stays in the Rincian. This replaces decision 10's second wording.
2. When spending exceeds income, the Rincian's total reads "Lebih dari uang masuk" with the positive amount, followed by the result "Aman dipakai Rp 0"; the dialog never shows a negative "Aman dipakai". The normal case is unchanged.
3. In "Perlu tindakan", overdue and due-today plans rank above the automatic-link check and the transfer pairing row (D6 gives the exact order), so an overdue or due-today plan is not hidden behind them.
The waiting note becomes two short sentences.

## What Changes

- **Hero** (prototype `.hero`):
  - the label row with the 16 × 2 px Aman mark, "Aman dipakai sampai gajian" and the small quiet "Rincian";
  - the figure in Manrope 44/52 (32/40 below 360 px), and one muted line with the days left and the expected salary date, or the reason when that date is unknown, or by how much the period is over;
  - the 8 px split bar (Pengeluaran, Ditabung & investasi, Terjadwal, Aman dipakai; 2 px gaps; percentages in its accessible name);
  - the legend of four supporting figures at 4 × 1fr, gap 24 (2 columns below 1024 px; on the phone only Pengeluaran and Ditabung & investasi).
- **Rincian dialog** (prototype `dlg-rincian-aman`): 480 px, centered, a bottom sheet on the phone; the rule, the four lines and the total "Aman dipakai" (when the period is over, "Lebih dari uang masuk" with the positive amount, then "Aman dipakai Rp 0"). The lines that have server lines expand in place (decision 3), and the waiting-for-answer note sits under the total.
- **Perlu tindakan:** the shared list row with an icon column, the item's name, a meta line (kind · date and time), its amount and a chevron. The whole row opens Tinjau at the item or "Rencana jatuh tempo". At most three rows. "Buka Tinjau · N" sits in the section header, and an empty state is one line.
- **Aktivitas terbaru:** sticky day headings ("KAMIS, 5 NOVEMBER · HARI INI"), compact one-line 56 px rows (time 40 · title and meta · status 112 · amount ≥ 120), and two-line 64 px rows on the phone. Rows waiting for an answer move to Perlu tindakan (decision 5). At most eight rows. "Semua transaksi" is a secondary button under the list. The row still opens the shared transaction dialog.
- **Context panel** (352 px, `--s1`, radius 12, padding 24):
  - Kekayaan bersih (20 px figure, "aset … · utang …"), the whole block linking to Rekening & Kantong;
  - Ketahanan Dana in one line ("5,4 bulan biaya pokok" and "dana darurat … · target 6 bulan"), linking to Target & Tagihan;
  - Kakeibo siklus ini: Kebutuhan, Keinginan, Budaya, Tak Terduga and Belum diklasifikasi with dot, amount and 2 px bar, "Perlu dipilih di Tinjau" when unclassified spending exists, and the "Analisis" link.
- **Shared pieces built here and reused by later phases:** figure blocks (hero, supporting), split bar, panel and panel block, section header, list row and its variants, day heading, the Rincian dialog body, the masked figure, and SPEC skeleton and error states for Beranda.
- **Masked figures:** Beranda's figure components render "Rp ••••••" with 0.08em spacing and the accessible name "Disembunyikan" (phase 2 owner decision 12).
- **Removed from Beranda** (each listed with its destination in `design.md` D10):
  - the five-up KPI strip;
  - the "Uang siklus ini" card with its "dari Rp … masuk" line, formula line and "Tagihan" link;
  - the separate Ketahanan Dana card with its coverage bar and rupiah target;
  - the repeated Pengeluaran total in the Kakeibo header and the Tabungan pillar row;
  - the dotted explain triggers (ten in the code, 20 counted by the audit across both layouts); one "Rincian" stays;
  - icon tiles and avatars;
  - the waiting amount beside "Perlu tindakan";
  - the "N dicatat otomatis hari ini" note;
  - the per-row date "4 Nov · 18.23" (day headings carry the date);
  - the React reordering of cards for one column (DOM order now equals visual order).
- **BREAKING (spec):** `finance-dashboard-cockpit` "Executive KPI Summary Ribbon" and `beranda-cycle-summary` "Beranda KPI Cards", "Uang Siklus Ini Card" and "Kakeibo and Ketahanan Dana Cards" are removed and replaced by "Beranda Hero", "Rincian of the Safe Amount" and "Beranda Context Panel". Beranda's explanations move from dotted figures to one "Rincian" dialog.

## Non-goals

- No change to any figure, money rule, API, schema, data or the Android companion. `backend/app/services/cycle_summary.py` and the overview API are read as they are. The figures come from the same server fields, and the client adds nothing it does not add today.
- No other page body. Transaksi keeps its KPI strip and table, and Analisis and Rekening keep their dotted figures and popover until phases 5, 6 and 8. The shared list row is built here but adopted by Transaksi only in phase 5.
- No change to the shell, the bell, Catat, the transaction dialog or "Rencana jatuh tempo" beyond being opened from the new rows.
- No new npm dependency.
- Local verification only, with the parity seed and the audit seed in disposable databases. Release and production stay with the owner through the approved pipeline after acceptance.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `beranda-cycle-summary`:
  - period line, safe-amount hero, Perlu tindakan, Aktivitas terbaru, loading and failure, and the waiting figure's place;
  - the KPI cards, "Uang siklus ini" and the separate cards are removed;
  - new requirements for the hero, its Rincian and the context panel.
- `explainable-figures`: the trigger is one "Rincian" control on a redesigned page (dotted figures stay only on pages not yet redesigned); the explanation is a centered 480 px dialog with expandable lines; the 760 px breakpoint becomes 768.
- `finance-dashboard-cockpit`: the KPI ribbon requirement is removed (its net-worth composition is owned by `net-worth-analytics`).
- `mobile-native-layout`: "Dedicated Mobile Home Information Architecture" follows the prototype's phone order.
- `tactile-pulse-ui`: expense amounts in the shared list row are ink with "−" (Semantic Colour Meaning), and Beranda has no icon tiles (No Decorative Effects).
- `theme-presets-switcher`: the "small row actions are quiet" scenario no longer names Beranda, whose rows are whole-row links.
- `investment-topups`: the home-feed presentation of a top-up (lavender amount, no icon tile, expense in ink).

Standards: UI and accessibility (WCAG 2.2 AA: headings and landmarks, focus visible and not obscured by the sticky top bar and day headings, dialog focus management, target size, reflow at 320 px, non-colour cues, reduced motion) and the copy register (`ui-copy-register`, `copyRegister.test.ts`). No capability in `AGENTS.md` changes.

## Impact

- **Frontend Beranda:** `components/beranda/BerandaView.tsx` (rewritten into hero, Perlu tindakan, Aktivitas and panel parts), `TodoCard.tsx`, `activity.tsx` (the exported helpers that Transaksi, Rekening, Kalender, Tinjau and the transaction drawer import keep their names and results), `explanations.ts` (the hero's Rincian content; unused `safeExplanation` and `waitingExplanation` are folded in or removed), `app/page.test.tsx`, `components/beranda/activity.test.ts`.
- **Shared components:** new figure, split bar, panel, section header, list row, day heading and masked-figure components (in `components/workspace/`), the Rincian dialog on the existing shared dialog primitive, and their rules in `app/globals.css`. `MoneyBar` and the Beranda-only `.ws-home-grid`, `.ws-money-legend`, `.ws-todo*` and `.ws-act-*` rules are removed. `KpiCard`, `.ws-kcards`, `.ws-kk*`, `.ws-dana*`, `.ws-ava` and `.ws-xn` stay for Transaksi, Analisis, Rekening, Target, Tinjau and the drawer.
- **Tests:** `app/page.test.tsx` and `activity.test.ts` are rewritten to the new requirements. `copyRegister.test.ts`, `explain.test.tsx` and `primitives.test.tsx` are extended.
- **Tooling:** `scripts/verify/markas/` gains the Beranda scenarios, the hero, legend, panel and row checklist, and the Rincian dialog scenario (local only).
- **No change** to `backend/`, `db/`, `mobile/` or `nginx/`. Rollback is a revert of the merge commit.
