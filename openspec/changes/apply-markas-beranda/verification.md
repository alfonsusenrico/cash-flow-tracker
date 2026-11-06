# Verification: apply-markas-beranda (Markas+ phase 3)

Date: 2026-11-06 · Branch `feat/markas-beranda`, fast-forwarded to `origin/main` `e8ec62d` (phase 2 merged) · Code under
test: `f08cced` for the first gate, and `f185584` for the owner's three fixes (section "Owner fixes, 2026-11-06"); this file
and `tasks.md` are committed on top.

Owner approval (2026-11-06): "approved", with every recommended default; the thirteen decisions are listed in
`proposal.md` › Approval and marked decided in `design.md`. `PROJECT_STATE.md` lives untracked in the owner's checkout,
which this run did not touch, so the approval is recorded in the change only.

## Environment

- Local only. PostgreSQL 16 in a disposable container `cft-beranda-pg` on `127.0.0.1:55460`:
  - `cft_markas_parity`: migrated by `flyway/flyway:10` to V51 and loaded by `seed_parity.py` (`dimas_contoh`,
    data.js shifted by 1 day to 2026-11-06; 188 transactions);
  - `cft_markas_audit`: the phase 1 audit seed (`raka_contoh`, 17 pockets, long names), at V51.
- Backend from this worktree in a `uv venv -p 3.12` environment with the pinned requirements; background jobs, AI and
  outbound HTTP off.
- "After": this worktree's production build with `NEXT_PUBLIC_COMPONENT_SHEET=1`, behind a loopback `/api` proxy on
  3496. "Before": `e8ec62d`'s `frontend/` exported with `git archive` into the session scratchpad and built the same
  way, on 3498. Prototype `design-options/` served statically on 3486.
- Headless Brave over CDP (`scripts/verify/markas/cdp.mjs`), device scale 1; axe-core 4 (wcag2a, wcag2aa, wcag21a,
  wcag21aa, wcag22aa).
- Captures, checklists and composed images stay in the session scratchpad (`…/scratchpad/markas-beranda/`), as the
  caller asked, not in `research/evidence/`; the repository holds only the tooling and this record.

## Tooling (task 1.3)

- `scenarios-app.json`: `beranda` (checklist on), `beranda-sembunyi` (hidden balances), `dlg-rincian-aman`,
  `dlg-rincian-aman-rincian-terbuka` (Pengeluaran line open), `dlg-rincian-aman-sembunyi`, `komponen-beranda`.
  `scenarios-prototype.json`: `beranda`, `beranda-sembunyi`, `dlg-rincian-aman` (`#/beranda?d=rincian:safeToSpend`),
  `dlg-rincian-aman-sembunyi`.
- `capture.mjs`: a scenario's `storage` (hidden balances in the app, `markas6-hide` in the prototype) is reset before
  every capture; widths 1199 and 1100 added; 1024 is 1024 × 768; `beranda-checklist.js` runs for `beranda` scenarios.
- `beranda-checklist.js`: generated from D1 and D3 to D9 and SPEC §2, §5, §7 (hero, label row and mark, Rincian
  button, figure and line, split bar, legend and figure blocks, section headers, rows, day headings, "Semua
  transaksi", panel, panel blocks, Kakeibo rows, figures inside their blocks, and the Rincian dialog: width, radius,
  header, meta, rule, four lines at 44, total at 56 with the strong rule, footer).
- `compare.mjs` composes any width and full-page captures (`--full 1`); `beranda-keys.mjs` scripts the keyboard flow.
- Dry run on the prototype (1440, 1024, 390): **330 of 336** checks; the 6 failures are "hero line without a per-day
  amount", which the prototype shows and owner decision 1 (B) leaves out.

## Before state (task 1.2)

`e8ec62d` with the parity seed: `beranda`, `beranda-sembunyi` and the dotted-figure explanation at 1440, 1280, 1199,
1100, 1024, 768, 767, 390 and 320 in both themes, **58 captures** (`shots/before`, `shots/before-dlg`): 0 axe serious or
critical, 0 console errors, 0 horizontal overflow. The phase 2 defect is reproduced by the shell checklist
(`shots/before-shell`): Kekayaan bersih's KPI figure (`button.ws-xn "Rp 45.828.280"`) reaches **+24 px** past the
frame at 1024, +5 at 1100, 0 at 1199; at 320 two figures pass it (+3, +16). The figure baseline is taken by
`parity.mjs` in the same run as the after build (below).

## Side by side (task 4.1)

24 composed images in `…/markas-beranda/compare/` (prototype left, app right): `beranda` and `beranda-sembunyi`
full page, `dlg-rincian-aman` and `dlg-rincian-aman-sembunyi` viewport, at 1440 × 900, 1024 × 768 and 390 × 844 in
both themes; the extra widths 1280, 1199, 1100, 768, 767 and 320 are in `shots/after-widths`.

Every visible difference, and why:
1. **Figures and rows differ by data**, not by layout: the app shows its own server figures for the parity seed one
   day later (Jumat, 6 November), with the known seed disagreements of phase 2 (the cycle starts 24 Okt; the
   scheduled amount includes goal set-asides, so the period is over and the hero is Rp 0 with "Siklus ini sudah lebih
   Rp 1.726.424 dari uang masuk"; Ketahanan Dana 8,1 bulan; Tinjau counts 4). Because the period is over, the split
   bar has no Aman dipakai segment. (In the first gate the Terjadwal caption "tagihan & setoran sampai 23 Nov" wrapped;
   the owner's fix 1 shortened it, see below.)
2. No per-day amount in the hero line (owner decision 1).
3. "termasuk setoran · …" when set-asides are scheduled (decision 10 as revised on 2026-11-06).
4. Perlu tindakan shows the app's own items in the revised D6 ranking: TRF BIFAST (question), Kelas Yoga (overdue) and
   Tagihan SPayLater (due today); the prototype shows a second question in place of Kelas Yoga.
5. Belum diklasifikasi keeps phase 1's dashed grey marker (D8), the prototype a solid grey dot.
6. Kakeibo labels are links (decision 7), ink at rest, so they look the same until hovered.
7. The Rincian has a 16 px chevron on each line with parts, the basis and waiting notes and the "Menunggu jawaban ·
   Rp …" disclosure (decision 3, D5), so the dialog is taller.
8. The movement arrow "→" draws slightly longer in the app; both use Inter, the prototype from Google Fonts and the
   app its self-hosted copy (phase 1). Not changed here.
9. On the phone the Rincian button is a 44 px target (D9) placed with a −6 px margin, so the label row stays 32 px as in
   the prototype.

## Measurement checklist and global rules (task 4.2)

| Set | Captures | Beranda checklist | Shell checklist | axe serious/critical | Console | Overflow |
| --- | --- | --- | --- | --- | --- | --- |
| Prototype, 1440/1024/390 × light/dark, 4 scenarios | 24 | 1,624 of 1,636 (12 = decision 1) | — | 0 | 0 | 0 |
| App, same cells, 5 scenarios (`shots/after`) | 30 | **1,782 of 1,782** | — | 0 | 0 | 0 |
| App, `beranda` and `shell-beranda` at 1440, 1280, 1199, 1100, 1024, 768, 767, 390, 320 × 2 themes | 36 | **2,016 of 2,016** | 818 of 832 | 0 | 0 | 0 |
| App after the last fix (`shots/final`), 1440/390 × 2 themes | 12 | 974 of 974 | — | 0 | 0 | 0 |

- Geometry: first content `y = 192` at 1440 (shell checklist); reading column and 352 px panel 24 apart from 1200 px;
  one column with the panel at full width below; sections 48 apart (32 on the phone); hero label row 32, figure Manrope
  44/52 600 −1.1 px (32/40 at 320), line 14/20 `--muted` 8 px below, split bar 8 px 24 below with 2 px gaps, legend 16
  below in 4 columns (2 below 1024) with gap 24 (16 on the phone), supporting figures Inter 20/28 600 −0.2 px; rows 56
  (64 on the phone), padding 12, gap 16, `--line` separators, time 40, status 112, amount ≥ 120; day headings 40 px,
  sticky at 56, `--s0`; panel `--s1`, radius 12, padding 24 (16), blocks ruled 24/24; Kakeibo rows 16 px column, gaps
  8, padding 12, 2 px bars; Rincian 480 px centred (full-width sheet with the grab at 390), header 24 24 16, rows 44,
  total 56 with the 1 px strong rule, footer 16 24 with "Lihat transaksi siklus ini".
- Global rules on every app capture: Inter and Manrope only; sizes 12, 14, 20, 24, 44; weights 400, 500, 600; 7 or 8
  text styles in `main`; one 44 px figure and none other ≥ 33 px; one "Rincian"; no dotted figure; no border ≥ 2 px;
  the only coloured edge is the 1.5 px dashed Belum diklasifikasi marker (D8); every amount tabular.
- The phase 2 overflow is resolved: "figures inside their blocks" is 0 at 1024, 1100 and 1199 (and every other width),
  and nothing scrolls sideways at any width.
- The 14 shell failures are "content inside the frame" from 1199 px down: the list rows' −12 px bleed (`.list`, SPEC
  §7) passes the frame by 11 px inside the page margin. The prototype fails the same check the same way at 1024, 390
  and 320 (`shots/prototype-shell`, 3 of 3), so this is the design, not an overflow.
- Component sheet (`komponen-beranda`, 5 captures, 0 axe): the hero, split bar, legend, figure blocks, rows and day
  heading checks pass, **70 of 71** at 1440 and 1024 and **58 of 59** at 390; the one failure is the sheet's
  two-row Kakeibo sample. The page-level checks (one Rincian, panel landmark, page links) do not apply to the sheet.

## Accessibility and keyboard (task 4.3)

- axe-core: 0 serious or critical on every capture above, in both themes at 1440, 1024 and 390, with Rincian open, with
  the Pengeluaran line open and with hidden balances; also 0 in the unit tests (normal, hidden, failure, dialog open).
- Console errors: 0 on every capture.
- `beranda-keys.mjs` at 1440 light and dark and 390 light: **33 of 33** (`…/markas-beranda/keys/beranda-keys.json`):
  Tab order hero → Perlu tindakan → Aktivitas terbaru → panel, with Rincian the first stop; "Buka Tinjau" precedes its
  rows, as the prototype's DOM has the header before the list (D14's sentence lists it after the rows); "Semua
  transaksi" follows the activity rows; no focused control under the top bar, phone header or a sticky day heading;
  Enter opens Rincian with focus inside, a line opens with Enter and closes with Space, Tab stays inside, Esc and the
  close button close it with focus back on Rincian; a row opens the transaction dialog and focus returns to the row.

## Figure parity (task 4.4)

`parity.mjs`, before `e8ec62d` and after, on the parity seed in the same minutes (`…/parity`, repeated on the final
build in `…/parity-final`): **17 of 18 routes identical** (Tinjau 15, Transaksi 128, Analisis 72, Laporan 105,
Rekening 62, Target 34, the Pengaturan sections, Transaksi with the shell 127). Beranda: 68 figures before, 40 after;
every value that remains is equal, **0 changed**. Between the two runs Pengeluaran rose by Rp 28.000 on both builds
alike, because a seeded row at 08.33 today passed the server's "now"; parity compares builds within one run.

| Figure (before) | After |
| --- | --- |
| Aman dipakai Rp 0, "lebih Rp 1.726.424 dari uang masuk" | kept: hero Rp 0 and its line; Rincian total −Rp 1.726.424 |
| Masuk Rp 9.518.400 (KPI, "dari … masuk") | kept once in the legend; the second occurrence is the split bar's accessible name and the Rincian line |
| "terbesar Gaji Rp 9.500.000" | moved to the Masuk disclosure of the Rincian (Gaji Rp 9.500.000) |
| Pengeluaran Rp 3.422.490 × 4 (KPI, money card × 2, Kakeibo header) | kept once in the legend; duplicates removed; Rincian line |
| Ditabung & investasi Rp 2.250.000 × 5 (KPI, caption, money card × 2, Tabungan row) | kept once in the legend; caption "Tabungan bertambah" (decision 9); Rincian line and parts |
| Terjadwal Rp 5.572.334 (money card) | kept in the legend; Rincian line with each bill and set-aside |
| Aman dipakai Rp 0 in the money card, over-by line | duplicates of the hero, removed |
| Kekayaan bersih Rp 45.828.280, aset Rp 58.971.280, utang Rp 13.143.000 | kept in the panel |
| Kakeibo five buckets | kept in the panel; Tabungan row removed (in the legend) |
| Ketahanan Dana 8,1 bulan, Rp 19.412.400 | kept in the panel |
| "target Rp 14.387.520" | moved: "target 6 bulan" (decision 8); the rupiah target stays on Target & Tagihan |
| "Rp 490.500 menunggu jawaban", "4 perlu ditinjau" | moved to the Rincian note and "Menunggu jawaban · Rp 490.500"; "Buka Tinjau · 4" |
| Waiting rows in Aktivitas (Rp 300.000, −Rp 43.000, −Rp 120.000) | moved: Perlu tindakan (TRF BIFAST −Rp 120.000) and the Rincian waiting lines (Rp 300.000, Rp 43.000, Rp 27.500); decision 5 |
| Group "2 transaksi · Rp 70.500", plans Kelas Yoga Rp 150.000, SPayLater Rp 434.000, Token Listrik | beyond the three Perlu tindakan rows (D6); reached through "Buka Tinjau · 4" and the bell; the plans are listed in the Terjadwal disclosure |
| Row dates "5 Nov · …", "6 Nov · …", "8 transaksi" | replaced by day headings and the time column; "hari ini dan kemarin · 7" counts the answered rows |
| New in the after list: "24 Okt", "23 Nov" × 2 | the title block's span and the Terjadwal caption (decision 2, 10) |
| New: two 07.55 rows (Biaya admin −Rp 1.000, movement Rp 200.000) | answered rows that the eight-row cap hid behind waiting rows before |

Rincian against today's `remainingExplanation`: Masuk Rp 9.518.400 (was "+Rp 9.518.400"), Pengeluaran −Rp 3.422.490,
Ditabung & investasi −Rp 2.250.000, Terjadwal −Rp 5.572.334, total −Rp 1.726.424 (was "Sisa"), and the waiting note
"4 hal (Rp 490.500) … Rp 190.500 di antaranya sudah termasuk" — equal values (two sentences since the 2026-11-06 fix).

## Real-data shape (task 4.5)

The audit seed (`raka_contoh`) at 1440, 1100, 1199, 1024, 390 and 320 (both themes at 1440, 1024, 390, 320), then the
same after inserting one income of Rp 1.234.567.890 in the disposable database (Masuk Rp 1.242.003.890, Kekayaan bersih
Rp 1.301.029.739, hero Rp 1.235.365.139), then with the owner's currency set to USD:

| Set | Captures | Beranda checklist | axe | Console | Overflow |
| --- | --- | --- | --- | --- | --- |
| Audit seed | 20 | 1,044 of 1,044 | 0 | 0 | 0 |
| Stress amount (with the Rincian) | 30 | 1,344 of 1,344 | 0 | 0 | 0 |
| USD | 20 | 1,100 of 1,100 | 0 | 0 | 0 |

No clipping, overlap or sideways scroll; long names ellipsize in rows. In this first gate the 13-character stress
figure "Rp 1.242.003.890" wrapped under "Rp" in its 154 px column at 1024. The re-run for the owner's fixes found the
narrowest legend between 1200 and 1383 px, and owner decision 13's two-column fallback now applies there (below).
Images: `…/markas-beranda/compare-audit/`.

## Frontend and repository checks (task 4.6)

On `f08cced`:
- `npm test`: **82 files, 802 tests passed**. New or rewritten: `app/page.test.tsx` (36: period line, hero figures and
  line states, one Rincian, Rincian lines, disclosures and waiting lines, savings decrease, Perlu tindakan order, cap,
  targets, due plan dialog, empty and error, Aktivitas grouping, statuses, amounts, drawer and focus, deleted row,
  movement routes, panel blocks and states, DOM order, loading, failure, axe, hidden balances),
  `activity.test.ts` (+3), `explain.test.tsx` (+7 Rincian dialog), `primitives.test.tsx` (MaskedMoney, SplitBar,
  figure blocks, rows, day heading, stylesheet geometry), `copyRegister.test.ts` (Beranda strings).
- `npm run type-check`: passed. `npm run lint`: passed. `npm run build` (production, no component sheet): passed.
- `openspec validate --all --strict`: **84 passed, 0 failed**. `git diff --check origin/main`: clean.
- `git diff --stat origin/main -- backend db mobile nginx`: empty.
- `rg` finds no `MoneyBar`, `BerandaKpis`, `MoneyCard`, `KakeiboCard`, `DanaCard`, `BerandaSkeleton`, `TodoCard`,
  `ActivityCard`, `.ws-home-grid`, `.ws-money-legend`, `.ws-moneybar`, `.ws-legend`, `.ws-todo*`, `.ws-act-*`,
  `.ws-kk-rows`, `safeExplanation`, `waitingExplanation`, `remainingExplanation`, `netWorthExplanation` or
  `savingChangeLabel`; the kept `.ws-kcards` (Transaksi), `.ws-kk` (Transaksi, drawer), `.ws-dana*` (Target),
  `.ws-ava` (Tinjau, Transaksi) and `.ws-xn` (Analisis, Rekening) still have users.

## Deviations and notes for the owner

- No change outside an approved decision was needed. Choices made inside the design's latitude:
  1. The split bar's percentages read "37%" (prototype, standard Indonesian), where D4's example writes "37 %".
  2. The Rincian's over-by note drops "hari ini" from today's text: "Periode ini sudah lebih Rp … dari uang masuk, jadi
     belum ada yang aman dipakai."
  3. The waiting lines say "sudah dihitung di Pengeluaran sebagai …" / "belum dihitung" instead of today's
     "… Keluar", as the spec scenario requires.
  4. The prototype's calendar, swap, chevron-down and alert drawings are added to the shared icon set; the calendar
     drawing therefore also changes on Transaksi's stepper, the Target and Kalender headers and Rekening's menu.
- (Resolved by the owner's fix 3 below: an overdue or due-today plan now ranks above the automatic-link check and the
  pairing row.)
- Interim inconsistency (as planned): Beranda shows expenses in the ink, Transaksi still in coral until phase 5.
- Not verified: zoom to 400 % and a real screen reader; the 1199/1100 captures cover the reflow.

## Owner fixes, 2026-11-06

The owner approved three fixes the same day ("ok"). They are recorded in `proposal.md` › Approval, `design.md` D4, D5,
D6, D10 and decisions 10 and 13, and the `beranda-cycle-summary` delta (new scenarios "Terjadwal caption", "Period
over" and "Due plans come before checks and pairing"; "Items and order" updated).
1. **Terjadwal caption:** "termasuk setoran · 23 Nov" with goal set-asides, "tagihan sampai …" without.
   - Measured on the app: the longest caption needs 150 px and "Rp 1.234.567.890" 174 px.
   - Beside the panel the reading column is 568 to 671 px between 1200 and 1383 px wide. There four legend columns are
     only 124 to 149 px, so a caption still wrapped at 1280 and the stress figure passed its block there.
   - Owner decision 13's fallback is therefore applied: `@container hero (max-width: 671px)` shows two columns. The
     four columns stay at 1384 px and up (1440: 728 px), at 1024 to 1199 px and below 1024 px as before.
   - The checklist has a new "caption on one line" check for every supporting figure. It passes at 1440, 1400, 1380,
     1280, 1199, 1100 and 1024 with the parity seed, the stress amount and USD.
   - A ten-digit figure may still wrap under "Rp" in the four columns at 1440 and 1024, inside its block, as the
     prototype's `.t-fig` would.
2. **Over-period total:** after the four lines the Rincian reads "Lebih dari uang masuk Rp 1.754.424" under the strong
   rule, then "Aman dipakai Rp 0" as the result. No negative "Aman dipakai" remains anywhere. The normal case is
   unchanged.
3. **Perlu tindakan ranking (D6):** questions, plans past due, plans due today, awaiting transfers, ambiguous pairs,
   groups, the automatic-link check, then plans due within two days; the first three are shown. On the parity seed the
   rows are TRF BIFAST, Kelas Yoga (Lewat jatuh tempo) and Tagihan SPayLater (Jatuh tempo hari ini).
4. **Waiting note:** two sentences, "4 hal (Rp 490.500) masih menunggu jawaban. Rp 190.500 di antaranya sudah termasuk."

Re-run on `f185584` (the parity seed again shifted to 2026-11-06; Pengeluaran Rp 3.450.490 because a seeded row at 08.33
had passed the server's "now"):

| Set | Captures | Beranda checklist | axe serious/critical | Console | Overflow |
| --- | --- | --- | --- | --- | --- |
| Parity seed: `beranda`, `beranda-sembunyi`, three Rincian scenarios at 1440, 1024, 390 × 2 themes, plus 1280 × 2, 1199, 1100 (`shots/fix-after`) | 50 | **3,168 of 3,168** | 0 | 0 | 0 |
| Audit seed + Rp 1.234.567.890, `beranda` and `dlg-rincian-aman` at 1440 × 2, 1400, 1380, 1280, 1199, 1100, 1024 × 2, 390 × 2, 320 (`shots/fix-stress`) | 24 | **1,680 of 1,680** | 0 | 0 | 0 |
| The same with USD (`shots/fix-usd`) | 12 | **1,389 of 1,389** | 0 | 0 | 0 |

- axe with Rincian open, with a line open and with hidden balances: 0 serious or critical in both themes.
- `beranda-keys.mjs`: **33 of 33**, including the Tab order through the revised Perlu tindakan rows.
- New composed images in `…/markas-beranda/compare-fix/`: 24 prototype/app pairs at 1440, 1024 and 390, both themes, with
  and without hidden balances, page and Rincian. Stress and USD pairs at 1440, 1280 and 1024 are in
  `…/markas-beranda/compare-fix-audit/`.
- On `f185584`: `npm test` **82 files, 805 tests passed**, `npm run type-check`, `npm run lint` and `npm run build`
  passed. `openspec validate --all --strict` gives 84 passed, 0 failed. `git diff --check origin/main` is clean, and
  `git diff --stat origin/main -- backend db mobile nginx` is empty.
- New tests:
  - `page.test.tsx`: ranking, due plans above checks and pairing, the automatic-link check and an upcoming plan when
    nothing ranks above them, the over-period total, the two-sentence waiting note, the new caption;
  - `explain.test.tsx`: the over row;
  - `primitives.test.tsx`: the container fallback.
- Figure parity was not re-run: the fixes change wording, order and the over-period total row, not a figure's value.
  The Rincian's over amount is `-remaining.total`, the same field as before.
- Visible difference added by the fixes: between 1200 and 1383 px the legend shows 2 × 2 figures where the prototype has
  four in a row.

## Not done here

4.7 (owner acceptance) and 4.8 (archive and push) wait for the owner. Nothing is pushed.
