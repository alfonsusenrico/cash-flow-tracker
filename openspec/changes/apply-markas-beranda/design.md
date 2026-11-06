## Context

Beranda (`/`, `components/beranda/`) shows, on the phase 2 shell:
- a five-up KPI strip (`BerandaKpis`): Aman dipakai sampai gajian, Masuk, Pengeluaran, Ditabung & investasi, Kekayaan bersih, each a dotted `ExplainableFigure`;
- a two-column grid (`.ws-home-grid`, `1fr 340px`): "Uang siklus ini" (`MoneyCard`: bar, four dotted amounts, formula line) and "Aktivitas terbaru" (`ActivityCard`) on the left; "Perlu tindakan" (`TodoCard`), "Kakeibo siklus ini" (`KakeiboCard`, with a dotted Pengeluaran total and a Tabungan row) and "Ketahanan Dana" (`DanaCard`) on the right;
- below 900 px of main width, `useWidthAtMost` re-renders the cards in phone order.

Every figure comes from `GET /api/dashboard/cycle-summary` (`backend/app/services/cycle_summary.py`), except Ketahanan Dana, which comes from `GET /api/dashboard/overview` `kpis`. Perlu tindakan reads `/review/queue`, `/recurring` and `/recurring/pending`, and Aktivitas reads `/transactions?from_date=<yesterday 00.00 WIB>&limit=60`, consolidated by `consolidateLedgerMovements`.

The prototype (`option-6/js/pages.js` `PAGES.beranda`, `styles.css` §Hero, Bars, Rows, Panel, Phone) is one reading column (`hero`, `Perlu tindakan`, `Aktivitas terbaru`, 48 apart) beside a 352 px panel. The audit (`AUDIT.md` §Beranda) and the inventory (`INVENTORY.md` §1) mark the KPI strip and "Uang siklus ini" as R → merge, the supporting figures and Kekayaan bersih as M, and Kakeibo and Ketahanan as N (compact).

Phase 2 recorded for this phase:
- Beranda's Kekayaan bersih overflows its KPI card by 24 px between 1024 and about 1100 px;
- two Beranda figures pass the frame at 320 px;
- the masked figures' accessible name is due with each page's figure components (its owner decision 12).

Constraints:
- No money logic change: every figure is the server's field, formatted as today. Client arithmetic stays where it is today (the aset caption, the Kakeibo bar lengths) and is display only.
- `activity.tsx` helpers (`rowKind`, `rowTitle`, `rowStatus`, `movementRoute`, `RowAmount`, `rowWhen`, `jakartaDay`, `UNPAIRED_LABEL`, `movementDirection`, `accountLabel`) are imported by Transaksi, Rekening, Kalender and the transaction drawer. They keep their names and results; new Beranda-only presentation lives beside them.
- `ExplainableFigure` stays for Analisis (`RingkasanTab`) and Rekening (`WalletSummary`) until phases 6 and 8. `KpiCard` and `.ws-kcards` stay for Transaksi until phase 5.

## Goals / Non-Goals

**Goals:**
- Beranda matches the prototype at 1440, 1024 and 390 px in both themes, measured against `styles.css`.
- Every figure appears once, and the server figures the prototype shows keep their values.
- No figure leaves its block from 320 px up, including between 1024 and 1199 px.
- The shared pieces (figure blocks, split bar, panel, section header, list row, day heading, Rincian body, masked figure) are built once, for phases 4 to 9.

**Non-Goals:**
- Backend, API, data or companion changes; other page bodies; Catat and the transaction dialog; the bell.
- A per-day allowance unless the owner chooses it (Owner decision 1).

## Decisions

### D1. Page structure: reading column and context panel

`.cols` from `styles.css`: `grid-template-columns: minmax(0, 1fr) 352px; gap: 24px; align-items: start`, children `min-width: 0`.
- Reading column (`.stack`): hero, Perlu tindakan, Aktivitas terbaru, 48 px apart (32 px on the phone).
- Context column: the panel (D8), `aside` labelled "Ringkasan simpanan".
- Below 1200 px one column: the panel follows the reading column. At 1024 to 1199 px the panel is the full content width, which removes the phase 2 overflow (Kekayaan bersih gets 640 px or more instead of a fifth of the row).
- DOM order equals the visual and phone order, so `useWidthAtMost` and the two render branches in `BerandaView` go. Focus order follows the layout at every width without re-rendering.

Rejected alternatives:
- Keep the old grid and shrink the KPI figure: it fixes the symptom, not the five competing figures (audit finding 1).
- Reorder with CSS `order`: the reading and focus order would then differ from the visual order (WCAG 1.3.2 and 2.4.3).

### D2. Title block line

The shell's title block (phase 2 D4) receives:
- the title: today's date ("Kamis, 5 November", unchanged);
- the subtitle:
  - in an open cycle with a known expected salary, "Siklus gaji 23 Okt – 24 Nov · hari ke-14". The end date is `scheduled.window_end`, which the server sets to the day before the expected salary when `window_basis = "expected_salary"`;
  - in an open cycle without one, "Siklus gaji sejak 23 Okt · hari ke-14";
  - without a cycle, the current "November · siklus gaji belum tersedia". The note with the "Atur gaji di Pengaturan" link stays as a `t-meta` line above the hero.

There are no title-block actions, as in the prototype.

Rejected alternatives:
- Today's "Siklus sejak 23 Okt": the prototype names the cycle's span.
- Computing the end on the client from the payday day: the server already returns it.

### D3. Hero "Aman dipakai sampai gajian"

Markup and values from `styles.css` `.hero`:
- `section.hero`, labelled by its `h2`.
- **Label row** (`.hero-top`, flex, gap 8, min-height 32):
  - a 16 × 2 px `--m-income` mark 24 px above the label (`.sw-bar`, absolute);
  - `h2.t-label` "Aman dipakai sampai gajian" (12/16, 500, +0.06em, uppercase, `--muted`);
  - the quiet small "Rincian" button (h 28, padding 0 8, radius 6, 14/500 `--muted`; hover `--s1` + `--ink`; pressed `--s2`). Its accessible name is "Rincian aman dipakai sampai gajian", with `aria-haspopup="dialog"` and `aria-expanded`.
- **Figure** `p.t-hero`: 8 px below the label row; Manrope 44/52, 600, −0.025em, tabular (`--features-hero`), `--ink`; 32/40 below 360 px. Its value is `max(0, remaining.total)`, as today.
- **Line** `p.hero-sub.t-body`: 8 px below the figure; 14/20, `--muted`, proportional figures. The line reads:
  - available with days left: "20 hari lagi sampai gajian 25 Nov" (`safe_to_spend.days_left`, `expected_salary.date`). With Owner decision 1 (A) it is preceded by "≈ Rp 113.000 per hari · " (`safe_to_spend.per_day`);
  - next salary date unknown or irregular: "Tanggal gaji berikutnya belum diketahui";
  - salary late: "Gaji baru belum tercatat";
  - no salary source: "Sumber gaji belum diatur";
  - no cycle: "Siklus gaji belum ada" (reasons from `unavailableReason`, unchanged);
  - over: "Siklus ini sudah lebih Rp 300.000 dari uang masuk" (`-remaining.total`), with the figure at Rp 0. No negative safe amount is shown.
- The hero is the page's only 44 px figure; no other figure on the page is 33 px or more.

Rejected alternatives:
- The figure as a dotted button: the trigger moves to one "Rincian" (D5, audit finding 5).
- A coloured hero figure: `--ink` per SPEC §2; the green mark carries the meaning.

### D4. Split bar and supporting figures

**Split bar** (`.split`): 24 px below the line; height 8; segments separated by 2 px gaps; square; each segment at least 2 px wide; flex by amount:
- Pengeluaran `spending.amount`, `--m-expense`;
- Ditabung & investasi `max(0, fresh_saving.amount)`, `--m-invest`. A savings decrease takes no width, as today;
- Terjadwal `scheduled.total`, `--m-warning`;
- Aman dipakai `max(0, remaining.total)`, `--m-income`.

The bar has `role="img"` and the label "Uang siklus ini dari Rp 9.518.400 masuk: Pengeluaran 37 %, Ditabung & investasi 29 %, Terjadwal 10 %, Aman dipakai 24 %". The percentages are display-only rounding of server amounts against `masuk.total`. With `masuk.total = 0` the label says "belum ada uang masuk" and lists no percentages. With every segment at 0 the bar is not drawn.

**Legend** (`.legend`): 16 px below the bar; `repeat(4, minmax(0, 1fr))`, gap 24; 2 columns below 1024 px; on the phone 2 columns, gap 16, Masuk and Terjadwal hidden (`.is-phone-hide`). Each figure block (`.fig-block`) holds:
- a label stack (column, gap 4): the 16 × 2 px segment colour bar, then the label `t-meta`. Masuk has the bar slot without a colour, so labels align;
- the figure `t-fig`: Inter 20/28, 600, −0.01em, tabular, `--ink`;
- the caption `t-meta`, 4 px below.

| Figure | Value | Caption |
| --- | --- | --- |
| Masuk | `masuk.total` | cycle: "Gaji 23 Okt" (`period.start_date`); month: "sejak 1 November" |
| Pengeluaran | `spending.amount` | cycle: "sejak gaji 23 Okt"; month: "sejak 1 November" (today's `sinceCaption`) |
| Ditabung & investasi | `abs(fresh_saving.amount)` | "Tabungan bertambah" or "Tabungan berkurang" from the sign of `tabungan.total`, or of `fresh_saving.amount` for an older API (Owner decision 9) |
| Terjadwal | `scheduled.total` | "tagihan sampai 24 Nov" (`scheduled.window_end`); "termasuk setoran · 24 Nov" when goal set-asides are included (Owner decision 10, revised 2026-11-06 so the caption stays on one line at every width) |

No figure here is a button. Each figure's breakdown is in the Rincian dialog (D5).

**Width check at 1024 px:** the reading column is 688 px, so each legend column is 154 px. Inter 20/600 tabular "Rp 123.456.789" measures about 150 px and fits. The gate measures "Rp 1.234.567.890" and the USD display at 1024 (Owner decision 13).

Rejected alternatives:
- Keep "Uang siklus ini" as its own card: it repeats the four figures (audit finding 6).
- Percentages as visible text: the prototype shows amounts; the percentages stay in the accessible name.

### D5. "Rincian" dialog for the safe amount

The dialog is built on the existing shared dialog primitive (centered at 768 px and up, bottom sheet below):
- 480 px wide, radius 16, 1 px `--line` edge, focus trapped, Esc and the close button close it, and focus returns to "Rincian".
- **Header** (padding 24 24 16): the title "Aman dipakai sampai gajian" in Manrope 20/28, 600, with the meta "Rincian hitungan" (12/16 `--muted`) under it.
- **Body** (padding 0 24 24, blocks 24 apart):
  - the rule in `t-body`: "Masuk dikurangi Pengeluaran, Ditabung & investasi dan tagihan terjadwal sampai akhir siklus." (prototype wording; the month basis says "sampai akhir bulan");
  - a plain list. Four rows (min-height 44, padding 0, label 14/400, amount `t-amt` 14/600 tabular): Masuk `Rp 9.518.400` (no plus, as in the prototype), Pengeluaran `−Rp …`, Ditabung & investasi `−Rp …` (or "Tabungan berkurang" with `Rp …`), and "Terjadwal sampai 24 Nov" `−Rp …`;
  - the total row (min-height 56, 1 px `--line-strong` top rule): "Aman dipakai" `t-strong` and `remaining.total` as `t-fig` 20/600;
  - when the period is over (`remaining.total < 0`, owner fix 2026-11-06): the row after the four lines takes the 1 px `--line-strong` top rule and reads "Lebih dari uang masuk" `t-strong` with the positive amount `-remaining.total` as `t-amt` (min-height 44); the total row follows it, without the strong rule, as "Aman dipakai" `Rp 0`, equal to the hero. No negative "Aman dipakai" is shown;
  - under the list, at most two `t-meta` notes: the expected-salary basis ("Gaji berikutnya diperkirakan 25 November, dari gaji yang dikonfirmasi …", today's `expectedBasisNote`) and the waiting note in two sentences ("4 hal (Rp 190.500) masih menunggu jawaban. Rp 45.000 di antaranya sudah termasuk.", today's `waitingNote`). The over-by and unavailable reasons from today's explanations replace the basis note when they apply.
- **Footer** (padding 16 24, 1 px top line): the secondary button-link "Lihat transaksi siklus ini" (or "bulan ini") to `/ledger?period=<basis>`.

**Line breakdowns (Owner decision 3, default A):** each of the four rows whose figure has server lines is a disclosure button (`aria-expanded`) with a 16 px `chevD` in `--muted` after its amount. Opening it shows the server lines in place, as 12/16 rows indented 16 px:
- Masuk: `masuk.lines`;
- Pengeluaran: the five `spending.lines`, each linking to its `href` (ledger with `kakeibo=<bucket>`);
- Ditabung & investasi: the `tabungan` parts (Masuk ke tabungan, Diambil dari tabungan, Pengeluaran dari tabungan, Pengeluaran berpilar Tabungan), else `fresh_saving.lines`;
- Terjadwal: each plan "Tagihan: … · jatuh tempo 12 Nov" and goal set-aside "Setoran target: … · Rp 600.000 per bulan, sudah masuk Rp 400.000", with its window note.

Collapsed, the dialog looks like the prototype apart from the chevrons. These are the existing `spendingExplanation`, `savingExplanation`, `masukExplanation` and `scheduledExplanation` lines, so nothing is computed anew, and the requirements "Keluar Figure" and "Scheduled Amount" stay satisfied without change.

When rows wait for an answer, a fifth disclosure "Menunggu jawaban · Rp 190.500" sits under the notes. It lists each waiting line with its Tinjau section and whether it is already counted, and under which bucket (today's unused `waitingExplanation` lines), with "Jawab di Tinjau". This keeps "Waiting-for-Answer Figure" verifiable after the amount leaves the Perlu tindakan header (D6).

- One "Rincian" per page (SPEC §13). The Kekayaan bersih, Ketahanan Dana and Kakeibo blocks have no Rincian. Their explanations are the Rekening and Target pages (phases 8 and 9), which the blocks link to.
- With hidden balances every amount in the dialog is masked (D11).

Rejected alternatives:
- Strict prototype rows only (Owner decision 3, option B): this drops, from the whole app, the savings-change parts (they exist only on Beranda today), the set-aside funded amounts and the per-category Masuk.
- Dotted figures on the supporting figures (option C): it brings back the audit's helper noise and breaks "Rincian ≤ 1 per page".
- Keeping the anchored popover: the prototype's Rincian is a modal dialog. The owner prefers a centered modal or inline expansion (2026-11-04).

### D6. "Perlu tindakan"

Section header (`.sec-h`: min-height 32, margin-bottom 12; `h2.t-label` "Perlu tindakan"; at the right the link button "Buka Tinjau · 6" to `/review`, where N is `queue.counts.attention`; with N = 0 it reads "Buka Tinjau").

Rows are the shared list row (D7 anatomy) with an icon column (`.r-ico` 40 × 20, 16 px icon, `--muted`), the title, the meta line, the amount and a 16 px chevron. Each row is one `a` or `button`, and its accessible name is its title, meta and amount. Items keep today's targets (`todoItems`); their order is the ranking below:

| Item | Icon | Title | Meta | Amount | Target |
| --- | --- | --- | --- | --- | --- |
| Question | help | merchant, notes or "Transaksi tanpa nama" | "Belum dikenali · 4 Nov 18.23" (or "hari ini 18.23") | the row's amount with its sign and colour (D7) | `/review?item=<id>#review-questions` |
| Awaiting transfers | swap | "Pasangkan 2 pindah saldo" | "Pindah saldo belum lengkap · BCA, Jago" | none | `#review-awaiting` at the first leg |
| Ambiguous pairs | swap | "2 pasangan pindah saldo belum pasti" | "Pilih pasangan yang benar" | none | `#review-ambiguous` |
| Group | help | the group label | "Perlu jawaban · 3 transaksi · 4 Nov 11.43" | `group.total` | `#review-groups` |
| Auto-linked plans | calendar | "2 rencana ditautkan otomatis" | "Cek dan batalkan kalau keliru" | none | opens "Rencana jatuh tempo" |
| Plan due or overdue | calendar | plan name | "Jatuh tempo hari ini · Rencana" or "Lewat jatuh tempo · Rencana" in `--warning` | `plan.amount` | opens "Rencana jatuh tempo" (link, skip, record unchanged) |
| Plan due within two days | calendar | plan name | "Besok · Rencana" or "12 Nov · Rencana" | `plan.amount` | `/goals` |

- **Order** (owner fix 2026-11-06, so an overdue or due-today plan is never hidden behind the automatic-link check or the pairing row): one ranked list,
  1. open questions, in the queue's order;
  2. plans past their due date, earliest due first;
  3. plans due today;
  4. awaiting transfers ("Pasangkan N pindah saldo");
  5. ambiguous pairs;
  6. groups that need an answer, in the queue's order;
  7. the automatic-link check ("N rencana ditautkan otomatis");
  8. plans due within two days, earliest first.
- **At most three rows** (prototype): the first three of that list. The rest is reached through "Buka Tinjau · N" and the bell. On the parity seed the rows are the open question, Kelas Yoga (overdue) and Tagihan SPayLater (due today).
- **Empty:** "Tidak ada yang perlu dijawab." (`t-body`, `--muted`), no card. **Error:** "Daftar Tinjau belum bisa dimuat." (`t-meta`).
- **Removed:**
  - the per-item action pills ("Jawab", "Pasangkan", "Pilih", "Cek", "Atur", "Lihat"): the whole row is the action;
  - the count chip;
  - "Rp … menunggu jawaban" beside the title (moved into the Rincian, D5);
  - "Semua sudah ditinjau / Selesai";
  - the "N dicatat otomatis hari ini" note (Owner decision 4).

Rejected alternatives:
- Keep the pills next to whole-row links: two targets for one action, and coloured pills that SPEC §7 replaced.
- Show every item: the prototype caps at three, and the bell already lists every due plan.

### D7. "Aktivitas terbaru" and the shared list row

**Data:** the same query and consolidation. Rows with `review_status = "needs_review"` are left out (prototype: "reviewed rows only — pending ones are in Perlu tindakan"; Owner decision 5). Then the newest eight are kept (`ACTIVITY_LIMIT`, unchanged), grouped by Jakarta day.

**Section header:** "Aktivitas terbaru" with the aside "hari ini dan kemarin · 5" (12/16 `--muted`, tabular count).

**Day heading** (`.day-h`):
- height 40; sticky under the top bar (`top: var(--topbar-h)`; 56 under the phone header); `--s0` with a 1 px bottom line; padding 0 12 with a −12 bleed;
- `h3.t-label` in `--ink`: "KAMIS, 5 NOVEMBER · HARI INI" or "· KEMARIN";
- no totals on Beranda (prototype);
- groups 24 apart.

**List row** (`.list` and `.row`; shared, phase 5 reuses it with the category column):
- min-height 56 (64 on the phone); padding 0 12 with a −12 bleed; gap 16; 1 px `--line` bottom, none on the last row;
- states: hover `--s1` (`--s2` inside the panel), pressed `--s2`, selected `--s2` plus the 2 px bar; the keyboard focus is a 2 px `--focus` outline inset −2;
- the whole row is one `button` that opens the shared transaction dialog for that row, as today; focus returns to the row on close.
- Desktop columns (compact variant): time `r-time` 40 (12/16 `--muted`, tabular, "18.23"), then `r-main` (title 14/400 with ellipsis, then the inline meta 12/16 `--muted`), then status `r-pillar` 112 (dot 8 plus 12/16 text), then amount `r-amt` at least 120 (right-aligned, `t-amt` 14/600 tabular).

| Kind | Title | Meta (desktop) | Status column | Amount |
| --- | --- | --- | --- | --- |
| Expense | merchant, notes or category (`rowTitle`) | account with its bank (`accountLabel`) | pillar dot and name (`rowStatus`) | `−Rp …` in `--ink` |
| Income | as above | account | "Uang masuk" (Owner decision 6) | `+Rp …` in `--income` |
| Movement | "Jago · Main Pocket → BCA · ATM" (`movementRoute`) | notes, or "Pasangan belum ditemukan" in `--warning` | "Tidak dihitung" | `Rp …` in `--transfer` |
| Top up | "Top up Investasi" (Owner decision 12) | source account | Tabungan dot and "Tabungan" (Owner decision 6) | `Rp …` in `--invest` |

- **Phone** (below 768 px): two lines, the title (14/400) and the meta "18.23 · Kebutuhan" (time · status; income shows its category, movements "Pindah saldo" or the unpaired label in `--warning`). The time and status columns are hidden, and the amount has no minimum width.
- **Footer:** `.more-row` (centered, padding 16 0) with the secondary button-link "Semua transaksi" to `/ledger`.
- **Empty:** "Belum ada transaksi hari ini dan kemarin." **Error:** "Aktivitas belum bisa dimuat."
- **Removed:** the avatar or icon tile, the two-line desktop row, the right-hand "4 Nov · 18.23", the "account · category" subtitle (category stays in the transaction dialog and on Transaksi), and the header link.

New Beranda presentation functions (`activityMeta`, `activityPhoneMeta`, `activityStatus` and `groupByDay`, names indicative) sit beside the shared helpers and do not change their results.

Rejected alternatives:
- Reusing `LedgerList` from Transaksi: it is the pre-redesign table until phase 5.
- Keeping waiting rows with "Menunggu jawaban" (Owner decision 5, option B): this is the current spec, but the prototype moves them to Perlu tindakan to show each fact once.

### D8. Context panel

The panel is an `aside.panel`: `--s1`, radius 12, padding 24 (16 on the phone). Blocks are separated by a 1 px `--line` rule with 24 px above and below it.

1. **Kekayaan bersih.** The whole block is an `a` to `/accounts`:
   - the header row: `h2.t-label` "Kekayaan bersih" and a 16 px `chevR` in `--muted`, margin 0;
   - `p.t-fig` `net_worth.total` (20/28, 600, tabular), using `balance()` so a negative value reads "−Rp …";
   - `p.t-meta` "aset Rp … · utang Rp …", with today's expressions: aset `liquid + dana_investasi + investment − reconciliation_pending`, utang `debts + credit_owed`.

   The figure is not a button; its explanation is Rekening's (phase 8). At 352 px the block's content width is 304 px, enough for "−Rp 999.999.999.999".
2. **Ketahanan Dana.** The whole block is an `a` to `/goals`:
   - `h2.t-label` "Ketahanan Dana" with `chevR`;
   - `p.t-strong` "5,4 bulan biaya pokok" (`coverage_months ?? runway_months`, one decimal, id-ID);
   - `p.t-meta` "dana darurat Rp 20.412.400 · target 6 bulan". The first part reads "saldo kas …" when `emergency_balance_basis = "all_liquid"`. The target is `target_months` (Owner decision 8).
   - Unknown: `t-strong` "Belum bisa dihitung" with the `essentialBasisText` reason as `t-meta`, no number and no sentinel. Error: `t-meta` "Ketahanan Dana belum bisa dimuat." The coverage bar is removed.
3. **Kakeibo siklus ini** (or "bulan ini"):
   - `.sec-h` with the "Analisis" link button to `/insights`;
   - five `.kk-row`s from `spending.lines` (Kebutuhan, Keinginan, Budaya, Tak Terduga, Belum diklasifikasi). Each row is a grid `16px 1fr auto`, column gap 8, row gap 8, padding 12 0, holding the 8 px pillar dot (Belum diklasifikasi keeps phase 1's dashed grey marker), the label `t-body`, and the amount `t-amt`;
   - a 2 px bar under label and amount, `--s2` track, the pillar colour, its length against the largest of the five (display only, as today);
   - when Belum diklasifikasi holds spending, the bar's place shows "Perlu dipilih di Tinjau" (12/16 `--accent-text`) linking to `/review`;
   - no total and no Tabungan row; the saving bucket is Ditabung & investasi in the hero;
   - each label keeps its server `href` (ledger `kakeibo=<bucket>` for the period) as a link styled as `--ink` text, underlined on hover (Owner decision 7).

Rejected alternatives:
- Kekayaan bersih as a supporting figure in the hero: the prototype puts it in the panel, and the hero's figures add up to Masuk.
- A full Ketahanan card with its bar: that belongs to Target & Tagihan (phase 9; inventory N).

### D9. Phone composition (below 768 px)

The order:
1. the phase 2 phone header;
2. the title block (24/24 padding);
3. the hero with Pengeluaran and Ditabung & investasi;
4. Perlu tindakan;
5. Aktivitas terbaru with two-line rows;
6. the panel.

Sections are 32 apart, the page has 16 px side padding, and the bottom padding is `tabbar-h + 48` so the tab bar never covers the last block. Day headings stick under the 56 px phone header. Rincian opens as a bottom sheet (radius 16 16 0 0, grab 32 × 4, max-height 92vh). Every target is at least 44 × 44 except the inline text links, which keep 24 × 24. At 320 px nothing scrolls sideways, and the hero is 32/40 below 360 px.

Masuk and Terjadwal are not shown as supporting figures on the phone (prototype). They are the first and fourth lines of the Rincian.

### D10. What happens to each removed or merged element

| Current element | Phase 3 | Where its content goes |
| --- | --- | --- |
| Five-up KPI strip | removed | Aman → hero; Masuk, Pengeluaran, Ditabung & investasi → legend; Kekayaan bersih → panel |
| KPI captions "terbesar Gaji Rp …" and "Tabungan bertambah Rp …" | replaced | "Gaji 23 Okt" and "Tabungan bertambah" (the amounts repeat a figure); the top income line is in the Masuk disclosure |
| "Uang siklus ini" card | merged | the bar → split bar under the hero; the four amounts → the legend; "dari Rp … masuk" → the bar's accessible name and the Rincian Masuk line; the formula line → the Rincian rule; the "Tagihan" link → the Terjadwal disclosure and the bell |
| Explain triggers (ten in the code, 20 counted by the audit) | removed | one "Rincian" on the hero; each figure's lines → the Rincian disclosures (D5); Kekayaan bersih's explanation → Rekening (phase 8) |
| Kakeibo header "Pengeluaran Rp …" | removed | legend Pengeluaran |
| Kakeibo "Tabungan" row | removed | legend Ditabung & investasi, Rincian disclosure |
| Ketahanan Dana card, coverage bar, rupiah target | reduced | one panel block; bar and rupiah target on Target & Tagihan |
| Perlu tindakan count chip, waiting amount, action pills, "Semua sudah ditinjau / Selesai", auto-recorded note | removed or merged | "Buka Tinjau · N"; Rincian waiting note and disclosure; whole-row links; one calm line; Tinjau's "Dilewati otomatis" and Transaksi `review=auto` |
| Items beyond the first three of the D6 ranking | moved | "Buka Tinjau · N", the bell, and the Terjadwal disclosure for plans |
| Activity avatars, two-line desktop rows, "4 Nov · 18.23", "akun · kategori" | replaced | time column, day headings, account meta; category in the transaction dialog |
| Waiting rows in Aktivitas | moved | Perlu tindakan and Tinjau (Owner decision 5) |
| Icon tiles in card headers | removed | none (SPEC §7) |
| `useWidthAtMost` reorder, `.ws-home-grid`, `MoneyBar`, `.ws-money-legend`, `.ws-todo*`, `.ws-act-*`, `BerandaSkeleton` | removed | D1 and D13 |

### D11. Masked figures (phase 2 owner decision 12)

A `MaskedMoney` figure component wraps `bal()` for every Beranda figure: hero, legend, panel, rows and Rincian. With `hideBalances` it renders `<span class="mask" role="img" aria-label="Disembunyikan">Rp ••••••</span>` (`$ ••••••` for USD) with `letter-spacing: 0.08em` and the tabular feature settings. Without hidden balances it renders the plain text.
- `role="img"` makes the name valid (ARIA 1.2 prohibits `aria-label` on a generic `span`; axe reports `aria-prohibited-attr`). The prototype's bare `aria-label` is the reference for the name, not the markup (Owner decision 11).
- Accessible names that embed amounts (rows, the split bar label, "Rincian" lines) use the same masked text, so no amount leaks through a name. The split bar keeps its percentages, because the bar already shows the proportions.

### D12. Shared components and CSS

New components in `components/workspace/`, names indicative:
- `FigureBlock` (hero and supporting variants);
- `SplitBar`;
- `Panel` and `PanelBlock`;
- `SectionHeader`;
- `ListRow` (compact and full variants, phone two-line);
- `DayHeading`;
- `ExplanationDialog` (the Rincian body with disclosures);
- `MaskedMoney`.

Their CSS uses the phase 1 tokens (`--fs-*`, `--sp-*`, `--r-*`, `--m-*`, `--k-*`) with the prototype's class names where no app class exists. Every value comes from `styles.css`; the checklist (D14) compares computed values. The component sheet `/komponen` gains these pieces in both themes.

Removed: `MoneyBar` (its only user is Beranda) and the Beranda-only rules. Kept for later phases: `KpiCard`, `.ws-kcards`, `.ws-kk*`, `.ws-dana*`, `.ws-ava`, `.ws-xn`, the `ExplainableFigure` popover. `explanations.ts` keeps the builders the Rincian reads and folds `safeExplanation` into the basis note.

### D13. Loading and failure

- **Loading:** `aria-busy` placeholders in the final layout, using the SPEC skeleton (`--s2`, radius 6, no pulse):
  - the hero: a 12 px label bar, a 44 px figure bar 40 % wide, a 14 px line bar, the 8 px bar and four legend blocks;
  - three 56 px skeleton rows each for Perlu tindakan and Aktivitas;
  - the panel with three blocks.
- **Summary failure:**
  - the hero is replaced by the SPEC error box (`role="alert"`, 20 px alert icon, "Angka Beranda belum dapat dimuat", "Saldo dan transaksi tetap aman; coba muat lagi.", "Coba lagi" secondary). No Rp 0 is shown;
  - Perlu tindakan, Aktivitas and the Ketahanan Dana block render from their own data;
  - the Kekayaan bersih and Kakeibo blocks are omitted, because they come from the summary.
- **Overview, queue or activity failure:** each block says so in one `t-meta` line, as today.

### D14. Verification

The common gate of the plan, for the routes `beranda` and `dlg-rincian-aman`:
- **Side by side:** prototype `#/beranda` and `#/beranda?d=rincian:safeToSpend` against app `/` with the parity seed, at 1440 × 900, 1024 × 768 and 390 × 844, light and dark, with composed left/right images in `research/evidence/apply-markas-beranda/`. Captures at 1280, 1199, 768, 767 and 320 check the breakpoints.
- **Checklist** from D3 to D8 and SPEC §2, §5 and §7:
  - geometry: the first content `y = 192` at 1440, the reading column and panel 352, gap 24, stack 48 (32 phone);
  - the hero, legend, split bar, panel, Kakeibo row, day heading and list row values;
  - the Rincian dialog: 480, header, rows 44 and 56, footer.

  CSS values match exactly; positions that depend on text metrics may differ by 1 px.
- **Global rules (SPEC §13):** Inter and Manrope only; sizes ⊆ {12, 14, 16, 20, 24, 32, 44}; weights ⊆ {400, 500, 600}; ≤ 8 text styles; one 44 px figure and none other ≥ 33 px; one "Rincian"; no border ≥ 2 px or coloured border except the marks; no horizontal overflow at 1440, 1024, 390 and 320; every figure inside its block at 1024 to 1199 (the phase 2 defect); axe-core (wcag2a, wcag2aa, wcag21a, wcag21aa, wcag22aa) zero serious or critical in both themes, also with the Rincian open and with hidden balances; zero console errors.
- **Keyboard:** Tab order hero → Rincian → Perlu tindakan rows → "Buka Tinjau" → Aktivitas rows → "Semua transaksi" → panel links. Rincian opens and closes with Enter and Esc, and focus returns. The disclosures work with Enter and Space. A row opens the transaction dialog, and focus returns. Sticky headings do not cover the focused row.
- **Figure parity:** `parity.mjs` reads every money figure, count and date in `main` on the phase 2 build (`539b367`) and on this branch with the same seed and minute. The report lists:
  - figures kept (equal values);
  - removed duplicates (each named, with the kept occurrence it duplicated);
  - moved figures (waiting amount → Rincian, target months);
  - none changed.

  Rincian values are compared with today's explanation values.
- **Real-data shape:** the audit seed (17 pockets, long names, several platforms) and one stress figure (Rp 1.234.567.890) and USD at 1440, 1024, 390 and 320, with no clipping, overlap or overflow.
- **Frontend checks:** `npm test`, `npm run type-check`, `npm run lint`, `npm run build`, `git diff --check`, `openspec validate --all --strict`; `git diff --stat origin/main -- backend db mobile` empty.

The parity seed's known disagreements with the prototype (Ketahanan Dana, payday, saving rule, scheduled amount, Tinjau counting; phase 2 verification) are reported as evidence, not fixed. The app shows its own server figures.

### D15. Spec reconciliation

- `beranda-cycle-summary`:
  - MODIFIED: "Beranda Period", "Waiting-for-Answer Figure", "Safe-to-Spend Today", "Kerjakan Dulu", "Recent Activity on Beranda", "Beranda Loading and Failure";
  - REMOVED: "Uang Siklus Ini Card", "Kakeibo and Ketahanan Dana Cards", "Beranda KPI Cards";
  - ADDED: "Beranda Hero", "Rincian of the Safe Amount", "Beranda Context Panel";
  - "Keluar Figure", "Scheduled Amount" and "Cycle Summary API" are unchanged.
- `explainable-figures`: MODIFIED "Explainable Figure Trigger", "Explanation Content", "Explanation Presentation".
- `finance-dashboard-cockpit`: REMOVED "Executive KPI Summary Ribbon" (already superseded by "Beranda KPI Cards"; net-worth composition is in `net-worth-analytics`).
- `mobile-native-layout`: MODIFIED "Dedicated Mobile Home Information Architecture".
- `tactile-pulse-ui`: MODIFIED "Semantic Colour Meaning" and "No Decorative Effects".
- `theme-presets-switcher`: MODIFIED "Modern Button Styling System" (its last scenario).
- `investment-topups`: MODIFIED "Product entry and ledger presentation" (home feed).
- Not touched:
  - `kakeibo-reconciliation` item 9 stays true: the Rincian line reads "Tabungan berkurang" with its amount;
  - `kakeibo-classification-completeness` stays true with Owner decision 7 (A);
  - `emergency-cost-estimation` does not name the Beranda card.

## Owner decisions (recommended default in bold)

Decided on 2026-11-06: the owner approved the plan with every recommended default ("approved"), so each decision below takes its bold option. After the first verification the owner approved three fixes the same day ("ok"): decision 10's set-aside wording, the over-period total of the Rincian (D5) and the Perlu tindakan order (D6).

1. **Per-day amount in the hero line.** **(B) Leave it out. The line reads "20 hari lagi sampai gajian 25 Nov".** This follows the owner's 2026-11-04 rule ("show the safe amount for the whole period, not a daily or weekly allowance") and the approved "Safe-to-Spend Today" requirement; it is a deviation from the prototype. (A) The prototype's "≈ Rp 113.000 per hari · 20 hari lagi …" from the server's rounded `per_day`. The plan's scope names a "per-day line".
2. **Subtitle span.** **"Siklus gaji 23 Okt – 24 Nov · hari ke-14" with the server's window end, "Siklus gaji sejak 23 Okt" when the salary date is unknown**; or keep today's "Siklus sejak 23 Okt · hari ke-14".
3. **Breakdowns inside Rincian.** **(A) The prototype's four lines, each opening its server lines in place, plus a "Menunggu jawaban" disclosure; this deviates only by chevrons.** (B) Plain lines only, which drops the savings-change parts, set-aside funding and per-category Masuk from the app. (C) Dotted supporting figures.
4. **"N dicatat otomatis hari ini" note.** **Remove it (prototype)**; or keep it as one `t-meta` line under Perlu tindakan.
5. **Waiting rows in Aktivitas terbaru.** **Leave them out (prototype: "reviewed rows only"); they are in Perlu tindakan and Tinjau**; or keep them with "Menunggu jawaban".
6. **Status column wording.** **The app's words: income "Uang masuk", a top-up "Tabungan" with the saving dot, because both are counted (Masuk, Ditabung & investasi)**; or the prototype's "Tidak dihitung" for every row that is not an expense.
7. **Kakeibo labels.** **Keep each label's ledger link (ink at rest, underline on hover) plus "Perlu dipilih di Tinjau" when unclassified spending exists**; or plain labels as in the prototype (then `kakeibo-classification-completeness` needs a change).
8. **Ketahanan Dana target.** **"target 6 bulan" (`target_months`, prototype); the rupiah target stays on Target & Tagihan**; or keep "target Rp …".
9. **Ditabung & investasi caption.** **"Tabungan bertambah" or "Tabungan berkurang" without the amount, which the figure above already shows**; or keep the amount.
10. **Terjadwal caption.** **"tagihan sampai 24 Nov", and "termasuk setoran · 24 Nov" when goal set-asides are included** (revised by the owner on 2026-11-06 from "tagihan & setoran sampai 24 Nov", which wrapped); or always the prototype's "tagihan sampai …".
11. **Masked figure markup.** **`role="img"` with `aria-label="Disembunyikan"`**; or the prototype's bare `aria-label` (an axe finding); or visually hidden text.
12. **Top-up row title.** **"Top up Investasi" (the approved `investment-topups` wording)**; or the prototype's "Top up <product>".
13. **Legend at 1024 px with large amounts.** **The prototype's four columns. If the stress figure overflows at 1024 in the gate, a container query moves the legend to two columns below the measured width, recorded as a deviation**; or keep four columns and accept the overflow.

## Risks / Trade-offs

- [The safe-amount Rincian becomes the only place for the other figures' breakdowns.] → Its disclosures carry today's lines unchanged (D5). Parity compares them with today's explanation values.
- [Three items in Perlu tindakan hide the rest.] → "Buka Tinjau · N" shows the full count, and the bell lists every due plan.
- [Leaving waiting rows out of Aktivitas changes what the owner sees there.] → Owner decision 5, and the row count says how many are shown.
- [Interim inconsistency: Beranda rows show expenses in ink and Transaksi in coral until phase 5.] → It is recorded in the end-review list, and the `tactile-pulse-ui` delta names the scope.
- [The shared list row is designed for Transaksi, but Transaksi only adopts it in phase 5.] → The variants and the category column are built and shown on `/komponen` now, so phase 5 reuses them unchanged.
- [Sticky day headings in a short list can cover the focused row.] → `scroll-padding-top` includes the top bar and the 40 px heading; the keyboard check covers it (WCAG 2.4.11).
- [The parity seed disagrees with the prototype on some figures.] → Reported, not fixed (D14).

## Migration and rollback

No data, API or schema change. The deploy is the frontend image through the approved pipeline after the owner's merge. Rollback is a revert of the merge commit; no state migrates.

## Open Questions

None beyond the owner decisions above.
