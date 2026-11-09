## Context

See `proposal.md` for the motivation. This section records the state the design starts from.

**Base.** `origin/main` `0308b7b` holds phases 1 to 3, `add-category-system-keys` and `add-language-system`. Before this phase starts, `localize-server-text`, phase 4 (`apply-markas-tinjau`), phase 5 (`apply-markas-transaksi`) and phase 6 (`apply-markas-analisis`) merge (D25). This design names their pieces as they are planned; task 1.2 records every difference in the merged code.

**Laporan today** (`app/laporan/page.tsx`, `components/report/**`, `lib/monthlyReport.ts`, `lib/reportReady.ts`):
- URL `?basis=month|cycle&start=YYYY-MM-DD`, written with `replaceSearch`; `?lengkapi=1` opens the classification dialog and `?atur=anggaran` the budget editor on load. There is no `/reports` route or redirect; inbound links are the nav item, the bell's `url` from `GET /reports/monthly/latest`, and Analisis's `?atur=anggaran` and `?lengkapi=1`.
- Queries: `GET /reports/monthly/periods?basis=` and `GET /reports/monthly?basis=&start_date=` (the report, with `comparison` inside).
- The title block comes from `untranslatedPages.ts` ("Laporan", "Laporan bulanan Kakeibo"). `PageActions` holds the basis `Segmented` ("Bulan kalender" · "Siklus gaji", the latter only when cycles exist) and the `PeriodStepper` (newest-first list, offset 0 = the running period; the plain URL shows the latest closed one at offset −1, so "Bulan ini" is visible by default).
- Render order: a screen-reader `h2`, the info notice (running, or tracking started inside), the data-quality line (green "Data lengkap." when nothing is open), the KPI strip (running: three tiles; closed: Pengeluaran with two change chips, Masuk with one, Ditabung & investasi with "% dari uang masuk", Selisih), then `ReportLayout` (7fr/5fr, one column at ≤ 900 px by `useWidthAtMost`): pillar table, merchants, savings, spending details, budget; facts, monthly history (stacked pillar bars without a legend), income sources, the reflection panel (only when this or the previous period has a note), past reports.
- Every figure is a server field (`comparison.totals`, `pillars`, `facts`, `history`, `progress`, `baseline`, `previous`; `income.sources`; `savings`; `spending.*`; `budget`; `data_quality`; `reflection`, `previous_reflection`). The only browser computation is "% dari uang masuk" (`round(fresh_saving / inflow × 100)`).
- No dotted explain figure. Copy is hard-coded Indonesian. Facts and period labels are Indonesian server text until `localize-server-text`, which gives them codes (`facts.comparison.*`, `periods.*`) and renders them in the account's language.
- Known defects: the change chips format with a fixed `Intl.NumberFormat("id-ID")` and ignore hidden balances; the reflection panel cannot be opened without a note; "Rincian tabungan"'s two rows do not add up to Ditabung & investasi when money was paid from savings (`paid_from_savings` is not sent); the stepper and the KPI values overflow at 320 px.

**Prototype** (`option-6`, `pages2.js` 22–160): as summarized in the proposal. Where `SPEC.md` and `styles.css`/JS disagree, this design takes `styles.css`, except where a decision names a fix:
- `.q-num` is Inter 20/32 600 with no tracking (CSS), not the supporting-figure style;
- table rows are 48 px (`td`), though the prototype's pillar `th` says 44;
- pillar row headers and the total row render as `t-label` (uppercase muted), because they are `th` cells;
- the month bars' 160 px area holds the labels too, which clamps tall bars to the same 112 px (a prototype bug, fixed in D11);
- `.icon-btn:disabled` has no style in the prototype; the shared stepper's disabled state applies.

**Shared pieces this phase reuses:**
- Phases 1–3 (main): `TitleBlock`, `PageActions`, `ShellDialog`, `Segmented`, `HeroFigure`, `SectionHeader`, `Panel`, `PanelBlock`, `ListRow`, `MaskedMoney`, `useMoney`, `Skeleton`, `EmptyState`, `ErrorState`, the straight selection bar.
- Phase 4: `HeroFigure` without a mark ("Later user: Laporan").
- Phase 5: the restyled `PeriodStepper`, `RowAmount` (expense "−Rp" in `--out-fg`, income "+Rp" in green), `TransactionDialog`, `.tbl`, `.dl`, `.box`.
- Phase 6: `Meter`, `ChangeFigure`, `ChartTable`, `format.compactMoney`, `BudgetDialog`, `progress.baseline_share`, `.cols`, `.caption`, `.chart-foot`.

## Goals / Non-Goals

**Goals:**
- Laporan matches the prototype in structure, measurement and states at 1440, 1024 and 390 px in both themes, with every deviation recorded.
- Laporan reads as one narrative: hero, four numbered questions, one context panel.
- Every figure keeps its server source and value; the page sends today's requests.
- The known defects are fixed: 320 px overflow, the hidden-balance leak, the missing first-note path.
- Laporan ships in English and Indonesian, and the old report components are gone.

**Non-Goals:**
- No new report figure, period rule, fact rule or API field.
- No change to the readiness rule, the nav badge or the bell item.
- No redesign of the classification logic or the budget rules.

## Decisions

### D1. Page structure and breakpoints

- **Frame:** the shell's 1200 px frame. `.cols` (`minmax(0,1fr) 352px`, gap 24, `align-items: start`) from 1200 px: `article` (`max-width: 880px`, 728 px at 1440) then `aside.panel`. Below 1200 px one column: the article, then the panel at the article's width.
- **Breakpoints:** viewport media queries at 1200 and 768 px, replacing `ReportLayout`'s `useWidthAtMost(…, 900)`.
- **Phone (< 768 px):** title (24/24), the segment on one line and the stepper under it, the hero, the questions stacked, the panel last.
- **320 px:** the hero figure drops to 32/40 below 360 px (phase 3 rule); the hero line wraps between its two parts, never inside an amount; the table sits in `.tbl-scroll` with the first column sticky and "Porsi" and the change column hidden below 768 px; the month bars keep a 6 px gap; the stepper takes the full width under the segment.

Rejected: the prototype's 880 px column at 1440 (the panel needs the space; the cap applies at 1008–1199 px); keeping `ReportLayout` (two independent columns, the audit's main finding).

### D2. Title block, basis segment and stepper

- Title "Laporan", subtitle "Laporan bulanan Kakeibo", from `report.page`.
- Actions:
  1. `Segmented` labelled "Dasar laporan": "Bulan kalender" (`terms.calendarMonth`) · "Siklus gaji" (`terms.payCycle`), `aria-pressed`, the cycle item only when `cycle_available`. Switching resets `start` (as today).
  2. The shared `PeriodStepper` with `period: {start: start_date, end: end_date}` of the shown report, so it names "Oktober 2026" / "October 2026" or "Siklus 25 Sep – 22 Okt" itself; offset is the report's position in the newest-first list (as today); "Bulan ini" / "Siklus ini" resets to the running period (deviation 1).
- Rejected: dropping the reset (the prototype; `monthly-navigation-stepper` requires it); a "…" menu (nothing to put in it).

### D3. Hero and its line (owner rule: both comparisons)

- `HeroFigure` without a mark, `aria-labelledby` its label. Label "Pengeluaran {period}" ("Pengeluaran Oktober 2026", "Pengeluaran siklus 25 Sep – 22 Okt"), `{period}` formatted from the dates. Figure `comparison.totals.spending`.
- **Line, completed period:** "`ChangeFigure`(previous.change.spending) vs {previous short name}" and "`ChangeFigure`(baseline.change.spending) vs rata-rata {n} bulan", joined by " · ". With a partial previous period the first part reads "{previous} tercatat mulai {date} · belum lengkap"; without a baseline the second reads "Rata-rata muncul setelah 2 bulan lengkap"; without a previous period the line is absent. This applies the owner's rule (whole-month comparisons against the previous month and the 3-month average); the prototype names only the previous month (deviation 2).
- **Line, running period:** "{progress.share}% dari total {previous label}" and "{progress.baseline_share}% dari rata-rata {n} bulan", each part only when its value exists, then a `Meter` (240 px, `--m-expense`, at `min(100, share)`) and the caption "Periode berjalan · dibandingkan setelah selesai" (deviation 3). No ▲, ▼, "naik" or "turun".
- **Tracking started inside the period:** one muted line "Pencatatan dimulai {date}, jadi periode ini belum lengkap." under the line (today's info notice).
- Rejected: the KPI strip (the audit's four tiles); a "Rincian" (the report has no server explanation lines; every figure links to its rows instead).

### D4. Numbered questions

- Each question is `section.q[aria-labelledby]`: `span.q-num[aria-hidden]` (Inter 20/32 600 `--muted`, tabular) in a 32 px column, gap 16, then `h2.q-h` (Manrope 24/32 600 −0.015em `--ink`) and the body 16 px below; body children 24 apart.
- `.q + .q`: margin-top 48, padding-top 48, 1 px `--line` top border. Phone: one column with gap 8 (number above the heading), 32/32.
- The hero is 48 px above question 1.
- Section headers inside questions are `SectionHeader` (`.sec-h`, `t-label`).

### D5. Question 1 "Berapa uang masuk?"

- Figure `t-fig` (Inter 20/28 600): `comparison.totals.inflow`.
- "Sumber uang masuk": `income.sources`, sorted by amount, as `ListRow`s (whole-row link to the ledger filtered to the window, `type=income` and the category; deviation 4): title the category name, meta "{count} transaksi", amount `RowAmount` income "+Rp …" in green.
- Footnote (`.caption`): "Ganti uang dan pindah saldo antar rekening tidak dihitung sebagai uang masuk."
- Empty: "Belum ada uang masuk di periode ini."

### D6. Question 2 "Berapa yang ditabung?"

- Figure `comparison.totals.fresh_saving` with the meta "· {n}% dari uang masuk" when Masuk > 0 (today's browser share, unchanged).
- "Rincian tabungan": two `ListRow`s, as today (deviation 5):
  - "Pengeluaran berpilar Tabungan" `savings.saving_expenses`, linking to the window's `kakeibo=saving&type=expense` rows;
  - "Pindah bersih ke rekening tabungan atau investasi" `savings.net_saving_transfers`, meta "rekening tabungan atau investasi", in `--invest` without a sign, no link (it is not a sum of rows; `monthly-report` "Drill-down from every report figure").
- The rows do not add up to the figure when money was paid from savings, because `paid_from_savings` is not in the response. This is today's behaviour; a third row needs a backend field and is listed as a follow-up in `verification.md`, not built here.

Rejected: one row per destination (the prototype): the API has no per-destination split.

### D7. Question 3 table "Pengeluaran per pilar dan kategori"

- `table.tbl` in `.tbl-scroll`, caption (visually hidden) "Pengeluaran {period} per pilar dan kategori"; head "Pilar dan kategori" · "Jumlah" · "Porsi" · "vs {previous short name}" (or "dari {previous}" while running, "—" without a previous period).
- Pillar rows: `th[scope=row]` `t-label` with an 8 px pillar dot (dashed `--k-none` ring for Belum diklasifikasi), the amount, the share, then `ChangeFigure`(`pillar.change`), "belum lengkap" against a partial previous period, or the running share "{n}%" from `progress.pillars` (deviation 6).
- Category rows: name indented 36 px (24 on the phone), amount, share, `ChangeFigure`(`category.change`) or blank while running. Sorted by amount, as the server sends them.
- `tfoot`: "Total pengeluaran", `totals.spending`, "100%", empty; 1 px `--line-strong` top.
- Rows: 48 px, padding 0 12 (0 8 on the phone), 1 px `--line` bottom. Each row is one target: the first cell holds the link (to `refHref(ref)`), stretched over the row with `::after`, so the whole row opens the ledger and the table keeps its semantics (deviation 4).
- Phone: "Porsi" and the change column are hidden.
- `ChangeFigure` replaces `Change`, which fixes the hidden-balance leak (D20).

### D8. "Tempat belanja terbesar"

- `ol` of `ListRow`s: rank (`r-time`, 40 px, 12/400 `--muted`, hidden on the phone), merchant, `RowAmount` "−Rp …" in the expense red; whole-row link to the window's `type=expense&q=` rows (deviation 4). Top five from `spending.top_merchants`.
- The heading stays "Tempat belanja terbesar" (INVENTORY: "Label may stay"); it names places, not a figure, so the spending-term rule does not apply.

### D9. "Rincian pengeluaran" disclosure

- `details.box` (1 px `--line`, radius 12, no padding): `summary.row` (56 px, no border, no marker) with the title "Rincian pengeluaran" (`t-strong`), the meta "Tak terduga, tagihan berulang dan biaya hidup pokok" and a 16 px chevron that turns 180° when open (instant under reduced motion). Body padding 0 24 24.
- "Tak terduga": up to ten `ListRow`s from `spending.unexpected.items` (title the merchant, notes or category, else "Tanpa keterangan"; meta the date; `RowAmount` red), each opening `TransactionDialog`; "Lihat semua {n} transaksi tak terduga" when more exist (ledger link). Empty: "Tidak ada."
- "Langganan dan tagihan berulang": `spending.recurring.charges` rows (merchant; meta "bulanan" / "mingguan" plus " · sudah ada rencana" or " · nominal berubah"; `last_amount` red), whole-row link to the ledger with `q`. Empty: "Tidak ada."
- "Biaya hidup pokok · saat ini": `t-amt` `essential.amount` with `t-meta` "per bulan · {method sentence}" from `essential.method` in the interface language; "Belum bisa dihitung. Butuh satu bulan lengkap atau anggaran Kebutuhan." when unavailable. The emergency-fund months and the "Cara hitungnya di Analisis" link leave (deviation 7): Target & Tagihan shows the months, and the method sentence is the explanation `monthly-report` asks for.
- "Sudah dikurangi ganti uang {amount}." stays as a caption above the box when `flow.reimbursement_offset` > 0.

### D10. "Anggaran"

- `SectionHeader` "Anggaran" with the `.btn-link` "Atur anggaran", which opens phase 6's `BudgetDialog` (deviation 9); `?atur=anggaran` opens it on load (as today).
- With a budget: one `ListRow` per budgeted category (whole-row link to its ledger rows): title; `Meter` 160 × 4 (pillar colour, `--m-expense` over 100 %), hidden on the phone; amount "{spent} dari {budget}" with meta "sisa {remaining}" or "lebih {over}" in `--expense` (deviation 8). The overall line "Total: {spent} dari {budget}, sisa/lebih {x}." under the rows when an overall budget exists; on the cycle basis the caption "Anggaran bulanan dibandingkan apa adanya dengan satu siklus gaji."
- Without a budget: "Belum ada anggaran. Anggaran tidak wajib; laporan tetap lengkap tanpanya." and no figures (`monthly-report` "Optional budget in the report").

### D11. "Pengeluaran per bulan" — flagged: scope

- `ChartTable` with the chart and its table. Chart `.bars-v`: a grid of `repeat(k, minmax(0,1fr))` (k = the periods in `comparison.history`, at most five), gap 24 (6 on the phone), height 160 = 16 px amount label + 8 + a 112 px bar area + 8 + 16 px period label; the bar height is `round(spending / max × 112)` px, ≥ 2 px when > 0 (fixing the prototype's clamp; deviation 10).
- Colours: the viewed period `--m-expense`; others `--k-none`; the running period hatched (135°, 4 px clear, 2 px `--s0`), labelled "{month} · berjalan". Amount labels `compactMoney` (`t-meta`, centred); masked "••".
- Legend (`.chart-foot`): "{period} (dilihat)" with the `--m-expense` swatch and "Berjalan" with the hatched swatch, so both encodings are named (owner's chart rule; the prototype has none).
- Caption: "Bulan berjalan tidak dibandingkan sampai bulannya selesai." (or the cycle form).
- Table: "Periode" · "Pengeluaran", with " · berjalan" and " · dilihat".
- Accessibility: `role="img"` with an `aria-label` that names each period and amount, masked when balances are hidden (fixing the prototype's leak).
- **The flag:** today each bar is split into five pillar segments without a legend. The prototype draws one series. The per-month pillar split leaves the page; each month's pillar amounts stay in its own table (step to that month). Fallback: stacked bars with a five-item pillar legend and a table with one column per pillar.
- Contrast: `--k-none` and `--m-expense` bars measure ≥ 3:1 on `--s0` in both themes (phase 6 token pairs).

Rejected: a chart library; the prototype's fixed four bars from the data file (the server's `history` decides).

### D12. Question 4 facts "Yang bisa diperbaiki"

- `SectionHeader` "Yang bisa diperbaiki", or "Sejauh ini" while running (`monthly-report` "Improvement Facts").
- One `ListRow` per `comparison.facts` item (whole-row link, deviation 4): title the fact's text from its code (`localize-server-text`), the figure (`+{difference}` for a category above its average, "{amount}/bln" for recurring, else the amount), and the trailing `t-meta` `--accent-text` "Lihat transaksi" (or "Lihat daftar" for recurring, which opens `/insights?tab=ringkasan`).
- Empty: "Tidak ada catatan perbaikan dari angka periode ini."
- Deviation 11: the facts are the server's, not the prototype's three sentences.

### D13. The note — flagged: scope (first-note path)

- Under the facts, 24 px apart:
  - **Saved note:** a read-only block "Catatan untuk {period}" with the text and the quiet "Ubah".
  - **No note:** the quiet button "Tulis catatan untuk {period}".
  - Either opens the form in place: `.field` with the label "Catatan untuk {period}", `textarea` (min-h 96, max 1000, placeholder "Misalnya: masak di rumah tiga kali seminggu"), the help "Satu atau dua hal saja. Catatan ini muncul lagi di laporan berikutnya. {n}/1000", secondary "Simpan catatan" (disabled when empty or unchanged), "Hapus catatan" when a note exists, "Batal", and a polite status ("Catatan tersimpan." / "Catatan dihapus." / the error). Saving closes the form and shows the saved block; focus moves to "Ubah".
  - **Previous note:** "Catatan {previous period}: {text}" (`t-meta`) when `previous_reflection` exists; nothing otherwise (deviation 12).
- Requests are today's: `PUT /reports/monthly/reflection` and `DELETE`.
- **The flag:** today the block is hidden when neither period has a note (`monthly-report` "No reflection note"), so the first note cannot be written. The default shows a button, never an empty form on load, which answers the owner's "half-built" remark. Fallback: the prototype's always-visible form.

### D14. Context panel

`aside.panel` labelled "Konteks laporan" (`--s1`, radius 12, padding 24; 16 on the phone), blocks 24 + 24 apart with a 1 px rule:
- **Kualitas data:** one sentence. When nothing is open: "Data lengkap. Semua transaksi periode ini sudah ditinjau dan diklasifikasi." (neutral, not green). Otherwise the counts in one message ("{review} menunggu di Tinjau · {unclassified} belum diklasifikasi · {duplicates} kemungkinan duplikat · {days} hari tanpa catatan", each part only when > 0), then the fixes as `.btn-link`s: "Buka Tinjau" (`/review`), "Lengkapi klasifikasi" (dialog), "Periksa duplikat" (`dup=1` ledger), "Catat transaksi" (opens Catat; the empty dates in its accessible description).
- **Selisih** (completed periods; deviation 13): `t-fig` `totals.difference` with its sign in the ink, meta "masuk dikurangi pengeluaran".
- **Laporan sebelumnya:** `ul` of `ListRow`s (whole-row links to `/laporan?basis=&start=`), newest first: title the period, trailing `t-meta` "Berjalan" / "Lengkap" plus " · ada catatan". The shown report: `--s2` fill, the 2 px full-height `--accent` bar at x 0 (a square row), `aria-current="page"`. Shown when at least two periods exist.

### D15. Classification dialog and deep links

`ClassificationDialog` moves onto `ShellDialog` 560 (a sheet below 768) with SPEC parts (deviation 14): "Kategori tanpa pilar" rows with a pillar select and "Simpan"; "Transaksi belum diklasifikasi ({n})" with checkboxes, "Pilih semua" and "Terapkan ke {n} transaksi"; footer "Selesai". The requests (`GET /classification/gaps`, `POST /classification/categories/{id}`, `POST /classification/transactions`) and statuses are unchanged. `?lengkapi=1` (Analisis's "Perlu dipilih") and `?atur=anggaran` keep opening their dialogs.

### D16. Loading, failure and empty states

- Loading: a 44 px hero skeleton (240 px), the line, and four question blocks (number, heading bar, three 48 px rows), `role="status"`, `aria-busy`, static.
- Failure: the SPEC error box with the server message or "Laporan belum bisa dimuat." and "Coba lagi"; the title block stays.
- Empty ledger: `EmptyState` "Belum ada laporan" with "Laporan pertama muncul setelah ada transaksi tercatat. Catat pengeluaran atau sambungkan ponsel untuk mulai." and no figures (`monthly-report` "New user").

### D17. Keys and focus

No page-specific keys. Tab order: segment, stepper, hero, questions (rows, the disclosure summary with Enter/Space, the chart toggle, the note controls), panel. Every dialog returns focus to its opener; `scroll-margin-top` 72 px keeps focus clear of the sticky top bar.

### D18. URL

Unchanged: `basis`, `start`, `lengkapi`, `atur`; written with `replaceSearch`. Past-report rows and the stepper keep producing `/laporan?basis=month&start=2026-09-01`.

### D19. Language

- **Namespace `report`:** `page`, `hero`, `questions`, `income`, `saving`, `table`, `merchants`, `details`, `budget`, `history`, `facts`, `note`, `context`, `quality`, `classify`, `states` (Appendix A).
- **Terms:** `terms.spending`, `income`, `savedInvested`, the pillars, `noPillarYet`, `calendarMonth`, `payCycle`, `threeMonthAverage` (phase 6). This phase adds `terms.difference` ("Selisih" / "Difference").
- **Server text:** period labels and facts from the codes `localize-server-text` adds; error messages from `errors.*`. The page formats periods from dates where it can (hero, stepper, table head).
- **Formats:** every date, month, amount and percent through `useFormat()`; `rg 'id-ID|Intl\.' app/laporan components/report` finds nothing.
- **Registry and guards:**
  - `/laporan` joins `lib/i18n/translatedRoutes.ts`, and its entry leaves `untranslatedPages.ts`;
  - the untranslated-page test example, which phase 6 moved to Laporan, moves to Rekening (`/accounts`), untranslated until phase 8;
  - the copy register's translated roots gain `app/laporan` and `components/report`, which leave the untranslated roots;
  - `SPENDING_ID` covers `report.*`, with `report.merchants.title` ("Tempat belanja terbesar") allowed as a place name, not a figure label.

### D20. Hidden balances

Every amount reads "Rp ••••••" (`MaskedMoney`): hero, `ChangeFigure`s, the three question figures, every row, the table and its total, the budget rows, the month bars' labels ("••") and their `aria-label`, the table alternative, Selisih, the essential cost. Percentages and counts stay. This fixes the change-chip and chart-label leaks.

### D21. Shared components and CSS

Reused: `HeroFigure`, `ChangeFigure`, `Meter`, `ChartTable`, `compactMoney`, `BudgetDialog`, `SectionHeader`, `Panel`, `PanelBlock`, `ListRow`, `RowAmount`, `TransactionDialog`, `ShellDialog`, `Segmented`, `PeriodStepper`, `MaskedMoney`, `Skeleton`, `EmptyState`, `ErrorState`, `.cols`, `.tbl`, `.tbl-scroll`, `.box`, `.caption`, `.chart-foot`.

New CSS from `styles.css`: `.q`, `.q-num`, `.q-h`, `.bars-v`, `.b`, the `details.box` summary rules and the hatch. Laporan-only components stay in `components/report/`; nothing else needs them.

Rejected: a Laporan copy of the change chip or the meter (phase 6 built them for this reuse).

### D22. What happens to each removed element

| Element | Destination |
| --- | --- |
| KPI strip (`KpiStrip`, `Kpi`, `.ws-kpis`, `.ws-kpi*`) | hero (Pengeluaran), Q1 (Masuk), Q2 (Ditabung & investasi), panel (Selisih); the components are removed, Laporan being their last user |
| `ReportLayout` and its two columns | the article and the panel (D1) |
| Info notice (`periodNote`) | the hero line and its caption (D3) |
| Green "Data lengkap." notice | the panel's Kualitas data line (D14) |
| `PillarTable`, `Change`, `Progress` (`ComparisonParts.tsx`) | the Q3 table, `ChangeFigure`, `Meter` (D3, D7) |
| `ImprovementFacts` | Q4 facts (D12) |
| `MonthlyHistory` (stacked bars) | "Pengeluaran per bulan" (D11, flagged) |
| `TopMerchantsCard`, `SpendingDetailsCard`, `IncomeSourcesCard`, `SavingsCard`, `ReportPanel`, `AmountRow`, `ReportParts.tsx`, `SpendingAnswer.tsx`, `IncomeSavingsAnswers.tsx` | the question sections (D5–D9) |
| `ReflectionAnswer.tsx` | Q4 note (D13) |
| `PastReports` card | the panel (D14) |
| `report.module.css` | `globals.css` rules (D21) |
| Card header icon tiles | removed |
| "Cara hitungnya di Analisis" link, emergency-fund months in the essential line | removed (D9) |

### D23. Verification tooling and gate

The plan's common gate, with Laporan's scenarios:
- **Scenarios** (prototype and app, same seed): `laporan` (Oktober), `laporan-juli` (no previous period), `laporan-november` (running), `laporan-siklus`, `laporan-sebagian` (partial previous month), `laporan-anggaran` (budget over limit), `laporan-tanpa-anggaran`, `laporan-catatan` (saved note and previous note), `laporan-catatan-baru` (form open), `laporan-rincian` (disclosure open), `laporan-tabel` (chart table), `dlg-klasifikasi`, `dlg-anggaran`, `laporan-sembunyi`, loading, failure and the empty ledger.
- **Matrix:** 1440 × 900, 1024 × 768 and 390 × 844, light and dark, `en` and `id`, composed left/right images; plus 1280, 1200, 1199, 1008, 768, 767 and 320 for overflow.
- **Measurement checklist** (`laporan-checklist.js`): first content at `y = 192`; article 728 and panel 352 at 1440; article ≤ 880 at 1100; hero 44/52; `.q` 32 | 1fr with gap 16, 48 + 48 with a 1 px rule; `.q-num` 20/32; `.q-h` 24/32; `t-fig` 20/28; table rows 48, header padding 8 12, total rule `--line-strong`; rows 56 (64 on the phone); `Meter` 160 × 4; bars area 112 and heights proportional within 1 px; the panel's padding, radius and rules; the selection bar 2 px full height.
- **SPEC §13 rules:** fonts, sizes, weights, ≤ 8 text styles, one 44 px figure, no dotted underline, no border ≥ 2 px except the allowed marks, no horizontal overflow at 1440, 1024, 390 and 320.
- **Figure parity:** every amount, count, percent and date before (task 1.3) against after, and `en` against `id`.
- **Keyboard flows** (`laporan-keys.mjs`): segment and stepper; past report; a table row; a merchant row; the disclosure and an unexpected item's dialog with focus back; "Atur anggaran"; "Lengkapi klasifikasi"; write, save, edit and delete a note; the chart table toggle; "Buka Tinjau".

### D24. Spec reconciliation

| Capability | Requirement | Change |
| --- | --- | --- |
| `monthly-report` | Report page layout | MODIFIED: article, hero, questions, panel; English and Indonesian |
| | Four Kakeibo questions in a fixed order | MODIFIED: numbered questions, the note entry |
| | Running Period Progress | MODIFIED: both shares in the hero line |
| | Monthly Spending History | MODIFIED: one series, legend, table |
| | Laporan in Both Languages | ADDED |
| `monthly-navigation-stepper` | Month-and-Year Stepper Navigation | MODIFIED: example page Rekening (written over phase 6's version) |
| `tactile-pulse-ui` | No Decorative Effects | MODIFIED: examples Rekening and Target & Tagihan; Laporan scenario added (written over phase 6's version) |

No delta: the report API requirements, "Spending answer", "Data quality box", "Drill-down from every report figure", "Reflection note per period", "Optional budget in the report", "Spending by Pillar and Category", "Improvement Facts", "Three-Period Average" and "Comparison with the previous period" keep their wording, which the page satisfies.

### D25. Dependencies and order

1. `add-language-system`, `add-category-system-keys` (merged), `localize-server-text` (facts and period labels as codes).
2. Phase 4 (`HeroFigure` without a mark), phase 5 (`PeriodStepper`, `RowAmount`, `TransactionDialog`, `.tbl`, `.box`).
3. Phase 6 `apply-markas-analisis` (`Meter`, `ChangeFigure`, `ChartTable`, `compactMoney`, `BudgetDialog`, `progress.baseline_share` — or its fallback, in which case the running line omits the average share — and the untranslated-test example at `/laporan`).
4. **This change.** It stops at task 1.2 and reports if any of the above is not on `main`.

### D26. Credit figures — flagged: scope

The credit change hands nothing to Laporan (blocks H5, H8, H9, H11). `spent_on_credit` and `paid_to_credit` exist only in the current cycle summary and per credit account; a closed month needs them in the report, which is backend work outside a UI phase. Default: not shown. Credit purchases already count in Pengeluaran and its pillars, and a bill payment is a transfer, as today. Fallback: a backend change adds both to the report and a UI change shows them under question 3.

### D27. Capital gain

The report has no capital-gain field; Q2 shows money moved into investments, not their value. Gains belong to Rekening (phase 8). Nothing to show here.

### D28. Readiness, badge and bell

Unchanged: `GET /reports/monthly/latest`, `freshReport` (14 days), the sidebar count and the bell item "Laporan {period} siap dibaca". The bell's `url` opens the report it names.

## Decisions and their defaults

Approved under the owner's standing instruction "complete all until finished" (2026-11-06, renewed 2026-11-08). Every decision takes its recommended default. **Flag** marks a decision reviewed before implementation because it changes scope beyond the approved phased plan.

| # | Decision | Default (taken) | Rejected alternatives | Flag |
| --- | --- | --- | --- | --- |
| D1 | Structure | article + panel from 1200, media queries | 880 at 1440; `ReportLayout` | — |
| D2 | Title block | segment + shared stepper with dates, reset kept | no reset; "…" menu | — |
| D3 | Hero | Pengeluaran with previous and average changes, or running shares | KPI strip; previous only | — |
| D4 | Questions | prototype `.q` grid | cards | — |
| D5 | Q1 | sources as whole-row links | plain rows | — |
| D6 | Q2 | today's two rows | per destination (no API) | — |
| D7 | Table | `.tbl` with stretched row links, `ChangeFigure` | cell links only | — |
| D8 | Merchants | top five, red amounts, row links | plain rows | — |
| D9 | Details | disclosure box, dialog rows, method sentence | months and Analisis link | — |
| D10 | Budget | meter rows with amounts; shared dialog | percent only; Pengaturan link | — |
| D11 | Month chart | one series, proportional, legend, table | stacked without legend | **scope** |
| D12 | Facts | server facts as row links | prototype sentences | — |
| D13 | Note | read-only or "Tulis catatan", form in place | hidden block; always-open form | **scope** |
| D14 | Panel | quality line, Selisih for completed, past reports | green notice; Selisih while running | — |
| D15 | Classification | ShellDialog 560, same requests | Modal | — |
| D19 | Language | `report` namespace, `/laporan` translated, example to `/accounts` | — | — |
| D20 | Hidden balances | every amount masked, leaks fixed | — | — |
| D26 | Credit figures | not shown | backend field in this phase | **scope** |
| D27 | Capital gain | none on Laporan | — | — |

No default changes money behaviour: every figure keeps its server source and value, every action sends today's request, and the backend is untouched.

## Risks / Trade-offs

- [Phase 6 lands with different shared names or without `baseline_share`] → task 1.2 records differences; the running line falls back to the previous-period share only.
- [Facts arrive without codes if `localize-server-text` changed shape] → task 1.2 checks the fact payload; without codes, an English page would show Indonesian facts, which stops the phase.
- [Stretched row links in a table confuse some screen readers] → the link text names the row ("{pillar}: {amount}, lihat transaksi"); axe and a VoiceOver spot check in 8.3.
- [Removing the per-month pillar split] → flagged with a fallback; each month's table keeps the split.
- [The note form in place moves focus] → focus goes to the textarea on open and to "Ubah" after saving; tested.

## Migration and rollback

No data or API change. Rollback is a revert of the merge commit; the web returns to the old page.

## Open Questions

None that change the specs or tasks. The flagged decisions have fallbacks written into the tasks.

## Appendix A. Laporan copy as message keys (`report` namespace; Indonesian · English)

Arguments arrive formatted from `lib/format.ts`; `{count}` selects plural forms (English `one`/`other`, Indonesian `other`). Existing Indonesian is kept byte for byte where the page keeps the text; "(new)" marks new strings and "(changed)" a changed one. Task 3.1 writes the remaining keys (the classification dialog, `aria-label`s, errors) into `research/evidence/apply-markas-laporan/copy-keys.md` before the page work.

### `page`, `hero`, `questions`

| Key | Indonesian | English |
| --- | --- | --- |
| `page.title` | Laporan | Report |
| `page.subtitle` | Laporan bulanan Kakeibo | Monthly Kakeibo report |
| `page.basis` (changed) | Dasar laporan | Report basis |
| `page.documentTitle` | Laporan {period} | Report for {period} |
| `hero.label` (new) | Pengeluaran {period} | Spending for {period} |
| `hero.vsPrevious` (new) | {change} vs {period} | {change} vs {period} |
| `hero.vsAverage` (new) | {change} vs rata-rata {count} bulan | {change} vs the {count}-month average |
| `hero.vsAverageCycle` (new) | {change} vs rata-rata {count} siklus | {change} vs the {count}-cycle average |
| `hero.partialPrevious` (new) | {period} tercatat mulai {date} · belum lengkap | {period} recorded from {date} · incomplete |
| `hero.noAverage` | Rata-rata muncul setelah 2 bulan lengkap | The average appears after 2 complete months |
| `hero.shareOfPrevious` | {share}% dari total {period} | {share}% of the total for {period} |
| `hero.shareOfAverage` (new) | {share}% dari rata-rata {count} bulan | {share}% of the {count}-month average |
| `hero.running` (new) | Periode berjalan · dibandingkan setelah selesai | Period in progress · compared once it ends |
| `hero.trackingStarted` (changed) | Pencatatan dimulai {date}, jadi periode ini belum lengkap. | Recording started on {date}, so this period is incomplete. |
| `questions.income` | Berapa uang masuk? | How much came in? |
| `questions.saving` | Berapa yang ditabung? | How much was saved? |
| `questions.spending` (changed) | Berapa pengeluarannya? | How much was spent? |
| `questions.improve` | Apa yang mau diperbaiki bulan depan? | What to improve next month? |

"How much was spent?" is a question, not a figure label; the English figure stays "Spending". The copy guard's English spending rule covers `terms.*` only, so the question passes without an exception.

### `income`, `saving`, `table`, `merchants`, `details`

| Key | Indonesian | English |
| --- | --- | --- |
| `income.sources` | Sumber uang masuk | Income sources |
| `income.count` | {count} transaksi | {count, plural, one {# transaction} other {# transactions}} |
| `income.note` | Ganti uang dan pindah saldo antar rekening tidak dihitung sebagai uang masuk. | Reimbursements and transfers between accounts do not count as income. |
| `income.empty` (changed) | Belum ada uang masuk di periode ini. | No income in this period yet. |
| `saving.shareOfIncome` | {share}% dari uang masuk | {share}% of income |
| `saving.breakdown` | Rincian tabungan | Savings breakdown |
| `saving.pillarExpenses` | Pengeluaran berpilar Tabungan | Spending in the Savings pillar |
| `saving.netMoves` | Pindah bersih ke rekening tabungan atau investasi | Net moved into savings or investment accounts |
| `saving.netMovesMeta` (new) | rekening tabungan atau investasi | savings or investment accounts |
| `table.title` | Pengeluaran per pilar dan kategori | Spending by pillar and category |
| `table.caption` | Pengeluaran {period} per pilar dan kategori | Spending for {period} by pillar and category |
| `table.pillarCategory` | Pilar dan kategori | Pillar and category |
| `table.amount` | Jumlah | Amount |
| `table.share` | Porsi | Share |
| `table.vs` | vs {period} | vs {period} |
| `table.of` | dari {period} | of {period} |
| `table.total` | Total pengeluaran | Total spending |
| `table.rowLink` (new) | {name}: {amount}, lihat transaksi | {name}: {amount}, view transactions |
| `merchants.title` | Tempat belanja terbesar | Top places |
| `details.title` | Rincian pengeluaran | Spending details |
| `details.meta` | Tak terduga, tagihan berulang dan biaya hidup pokok | Unexpected, repeating bills and essential living costs |
| `details.reimbursed` | Sudah dikurangi ganti uang {amount}. | Reimbursements of {amount} already subtracted. |
| `details.unexpected` | Tak terduga | Unexpected |
| `details.noDescription` | Tanpa keterangan | No description |
| `details.allUnexpected` | Lihat semua {count} transaksi tak terduga | View all {count, plural, one {# unexpected transaction} other {# unexpected transactions}} |
| `details.recurring` | Langganan dan tagihan berulang | Subscriptions and repeating bills |
| `details.monthly` | bulanan | monthly |
| `details.weekly` | mingguan | weekly |
| `details.hasPlan` | sudah ada rencana | already planned |
| `details.amountChanged` | nominal berubah | amount changed |
| `details.essential` | Biaya hidup pokok · saat ini | Essential living costs · now |
| `details.perMonth` (new) | per bulan · {method} | per month · {method} |
| `details.essentialUnavailable` | Belum bisa dihitung. Butuh satu bulan lengkap atau anggaran Kebutuhan. | Cannot be calculated yet. Needs one complete month or a Needs budget. |
| `details.none` (new) | Tidak ada. | None. |

### `budget`, `history`, `facts`, `note`

| Key | Indonesian | English |
| --- | --- | --- |
| `budget.title` | Anggaran | Budget |
| `budget.set` | Atur anggaran | Set budgets |
| `budget.spentOf` (new) | {spent} dari {budget} | {spent} of {budget} |
| `budget.left` (changed) | sisa {amount} | {amount} left |
| `budget.over` (changed) | lebih {amount} | {amount} over |
| `budget.total` (changed) | Total: {spent} dari {budget}, {rest}. | Total: {spent} of {budget}, {rest}. |
| `budget.cycleNote` | Anggaran bulanan dibandingkan apa adanya dengan satu siklus gaji. | Monthly budgets are compared as they are with one pay cycle. |
| `budget.empty` | Belum ada anggaran. Anggaran tidak wajib; laporan tetap lengkap tanpanya. | No budget yet. A budget is optional; the report is complete without one. |
| `history.title` | Pengeluaran per bulan | Spending per month |
| `history.titleCycle` | Pengeluaran per siklus | Spending per cycle |
| `history.running` | berjalan | in progress |
| `history.viewed` (new) | dilihat | shown |
| `history.caption` | Bulan berjalan tidak dibandingkan sampai bulannya selesai. | The current month is not compared until it ends. |
| `history.captionCycle` | Siklus berjalan tidak dibandingkan sampai siklusnya selesai. | The current cycle is not compared until it ends. |
| `history.period` (new) | Periode | Period |
| `history.chartLabel` (new) | Pengeluaran per bulan: {list} | Spending per month: {list} |
| `facts.title` | Yang bisa diperbaiki | What could improve |
| `facts.soFar` | Sejauh ini | So far |
| `facts.view` | Lihat transaksi | View transactions |
| `facts.viewList` | Lihat daftar | View the list |
| `facts.perMonth` | {amount}/bln | {amount}/mo |
| `facts.empty` (changed) | Tidak ada catatan perbaikan dari angka periode ini. | Nothing to improve from this period's figures. |
| `note.label` (new) | Catatan untuk {period} | Note for {period} |
| `note.write` | Tulis catatan untuk {period} | Write a note for {period} |
| `note.edit` | Ubah | Edit |
| `note.placeholder` | Misalnya: masak di rumah tiga kali seminggu | For example: cook at home three times a week |
| `note.help` | Satu atau dua hal saja. Catatan ini muncul lagi di laporan berikutnya. {count}/1000 | One or two things. This note shows again in the next report. {count}/1000 |
| `note.save` | Simpan catatan | Save note |
| `note.delete` | Hapus catatan | Delete note |
| `note.saved` | Catatan tersimpan. | Note saved. |
| `note.deleted` | Catatan dihapus. | Note deleted. |
| `note.saveError` | Catatan belum tersimpan. Coba lagi. | The note was not saved. Try again. |
| `note.deleteError` | Catatan belum terhapus. Coba lagi. | The note was not deleted. Try again. |
| `note.previous` (changed) | Catatan {period}: {note} | Note from {period}: {note} |

### `context`, `quality`, `states`

| Key | Indonesian | English |
| --- | --- | --- |
| `context.label` (new) | Konteks laporan | Report context |
| `quality.title` | Kualitas data | Data quality |
| `quality.complete` | Data lengkap. Semua transaksi periode ini sudah ditinjau dan diklasifikasi. | Data complete. Every transaction in this period is reviewed and classified. |
| `quality.summary` (new) | {parts} | {parts} |
| `quality.review` | {count} menunggu di Tinjau | {count} waiting in Review |
| `quality.unclassified` | {count} belum diklasifikasi | {count} without a pillar |
| `quality.duplicates` | {count} kemungkinan duplikat | {count, plural, one {# possible duplicate} other {# possible duplicates}} |
| `quality.emptyDays` | {count} hari tanpa catatan | {count, plural, one {# day} other {# days}} without records |
| `quality.openReview` | Buka Tinjau | Open Review |
| `quality.classify` | Lengkapi klasifikasi | Complete classification |
| `quality.checkDuplicates` | Periksa duplikat | Check duplicates |
| `quality.record` | Catat transaksi | Record a transaction |
| `context.difference` | Selisih | Difference |
| `context.differenceMeta` | masuk dikurangi pengeluaran | income minus spending |
| `context.past` | Laporan sebelumnya | Earlier reports |
| `context.running` | Berjalan | In progress |
| `context.complete` | Lengkap | Complete |
| `context.hasNote` | ada catatan | has a note |
| `states.loading` | Menyiapkan laporan… | Preparing the report… |
| `states.error` | Laporan belum bisa dimuat. | The report could not be loaded. |
| `states.emptyTitle` | Belum ada laporan | No report yet |
| `states.emptyText` | Laporan pertama muncul setelah ada transaksi tercatat. Catat pengeluaran atau sambungkan ponsel untuk mulai. | The first report appears once transactions are recorded. Record spending or connect a phone to start. |

## Appendix B. Laporan glossary (Indonesian → English)

Consistent with `research/2026-11-06-language-system.md` §7, the `terms` namespace and phase 6's glossary.

| Indonesian | English | Note |
| --- | --- | --- |
| Laporan | Report | page title and nav (`nav.reports`) |
| Laporan bulanan Kakeibo | Monthly Kakeibo report | subtitle |
| Laporan sebelumnya | Earlier reports | panel block; not "Previous reports" to avoid "previous period" confusion |
| Pengeluaran | Spending | the only figure name (`terms.spending`) |
| Masuk | Income | `terms.income` |
| Ditabung & investasi | Saved & invested | `terms.savedInvested` |
| Selisih | Difference | `terms.difference` (new); income minus spending, never "surplus" |
| rata-rata 3 bulan | 3-month average | `terms.threeMonthAverage` (phase 6) |
| belum lengkap | incomplete | a partial previous period |
| berjalan | in progress | a running period |
| Lengkap | Complete | a closed period in the list |
| Kualitas data | Data quality | |
| Yang bisa diperbaiki | What could improve | |
| Sejauh ini | So far | running facts |
| Tempat belanja terbesar | Top places | merchants; a place name, not a figure |
| Rincian pengeluaran | Spending details | |
| Rincian tabungan | Savings breakdown | |
| Biaya hidup pokok | Essential living costs | |
| Tak terduga | Unexpected | pillar name (`terms.unexpected`) |
| Anggaran | Budget | |
| Atur anggaran | Set budgets | |
| Lengkapi klasifikasi | Complete classification | |
| Catatan | Note | the reflection note |
| Bulan kalender · Siklus gaji | Calendar month · Pay cycle | `terms.calendarMonth`, `terms.payCycle` |
