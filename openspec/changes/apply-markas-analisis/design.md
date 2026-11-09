## Context

See `proposal.md` for the motivation. This section records the state the design starts from.

**Base.** `origin/main` `0308b7b` holds phases 1 to 3, `add-category-system-keys` and `add-language-system`. Before this phase starts, `localize-server-text`, phase 4 (`apply-markas-tinjau`) and phase 5 (`apply-markas-transaksi`) merge (D28). This design names their pieces as they are planned; task 1.2 records every difference in the merged code.

**Analisis today** (`app/insights/page.tsx`, `components/analysis/**`, `components/insights/**`):
- The page shell keeps `?tab=ringkasan|kalender|aliran`, `?day=` and `?view=tabel` (`url.ts`), and shares the period through `useAppCtx()` (`timeframe`, `cycleOffset`). A rolling shared period is shown as the current cycle with a caption.
- The title block comes from `untranslatedPages.ts` ("Analisis", "Ke mana uang digunakan, dibanding periode lalu"). `PageActions` holds only the `PeriodStepper`, which gets `unit` and the server's `timeframe_label` but no `period`; in English it shows the Indonesian label.
- The toolbar holds a segmented-pill tab list (arrow, Home and End keys), the "Siklus gaji · Bulan kalender" `Segmented` and the coral "Baca Laporan Bulanan" link.
- **Ringkasan** (`RingkasanTab.tsx`):
  - `KpiStrip` "Arus kas tercatat" with three `Kpi`s (Masuk, Pengeluaran "Tanpa tabungan", Ditabung & investasi) from `GET /reports/monthly/comparison` `totals`;
  - Pengeluaran is the page's one dotted `ExplainableFigure`, shown only when `analytics.explanations.spending.total` equals the figure;
  - `ReportLayout` (7fr/5fr, one column at ≤ 900 px measured by `useWidthAtMost`): `ProgressCard` (running period against the whole previous period, per pillar), `BaselineCard` (the last completed period against its average, per pillar, as amounts), "Belanja harian" (CSS div bars, 76 px axis, `aria-hidden`, table toggle, hover and tap tooltip), `RecurringCharges`, `QuestionRate` and `HabitsLearnedCard`;
  - `CategoryBudgetSection` (927 lines): an expense table or card list with inline budget editing, income and archived lists, and a `Modal` form with icons and colours.
- **Kalender** (`KalenderTab.tsx`): `ProgressBand`, a grid of buttons with heat fills (`--heat-1..5`), text markers (Masuk, Pindah, Top up, the first plan's label) and a "?" waiting tag; `DaySheet` with the waiting rows, plans and the transactions (`TransactionDrawer`, renamed `TransactionDialog` by phase 5); `UpcomingCard`; a week strip on the phone, switched by `useWidthAtMost(rootRef, 767)`.
- **Peta Aliran** (`PetaAliranTab.tsx`, `sankey.ts`): a deterministic Sankey with five column heads, stations for accounts (names cut at 15 characters), labels with halos and no gutter, ribbons at `--band-op` (0.42 light, 0.5 dark) with stripe, dot and hatch patterns, the fold control (localStorage `analysis.flow.folded`), highlight chips (including "Belanja"), the replay (800 ms per day, `through`), four summary cards, a `Drawer` for node and link details and a table. On the phone it shows a decorative `MiniMap` above the table.
- Every visible string on the page body is hard-coded Indonesian; `format.ts` holds `id-ID` formatters and `compactMoney` ("rb", "jt").
- Every comparison is computed by the server (`services/monthly_comparison.py` `build_comparison`): previous = the newest earlier whole period; baseline = up to three earlier whole, non-partial periods, given when at least two exist; while running, `progress.share` is Pengeluaran so far as a whole percent of the whole previous period. The baseline is returned while running too, without a verdict.

**Prototype** (`option-6`, `pages.js` 490–805, `dialogs.js` 580–588, `styles.css`): as summarized in the proposal. Where `SPEC.md` and `styles.css`/JS disagree for Analisis, this design takes `styles.css` and the JS geometry, except where a decision below names a fix:
- the daily bar width is `max(4, floor((W − 8)/n) − 4)` with a 4 px inset (JS), not SPEC §9's `floor(W/n) − 4`;
- the today hatch is a 4 × 4 pattern rotated 45° with a 2 px `--s0` stripe at opacity 0.6 (JS);
- category groups on Ringkasan are 16 px apart (inline), not 48;
- the map section is 32 px below the hero (inline), not 48;
- flow column heads are 12/400 `--muted` with 0.06em tracking (JS);
- the light layer shadow is `0 16px 48px rgba(40,34,26,.14), 0 2px 6px rgba(40,34,26,.06)` (§3.2 and CSS).

**Shared pieces this phase reuses:**
- Phase 2 (main): `TitleBlock`, `PageActions`, `ShellDialog` (centered, a bottom sheet below 768 px), `Menu`, `ToastProvider`.
- Phase 3 (main): `HeroFigure`, `FigureBlock`, `SectionHeader`, `Panel`, `PanelBlock`, `ListRow`, `DayHeading`, `ExplanationDialog`, `MaskedMoney`, `useMoney`.
- Phase 1 (main): `Segmented`, `Skeleton`, `EmptyState`, `ErrorState`, the straight selection bar and group rail.
- Phase 4: `HeroFigure`'s optional `mark`, `ListRow`'s `rowKey`, `current` and `selected`, `RailItem` (not used here).
- Phase 5: the restyled `PeriodStepper` (36 × 36 chevrons, 14/500 label ≥ 168 px, reset kept), `HeroFigure`'s `aside` and optional `line`, `FigureBlock bar={false}`, `RowAmount` and `RowStatus` in `components/transactions/rows.tsx` (expense "−Rp" in `--out-fg`), `TransactionDialog`, the `.tbl`, `.dl` and `.box` rules.

## Goals / Non-Goals

**Goals:**
- Analisis matches the prototype in structure, measurement and states at 1440, 1024 and 390 px in both themes, with every deviation recorded.
- Every figure keeps its server source and value. The only new value is `progress.baseline_share`, from the server.
- The five known items are fixed and proven: marker contrast, flow labels, the English stepper label, 320 px overflow and the dotted figure.
- Analisis ships in English and Indonesian.
- Each component this page builds that phase 7 also needs is built once, in a shared place.

**Non-Goals:**
- No change to comparison rules, windows, the calendar's day apportionment, the flow model's conservation or the category and budget requests.
- No new chart library; the charts stay hand-built SVG.
- No Pengaturan or Laporan redesign.

## Decisions

### D1. Page structure and breakpoints

- **Frame:** the shell's 1200 px frame (`.page-inner`). Ringkasan and Kalender use `.cols` (`minmax(0,1fr) 352px`, gap 24, `align-items: start`) from 1200 px; below 1200 px one column, the panel after the reading stack, full width. Peta Aliran has no panel and uses the full frame.
- **Reading stack:** `.stack > * + *` 48 px (32 on the phone). The tabs have `margin-bottom: 32px`.
- **Breakpoints:** viewport media queries only, at 1200 px (`.cols`) and 768 px (phone composition), as SPEC §6 and phase 5. They replace `useWidthAtMost` in `KalenderTab` and Analisis's use of `ReportLayout` (≤ 900 px), and the container queries at 900 and 760 px in `analysis.module.css`.
- **Phone (< 768 px):** title (24/24 padding), the segment on one line and the stepper under it, the tabs (44 px, horizontal scroll, "Baca Laporan Bulanan" hidden), then the tab's content in DOM order, then the panel.
- **Touch targets (deviation 16):** below 768 px every segment item and small button is 40 px high (the prototype's `.seg.is-small` 26 px and inline 28 px buttons stay at 26/28 from 768 px).
- **320 px:** no element may be wider than the column. `.cmp-row` uses the phone grid `minmax(0,1fr) auto 40px`; amounts never wrap inside a figure but the label wraps; tables sit in `.tbl-scroll` (`overflow-x: auto`) with the first column sticky; SVGs are drawn at the column's width (D7, D15).

Rejected:
- Keeping `useWidthAtMost`: it switched layouts on the element's own width, which caused phase 5's "phone feed on a laptop" defect.
- A two-column Ringkasan below 1200 px: the 352 px panel leaves less than 456 px for the comparison grid at 1024 px.

### D2. Title block, basis segment and stepper (known item: stepper label)

- Title "Analisis", subtitle "Ke mana uang digunakan, dibanding periode lalu", both from `analysis.page`.
- Actions, two as `executive-sidebar-layout` allows:
  1. `Segmented` labelled "Dasar periode": "Siklus gaji" (`terms.payCycle`) · "Bulan kalender" (`terms.calendarMonth`, phase 5), `aria-pressed`. "Siklus gaji" is always offered, as today; a cycle that is not available falls back to the labelled calendar month (`report-periods-and-drilldown` "Period Controls").
  2. The shared `PeriodStepper` with `unit`, `offset`, `onChange` and now `period: {start, end}`. The dates come from the selected entry of `GET /reports/monthly/periods?basis=` (already loaded for Ringkasan's comparison); offset 0 is the running period, −n the n-th earlier one. With `period` the stepper writes "Siklus 23 Okt – 24 Nov" or "Oct 2026" itself in the interface language.
- When the step has no listed period (a cycle before the first confirmed salary receipt), the stepper shows the server's `timeframe_label`, which `localize-server-text` renders in the account's language, so English never shows "Oktober 2026".
- A rolling shared period: the page shows the current cycle and one muted line under the title, "Analisis memakai siklus gaji atau bulan kalender. Yang tampil: siklus ini." (`analysis.page.rollingNote`), and does not change the shared value.
- Deviation 1: the label is phase 5's, and the reset ("Siklus ini" / "Bulan ini") stays.

Rejected: a page-local stepper with the prototype's "· berjalan" label (a second stepper style); passing the server label in English through a new endpoint (the dates already exist).

### D3. Tabs and the report link

- `AnalysisTabs` (`components/analysis/`): `div.tabs` (1 px `--line` bottom, gap 24, flex) holding `div[role=tablist][aria-label]` with three `button[role=tab]` (`id="analysis-tab-{value}"`, `aria-controls="analysis-panel"`, `aria-selected`, roving `tabindex`), and the link at the right.
- Tab: h 40 (44 on the phone), 14/500 `--muted`, hover `--ink`; selected `--ink` with `::after` 2 px `--accent`, full tab width, at `bottom: -1px`. It is a straight bar under a square tab, so the rounded-element rule is kept.
- Keys: ←/→ move and select, Home and End, as today. Selecting writes `?tab=` and keeps every other parameter (`analysis-workspace`).
- "Baca Laporan Bulanan" (`.btn-link`, `--accent-text`, underline on hover) opens `/laporan`, the latest closed report, from 768 px; hidden on the phone, where the panel's Laporan block links to it (deviation 3).
- The component lives in `components/analysis/`, because no other page has tabs (phase 5 rejected tabs for Transaksi; Rekening uses segments).

Rejected: hash-route links as in the prototype (breaks `?tab=` links and the tab-list keys); a shared `Tabs` primitive with one user.

### D4. Ringkasan hero and its one "Rincian" (known item: dotted figure)

- `HeroFigure` without a mark (phase 4), `aria-labelledby` its label:
  - label (`t-label`): "Pengeluaran · {period}" with " sejauh ini" while running, for example "PENGELUARAN · SIKLUS 23 OKT – 24 NOV SEJAUH INI"; `{period}` is the stepper's formatted period in lower case where the language allows;
  - figure (`t-hero`, Manrope 44/52): `comparison.totals.spending` of the viewed period (unchanged source);
  - line (`hero-sub`, 14/20 `--muted`): "Masuk {inflow} · Ditabung & investasi {fresh_saving}", the amounts in `t-amt` with `--ink`, so the KPI row's three figures appear once.
- **One "Rincian"** (quiet button beside the label, `aria-haspopup="dialog"`, named "Rincian pengeluaran"): opens `ExplanationDialog` (480 px, centered; a sheet below 768) with `analytics.explanations.spending` (lines, caveat, link "Lihat transaksinya"). It is shown only when the server's `total` equals the hero figure, as today; otherwise no trigger and no Rp 0 (`analysis-workspace` "Older API without explanations").
- Hero loading: a 44 px skeleton bar 240 px wide and one 14 px line.

Rejected: keeping `ExplainableFigure` (dotted underline, forbidden on redesigned pages); a "Rincian" per figure (SPEC §13: one per page).

### D5. Comparison rows — flagged: backend addition (read-only), money presentation

Two `.cmp-row` rows under the hero (`margin-top: 24px`, the first with a 1 px `--line` top border): grid `minmax(0,1fr) 120px 72px 120px`, gap 16, min-h 48, 1 px `--line` bottom, tabular.

| State | Row 1 | Row 2 |
| --- | --- | --- |
| Running, whole previous period | "{progress.share}% dari total {previous label}" · `progress.previous_spending` · "seluruh periode" · `Meter` at `min(100, share)` in `--m-expense` | "{progress.baseline_share}% dari rata-rata {n} bulan ({first}–{last})" · `baseline.totals.spending` · "per bulan" · `Meter` |
| Complete | "Dibanding total {previous label}" · `ChangeFigure` of `previous.change.spending` · "seluruh periode" · no bar | "Dibanding rata-rata {n} bulan ({first}–{last})" · `ChangeFigure` of `baseline.change.spending` · "per bulan" · no bar |
| Partial previous period | "{previous label} tercatat mulai {date}" · "belum lengkap" | as above |
| No baseline | — | "Rata-rata muncul setelah 2 bulan lengkap" (or "siklus") |
| No previous period | one caption "Belum ada periode pembanding sebelum {label}." | — |

- A running period gets the caption "Dibanding total periode lalu; tanpa penilaian sampai periodenya selesai." No ▲, ▼, "naik" or "turun" appears while running.
- `ChangeFigure`: "▲ Rp 753.830" in `--expense` when spending rose, "▼ Rp 120.000" in `--income` when it fell, "sama" when equal; a visually hidden "naik"/"turun"; the amount through `useMoney` (masked when balances are hidden).
- The baseline labels name the first and last month of `baseline.labels`, for example "(Agu–Okt)"; with two periods the text says "rata-rata 2 bulan".
- **The flagged addition:** `progress.baseline_share` = `share_percent(current.spending, baseline.totals.spending)`, `null` without a baseline or when the baseline is 0, computed in `build_comparison` beside `progress.share`. It uses the existing rounding (whole percent, half up). It is read-only, additive and changes no other field. A test proves it for the "Oktober against Juli to September" amounts, for two usable periods and for no baseline.
- Fallback if rejected: row 2 while running reads "Rata-rata {n} bulan ({first}–{last})" · amount · "per bulan" with no percent and no bar.
- Phone: the meta and the bar are hidden; the grid is `minmax(0,1fr) auto 40px`, and the percent moves into the 40 px column.

Rejected:
- Computing the share in the browser: a second copy of the rounding rule and a figure the client computes (`analysis-workspace` forbids client money values).
- Keeping `ProgressCard` and `BaselineCard`: two cards for one comparison, with icon tiles.
- The prototype's percent row for a completed period: a share of the previous period is a verdict once the period is complete, so changes are clearer.

### D6. Kakeibo per pilar — flagged: scope

- Section "Kakeibo · per pilar" (`SectionHeader`, aside "siklus ini · porsi · periode lalu" while running, "jumlah · porsi · vs rata-rata 3 bulan" when complete, "jumlah · porsi" without either).
- One `.cmp-row` per pillar in Kakeibo order (Kebutuhan, Keinginan, Budaya, Tak Terduga, Belum diklasifikasi), shown when this or the comparison period is non-zero:
  - an 8 px square-cornered dot in `--k-*` (dashed `--k-none` ring for Belum diklasifikasi, `tactile-pulse-ui` "Kakeibo Pillar Marks") and the pillar name from `terms`;
  - amount (`comparison.pillars[].amount`, or `progress.pillars[].amount` while running);
  - share of Pengeluaran (`pillars[].share`, server);
  - running: `progress.pillars[].previous`; complete: `ChangeFigure` of `pillars[].baseline_change`, or "—" without a baseline.
- Each row is one link (whole row) to the ledger filtered to the period and pillar (`refHref(pillar.ref)`, as today).
- "Belum diklasifikasi" is a plain row with two targets (as Tinjau D9 allows): the amount links to the ledger (`kakeibo=unclassified`), and "Perlu dipilih" (`.btn-link`) opens `/laporan?lengkapi=1&basis=…&start=…`, the classification dialog for that window (deviation 6). On the phone "Perlu dipilih" moves under the label, never alone on a line.
- **The flag:** today's `BaselineCard` shows, while the running period is viewed, the last completed period against its average per pillar. Here, the per-pillar change against the average appears when the owner steps back to a completed period. The total against the average stays on the first screen (D5 row 2). Fallback: a second block "{last completed} vs rata-rata 3 bulan" under Kakeibo while running.

Rejected: the prototype's "periode lalu" amount for completed periods too (it hides the 3-month comparison the owner asked for); Kakeibo bars (`.kk-row`, SPEC §7): Analisis's §8 table has none.

### D7. Pengeluaran harian chart and table

- Section "Pengeluaran harian" (renamed from "Belanja harian"), aside: the shared `ChartTable` toggle, a quiet small button "Lihat sebagai tabel" / "Lihat sebagai grafik" (`aria-pressed`), h 28 (40 on the phone), padding 0 8.
- **SVG** (`DailySpendingChart`), drawn at the measured column width `W` (ResizeObserver), so text stays 12 px at every width (deviation 7); `H = 200`, bottom 28, top inset 8:
  - scale: `nice = ceil(max / 500000) × 500000` (100.000 steps when `max < 500000`, so small periods do not flatten);
  - bars: `step = (W − 8)/n`, `bw = max(4, floor(step) − 4)`, square, ≥ 2 px when the value is > 0; fill `--k-none`; the peak (first maximum, > 0) `--m-expense`; today's bar `--k-none` with the hatch pattern (4 × 4, 45°, 2 px `--s0` at 0.6);
  - gridlines at `nice/2` and `nice`, plus the baseline: 1 px `--line`; tick labels (`.ax`, Inter 12 `--muted`, `compactMoney`) above the two upper lines; no "0" tick (deviation 7);
  - date labels (`.ax`, centred, `shortDate`) every `ceil(n/8)` bars (`ceil(n/4)` below 400 px), plus today; a regular label within one step of today's is skipped, so labels never overlap;
  - days come from `daily_cadence[].expense`, up to today; later days are absent (`analysis-workspace` "Ringkasan Daily Spending").
- **Pointer and touch:** hover shows "{date}: {amount}" above the bar inside the chart; a tap toggles it; Esc or leaving the chart hides it (unchanged behaviour). Each bar also has a `<title>`.
- **Legend** (`.chart-foot`, gap 24): an 8 × 8 `--m-expense` swatch "Tertinggi: {date} {amount}", a hatched swatch "Hari ini, masih berjalan: {amount}" (only when today is in the period), and "Di luar tabungan dan investasi". Two encodings, two swatches (dataviz rule).
- **Accessibility:** `role="img"` with `aria-label` "Pengeluaran harian, di luar tabungan dan investasi. Tertinggi {date} {amount}." (masked when balances are hidden). The table: `table.tbl` with caption "Pengeluaran per hari", "Tanggal" | "Pengeluaran", rows `weekdayDate` and amount, " · tertinggi" and " · hari ini, masih berjalan".
- **Contrast:** `--k-none` bars measure ≥ 3:1 on `--s0` in both themes (`tokens.test.ts` pair added); the peak `--m-expense` ≥ 3:1; the hatched bar keeps ≥ 3:1 on its unhatched pixels and is named in the legend.

Rejected: a chart library (new dependency); scaling a 720 px drawing (text below 12 px at 1024, 13.7 px in the map); CSS div bars without an accessible chart (today's `aria-hidden`).

### D8. Pengeluaran per kategori

- Section header "Pengeluaran per kategori" with two actions: quiet small "Kelola kategori" and `.btn-link` "Atur anggaran" (D9).
- One `.grp` per pillar in Kakeibo order (`--rail` = the pillar colour; padding-left 16; 2 px rail at left 0, top 4, bottom 4; groups 16 px apart), with `.grp-h`: `h3.t-label` in `--ink` "KEBUTUHAN" and the count "7 kategori".
- Rows (`ListRow`, one `button` per row, whole-row target) for every category with spending in the period, sorted by amount, from `analytics.category_variance`:
  - title: the category name (user data);
  - budget: `Meter` 160 × 4 (pillar colour; `--m-expense` over 100 %, from `percentage_used`) and the meta (136 px, right) "59% dari Rp 1.000.000", the percent in `--expense` when over 100 %; or the meta "tanpa anggaran" with a 160 px spacer;
  - amount (`t-amt`, ink, no sign: a figure, not a signed row amount).
  Selecting a row opens the ledger filtered to the period, `type=expense` and the category (`category=<id>` or `uncategorized`), as `report-periods-and-drilldown` requires.
- **Rows the prototype leaves out (deviation 8):** "Tanpa kategori" in a "Belum diklasifikasi" group (`--k-none` rail); archived categories with spending in their pillar's group with the meta "diarsipkan · tanpa anggaran". Neither offers a budget or an edit.
- Below the groups, `.more-row` quiet "Tampilkan {n} kategori tanpa pengeluaran" / "Sembunyikan …", as today.
- Phone: the bar and the budget meta are hidden; the meta under the title reads "59% dari Rp 1.000.000" so budget information is not lost (deviation from the prototype, which drops it).

Rejected: the editable budget table (`CategoryBudgetSection`): management belongs in a dialog (INVENTORY "R (management)").

### D9. "Kelola kategori" and "Atur anggaran" dialogs

**`CategoryManagerDialog`** (`components/categories/`), `ShellDialog` 640 px, title "Kelola kategori", meta "Tambah, ubah dan arsipkan kategori":
- Body: one `.grp` per pillar (expense categories by `kakeibo_type`, "Belum diklasifikasi" last), then "Pemasukan" (`--m-income` rail), then "Diarsipkan" (`--k-none`), 48 px apart.
- Row (`ListRow` plain, no hover): title and meta ("anggaran Rp 1.800.000" / "tanpa anggaran"); two 36 × 36 icon buttons (44 on the phone): "Ubah {name}" and "Arsipkan {name}". Archive asks in the row with `ConfirmActionButton` ("Ya, lanjutkan"), as today. Archived rows show "Pulihkan" instead.
- Footer: secondary "Tambah kategori pemasukan", primary "Tambah kategori".
- **The form (deviation 10)** replaces the list inside the same dialog: title "Ubah {name}" / "Tambah kategori" with a "Kembali" icon button; fields Nama, Jenis (segment Pengeluaran · Pemasukan), Pilar (segment with "Belum diklasifikasi"), Ikon and Warna (named options, as `custom-category-management` requires), Anggaran bulanan (money input; expense only); footer "Batal" and "Simpan" / "Buat kategori". It sends exactly today's requests: `POST /categories`, `PATCH /categories/{id}` (an explicit `kakeibo_type: null` for "Belum diklasifikasi"), `DELETE /categories/{id}` (archive) and `PATCH {is_archived: false}` (restore). One dialog layer, focus moves to the form's first field and back to the row's button.

**`BudgetDialog`** (`components/categories/`, moved from `components/report/BudgetPanel.tsx` `BudgetEditorDialog`), `ShellDialog` 560 px, title "Atur anggaran": one money input per expense category that reports count, "Tanpa batas" placeholder, `PATCH /categories/{id}` per changed category, "Batal" and "Simpan anggaran". "Atur anggaran" on Analisis opens it; Laporan's panel imports it from the new path (no visual change on Laporan until phase 7).

Rejected:
- Linking "Atur anggaran" to Pengaturan (the prototype): Pengaturan › Anggaran holds only the overall monthly limit.
- A second dialog over the first for the form: two stacked layers.
- The prototype's toast-only buttons: they would drop working features.

### D10. Context panel: Tagihan & langganan and Laporan bulanan

`aside.panel` labelled "Konteks" (`--s1`, radius 12, padding 24; 16 on the phone):
- **Tagihan & langganan** (from `GET /dashboard/recurring-charges`, unchanged): `SectionHeader` with the aside "{count} · {monthly_equivalent_total}/bln"; the four largest charges by monthly equivalent as `ListRow`s (whole-row link to the ledger with `q`): title the merchant, meta "bulanan · berikutnya {date}" (or "mingguan"), amount `t-amt` `--sm`; then two buttons: secondary "Lihat semua" and quiet "Jadikan rencana".
  - "Lihat semua" (deviation 11) opens a 560 px `ShellDialog` "Tagihan & langganan terdeteksi" with today's full list: cadence, count, account, typical amount, last date, the estimated next date, "Nominal terakhir berubah: A → B", and per charge "Sudah ada rencana: {name}" or "Jadikan rencana".
  - "Jadikan rencana" opens `RecurringRulesModal` prefilled from the largest charge without a plan, after owner confirmation, as today (`observed-insights` "Recurring charges and plans"). It is hidden when every charge has a plan.
  - Empty: "Belum ada tagihan berulang yang terlihat." Failure: the SPEC error box with "Coba lagi".
- **Laporan bulanan** (from `GET /reports/monthly/latest`, which the shell already loads): a whole-block link to the report's `url`, `.sec-h` "Laporan bulanan" with a 16 px chevron, `t-body` "Laporan {label} siap dibaca" and `t-meta` "Empat pertanyaan Kakeibo, satu per bagian". Hidden when `ready` is false.

Rejected: a "Rincian" button (D4 holds the page's one Rincian); listing plans instead of detected charges (Target & Tagihan owns plans).

### D11. Kalender grid, markers and heat (known item: marker contrast)

- **Hero:** label "Pengeluaran periode ini", figure `totals.spend` (renamed from "Belanja periode ini"), line "Hari ke-{i} dari {n}" plus " · {d} hari lagi sampai gaji {date}" for a cycle with an expected salary, or "Tanggal gaji berikutnya belum bisa diperkirakan: {reason}." (`cycle-calendar` "Kalender Grid").
- **Grid** (`div.cal[role=grid]`, labelled "Kalender {period}"): 7 × `minmax(0,1fr)`, 1 px `--line` top and left; `.dow` column headers Sen…Min (`weekday` short names in the interface language), 12/16 500 uppercase `--muted`.
- **Cell** (`button.cal-cell[role=gridcell]`, roving `tabindex`, `aria-selected`): min-h 80 (56 on the phone), padding 8 (4), right and bottom 1 px `--line`, flex column; hover `--s1`; selected `--s2` with `::before` 2 px `--accent` across the top (a straight bar on a square cell). Days outside the period are `div.cal-cell.is-out` on `--s1`, `aria-hidden`.
  - `.d`: the day number, "1 Nov" on the first of a month and on the period's first day; 12/16 `--muted`; today `--ink` 500 with `aria-current="date"`.
  - amount: `compactMoney(spend)` in `t-amt` `--ink` (12/16 500 on the phone) for days up to today with spending; amounts sit at one height whether or not the cell has markers (fixed rows, not `space-between`).
  - `.marks` (`aria-hidden`): 6 × 6 squares, 4 px apart: Masuk `--m-income`, Pindah `--m-transfer`, Top up `--m-invest`, Tagihan `--m-warning` (plans not yet met); Menunggu jawaban is an outlined square (1.5 px inset `--warning`). Filled versus outlined is the non-colour cue between Tagihan and Menunggu.
  - `.heat` (`aria-hidden`): 4 px at the bottom, `--m-expense` at opacity 0.2, 0.4, 0.6, 0.8 or 1.0 for the server's `heat` level 1–5; none at 0 (deviation 12).
  - accessible name: "{weekday date}. Pengeluaran {amount}. {waiting} menunggu jawaban. Masuk, Pindah, Tagihan {plan}." built from one message with optional parts.
- **Legend** (`.chart-foot`): the five markers with their names, and the heat scale "Pengeluaran per hari" with five 8 × 4 steps from "sampai {threshold 1}" to "di atas {threshold 4}" (`heat_thresholds`).
- **Contrast (the known item):** markers are no longer text on heat fills, so the axe `color-contrast` finding has no element to fail. Every marker colour must measure ≥ 3:1 against `--s0`, `--s1` and `--s2` in both themes (`tokens.test.ts` pairs). Where a light-theme `--m-*` falls below 3:1 on `--s2` (the selected cell), the marker takes its text ink (`--transfer`, `--income`, `--invest`, `--warning`) on the selected cell only; the test fixes which. The heat strip carries no information that the cell's text does not, so it is exempt from 1.4.11 and marked `aria-hidden`; the selected cell's bar keeps the `--s2` fill and `aria-selected` as its second cue.

Rejected: keeping `--heat-1..5` fills (they hid markers and failed contrast); the prototype's opacity from the share of the peak day (a second, client-side heat rule beside the server's levels).

### D12. Kalender day panel and "Sampai akhir periode"

`aside.panel` labelled "Detail hari", `aria-live="polite"`:
- `SectionHeader` "{weekday date}" plus " · hari ini" or " · kemarin"; figure `t-fig` the day's spend; meta "pengeluaran hari ini" or "pengeluaran hari itu" (fixing the prototype's fixed text).
- "Menunggu jawaban · {n}": rows link to their Tinjau item (`/review?item=`), as today.
- Plans that day: "{name} · {amount} terjadwal" with their status words (Belum dibayar, Terlambat, Sudah dibayar, Dilewati).
- Transactions: `ListRow`s (time, title, meta "{time} · {pillar}", `RowAmount` signed and coloured, expense red) from `GET /transactions?from_date&to_date&logical_movements=true&limit=50`; each opens the shared `TransactionDialog`, and closing returns focus to the row.
- Secondary "Lihat di Transaksi" opens the ledger filtered to that day (`from`, `to`), fixing the prototype, which ignored the day.
- Empty: "Hari ini belum tiba." for a future day, "Tidak ada transaksi." otherwise.
- Second block, current period only: "Sebelum gaji berikutnya" (or "Sampai akhir periode" when the date is unknown), meta "Tagihan yang belum dibayar", rows "{name}" · "{date}" or "{date} · perkiraan" · amount, each selecting its day; "Total {upcoming_total}" from the server.
- Default day: today in the running period, otherwise the last day; `?day=` wins when it lies inside the period.

### D13. Kalender on the phone

Below 768 px the whole grid stays (cells min-h 56, padding 4, amount 12/16 500), as the prototype shows, and the week strip goes (deviation 13). At 358 px a cell is 51 px wide; "1,3jt" at 12 px fits. At 320 px a cell is 45 px wide: the amount drops its decimal ("1jt") through `compactMoney`'s narrow form and never overflows. The day panel follows the grid.

Rejected: keeping the week strip (one week at a time hides the month the prototype shows).

### D14. Peta Aliran figures and highlight

- **Hero:** label "Aliran uang periode ini · masuk", figure `summary.masuk`.
- **Supporting figures** (`.supports`, `--n: 4`, gap 24, `margin-top: 24px`; 2 × 2 on the phone), `FigureBlock bar={false}`: "Pengeluaran" `summary.belanja`; "Ditabung & top up" `saving_net + investment_net`; "Menunggu pasangan" `waiting_out + waiting_in`; "Masih ada" `remaining` with its sign. The two sums are today's, unchanged.
- **Toolbar** (`margin: 32px 0 16px`): `h2.t-label` "Peta", `Segmented` "Sorot aliran" (Semua · Pengeluaran · Pindah saldo · Tabungan & investasi · Menunggu pasangan; "Belanja" renamed), `Segmented` "Tampilan" (Peta · Tabel), `?view=tabel` as today. On the map the highlight dims the other bands; in the table it filters rows (both as today).
- Caption: "Lebar pita sebanding dengan jumlah uang. Saldo sebelum periode dan Masih ada ada di tabel."

### D15. Peta Aliran map (known item: labels) — flagged: scope (fold removed)

- **Layout:** `sankey.ts` keeps its model (folding of pockets, netting, mirroring, deterministic order) and conservation tests. Its layout options change to the prototype's look: four columns (Dari · Rekening · Lalu ke · Dipakai atau disimpan), node bars 8 px wide in `--ink`, stations removed (accounts are bars like every node), minimum slot 24, gap 8, top 32, scale so the tallest column is at most 320 px.
- **Bands:** fill by kind (`--m-income`, `--m-transfer`, `--m-expense`, `--m-invest` for saving and investment, `--m-warning` for waiting), `fill-opacity` 0.30, 0.06 when dimmed; transfer, investment and waiting bands keep their stripe, dot and hatch overlays (deviation 14). Each band has a `<title>` "{from} → {to}: {amount}".
- **Labels inside the frame:** the SVG is drawn at the measured width (min 720 px, horizontal scroll inside `.flow` below that); a right label gutter of 220 px is reserved inside the viewBox; labels sit right of their node (`.lbl-node` 12 `--ink`, `.lbl-amt` 12 `--muted`, 8 px apart) on a 4 px `--s0` halo; a name longer than the gutter is cut with "…" and the full name stays in the node's accessible name and `<title>`. A test asserts every label's box lies inside the viewBox at 1440, 1200 and 1024 px.
- **Column heads:** "DARI", "REKENING", "LALU KE", "DIPAKAI ATAU DISIMPAN" (`.lbl-amt`, 0.06em tracking), at y 12.
- **Legend** (`.chart-foot`, new; the prototype has none): Masuk, Pindah saldo (garis miring), Pengeluaran, Tabungan & investasi (titik), Menunggu pasangan (arsir).
- **Keyboard:** nodes stay focusable `role="button"` groups with "{label}: masuk {in}, keluar {out}." names; arrow keys move within and across columns, Enter opens the detail (D16), Esc closes it with focus returned (unchanged).
- **The flag (fold removed):** income bands are always drawn; the opening balance, negative balances and "Masih ada" links are never drawn on the map. "Masih ada" is the fourth supporting figure, and the table lists every link. The localStorage key `analysis.flow.folded` is no longer read. Fallback: keep the control, folded by default.
- Phone: the table is the default view and the primary content (`money-flow-map` "Peta Aliran Table"); choosing "Peta" shows the map scrolling inside its box. `MiniMap` is removed.

Rejected: keeping the stations and the five-column layout (the source of the labels past the frame); colour without pattern (1.4.1); a legend-free map (the owner's chart rule).

### D16. Peta Aliran table and details

- **Table** (`table.tbl` in `.tbl-scroll`, caption "Semua aliran sampai {date}, termasuk saldo awal dan Masih ada"): "Dari → Ke" (a button that opens the link detail), "Jenis" (with " · termasuk {n} menunggu jawaban"), "Jumlah" (num), "Transaksi" (num, hidden below 768 px; deviation 15). Every link of the response exactly once, sorted by amount.
- **Details:** `ShellDialog` 560 px (a sheet below 768) replaces the `Drawer`:
  - node: "Masuk", "Keluar", "Saldo sebelum periode", "Saldo di akhir hari" (`.dl`), the "Masuk dari" and "Keluar ke" link lists, "Jawab di Tinjau" when waiting rows exist, "Lihat semua transaksi rekening ini";
  - link: "Jumlah", "Transaksi", "Menunggu jawaban", then the rows 50 at a time (`ListRow` with `RowAmount`, "Muat lagi"), each opening `TransactionDialog`.
  The map keeps its full width while open; Esc returns focus to the node, band button or table button.

### D17. Replay removed

"Putar ulang", "Jeda", the slider `#flow-replay`, the day readout, `REPLAY_MS`, the prefetch and the reduced-motion caption are removed (INVENTORY [R], plan phase 6). The web always requests the flow without `through`, so it shows the period to today or to its end. `GET /dashboard/flow` keeps `through` for API consumers; `money-flow-map` "Flow Endpoint" is unchanged.

### D18. Moved system cards

- **"Seberapa sering ditanya"** moves to Pengaturan › Kebiasaan (`/settings/kebiasaan`) as a section after the two tables, with today's component and behaviour (`QuestionRate`, `GET /dashboard/question-rate?weeks=8`), moved to `components/settings/QuestionRate.tsx`. Pengaturan is untranslated until phase 10, so its strings stay Indonesian in the source under the copy register's untranslated roots.
- **"Kebiasaan dihafal"** is removed. Its content (ready habits) is the first table of the same Pengaturan section, which its "Lihat semua" opened.
- Both leave Analisis in the same change, so no release lacks the panel.

### D19. Loading, failure and empty states

Per tab, inside the panel (`analysis-workspace` "Analisis Layout and Accessibility"):
- Loading: SPEC skeletons (hero bar, two 48 px rows, a 200 px chart block; the Kalender grid as 35 static cells; the flow as a 320 px block), `role="status"`, `aria-busy="true"`, no pulse.
- Failure: the SPEC error box (`role="alert"`, alert icon 20, title 14/500, one line 12/400, secondary "Coba lagi"), per query, so the stepper and the other tabs keep working.
- Empty: "Belum ada pengeluaran di periode ini." on Ringkasan (no Rp 0 figures presented as results), "Belum ada uang yang bergerak." on Peta Aliran; Kalender always shows its grid.

### D20. Keys and focus

- Tabs: ←/→, Home, End.
- Kalender: ←/→ ±1 day, ↑/↓ ±7, Home/End to the week's start or end, PageUp/PageDown ±7, Enter selects; the keys work whatever the single-key shortcut setting is (fixing the prototype), because they act only with focus in the grid.
- Peta Aliran: arrow keys between nodes, Enter opens, Esc closes.
- Every dialog traps focus and returns it to its opener. Focus is never hidden under the sticky top bar (`scroll-margin-top` 72 px).
- The Bantuan dialog lists "Panah — Pindah hari di Kalender" and the tab keys in the page's key group.

### D21. URL, deep links and history

Unchanged: `?tab=`, `?day=`, `?view=tabel`, written with `replaceSearch` and keeping other parameters; unknown values fall back. Links out: the ledger with the period and pillar or category; `/review?item=`; `/laporan`, `/laporan?lengkapi=1` with the window, and the report's own `url`; `/settings/kebiasaan`. Links in: Beranda's `/insights`; Laporan's `/insights?tab=ringkasan` (recurring fact), which keeps working.

### D22. Language

- **Namespace `analysis`:** `page`, `tabs`, `hero`, `compare`, `kakeibo`, `daily`, `categories`, `bills`, `report`, `calendar`, `day`, `upcoming`, `flow`, `flowTable`, `flowDetail`, `manager`, `form`, `budget`, `states`, `announce`, `keys` (Appendix A).
- **Terms:** figure and concept names come from `terms` (`spending`, `income`, `savedInvested`, the pillars, `noPillarYet`, `payCycle`, `calendarMonth`, `transfer`, `bills`). This phase adds `terms.stillLeft` ("Masih ada" / "Still left"), `terms.waitingForPair` ("Menunggu pasangan" / "Waiting for its pair") and `terms.threeMonthAverage` ("rata-rata 3 bulan" / "3-month average"), checked against the glossary rules (no near-duplicates).
- **Formats:** every date, weekday, month, amount and compact amount through `useFormat()`; `lib/format.ts` gains `compactMoney` (ID: "79rb", "1,3jt"; EN: "79K", "1.3M"; region decides the decimal mark; masked "••"). `components/analysis/format.ts` keeps only non-display helpers. `rg 'id-ID|Intl\.' components/analysis app/insights` finds nothing.
- **ICU:** every interpolated string is one message with named arguments; counts use `plural`.
- **Server text:** period labels, flow node labels ("Saldo sebelum periode", "Masih ada", "Tidak dihitung belanja") and explanation lines arrive in the account's language from `localize-server-text`. User data (categories, accounts, merchants) is shown as stored, without a `lang` override.
- **Registry and guards:**
  - `/insights` joins `lib/i18n/translatedRoutes.ts`, and its entry leaves `components/workspace/untranslatedPages.ts`;
  - the untranslated-page test example, which phase 5 moved to Analisis, moves to Pengaturan (`/settings`), untranslated until phase 10 (coordinator, 2026-11-09: phase 7 translates Laporan first);
  - the copy register's translated roots gain `app/insights`, `components/analysis` and `components/categories`, and the untranslated roots lose `components/analysis`, `components/insights` and `app/insights/page.tsx`; `components/settings/QuestionRate.tsx` falls under the existing `components/settings` untranslated root;
  - the Indonesian spending-term guard (`SPENDING_ID`) covers `analysis.*`, so "Belanja" can never label a figure again.

### D23. Hidden balances

With "Sembunyikan saldo" every amount reads "Rp ••••••" (`MaskedMoney`, announced "Disembunyikan" / "Hidden"): hero, line, comparison amounts and `ChangeFigure`, Kakeibo, category amounts and budget metas, chart ticks (blank), the chart's legend and `aria-label`, the table, calendar cells ("••"), the day panel, the bills panel, flow figures, labels ("Rp ••••") and the table. Percentages stay, because they reveal no amount.

### D24. Shared components and CSS

Built here, used by Analisis and by phase 7:
- `Meter` (`components/workspace/figures.tsx`): `.prog`, h 4, track `--s2`, square fill, `value` 0–100 clamped, `tone` (a token), `aria-hidden` by default (the text beside it carries the value).
- `ChangeFigure` (same file): the ▲/▼/"sama" amount with its hidden words, `--expense` for a rise in spending and `--income` for a fall, money through `useMoney`; replaces `ComparisonParts`' `Change`, which formats with a fixed `id-ID` and leaks hidden balances (phase 7 removes the old one).
- `ChartTable` (`components/charts/ChartTable.tsx`): the toggle button and the swap between a chart and its table, with `aria-pressed` and focus kept on the button.
- `format.compactMoney` (`lib/format.ts`).
- `BudgetDialog` (`components/categories/`).
- CSS from `styles.css` into `app/globals.css`: `.tabs`, `.tab`, `.cmp-row`, `.prog`, `.grp`, `.grp-h`, `.cal`, `.dow`, `.cal-cell`, `.heat`, `.marks`, `.chart`, `.chart-foot`, `.ax`, `.flow`, `.lbl-node`, `.lbl-amt`, `.supports`, `.tbl`, `.tbl-scroll`, `.more-row`, `.caption`.

Reused unchanged: `HeroFigure`, `FigureBlock`, `SectionHeader`, `Panel`, `PanelBlock`, `ListRow`, `RowAmount`, `TransactionDialog`, `ShellDialog`, `ExplanationDialog`, `Segmented`, `PeriodStepper`, `MaskedMoney`, `Skeleton`, `EmptyState`, `ErrorState`, `ConfirmActionButton`, `RecurringRulesModal`.

Rejected: page-local copies of the meter and the change figure (Laporan needs both); lifting `ReportLayout` (it measures its own width).

### D25. What happens to each removed element

| Element | Destination |
| --- | --- |
| KPI row (Masuk, Pengeluaran, Ditabung & investasi) | the hero and its one line (D4) |
| Dotted `ExplainableFigure` on Pengeluaran | the hero's "Rincian" (D4); the component stays for Rekening until phase 8 |
| `ProgressCard`, `BaselineCard` | the comparison rows and Kakeibo rows (D5, D6) |
| "Belanja harian" CSS chart | `DailySpendingChart` (D7) |
| `CategoryBudgetSection` (table, cards, income list, archived list, Modal form) | the category list (D8) and the two dialogs (D9) |
| `RecurringCharges` card (`ObservedInsights.tsx`) | the bills panel and its "Lihat semua" dialog (D10) |
| `QuestionRate` card | Pengaturan › Kebiasaan (D18) |
| `HabitsLearnedCard` | removed; Pengaturan › Kebiasaan lists the habits (D18) |
| `ProgressBand`, `UpcomingCard` | the Kalender hero and the panel's second block (D11, D12) |
| Heat fills `.h1`–`.h5`, `--heat-1..5` and their `tokens.test.ts` pairs | the 4 px strip (D11) |
| Text markers and the "?" tag | 6 × 6 markers and the accessible name (D11) |
| `WeekStrip` | the full grid on the phone (D13) |
| Stations, five column heads, `MiniMap`, `mapWide` | the four-column map (D15) |
| Fold control and `analysis.flow.folded` | removed (D15, flagged) |
| Replay | removed (D17) |
| Four summary cards (`.sums`) | the supporting figures (D14) |
| `Drawer` in Peta Aliran | `ShellDialog` (D16) |
| `components/charts/theme.ts` | removed (dead code) |
| Analisis's use of `ReportLayout`, `ComparisonParts`, `useWidthAtMost`, `KpiStrip`, `Kpi` | removed here; the components stay for Laporan and Rekening until phases 7 and 8 |

### D26. Verification tooling and gate

The plan's common gate, with Analisis's scenarios:
- **Scenarios** (prototype and app, same seed): `analisis` (running cycle), `analisis-bulan`, `analisis-lalu` (completed period), `analisis-tabel`, `analisis-kalender`, `analisis-kalender-hari` (a past day with waiting rows), `analisis-peta-aliran`, `analisis-peta-aliran-tabel`, `analisis-sorot`, `dlg-kategori`, `dlg-kategori-ubah`, `dlg-anggaran`, `dlg-tagihan`, `dlg-rincian-pengeluaran`, `dlg-aliran-node`, `dlg-aliran-link`, `analisis-sembunyi`, and loading, failure and empty per tab; plus `pengaturan-kebiasaan` with the moved panel.
- **Matrix:** 1440 × 900, 1024 × 768 and 390 × 844 in light and dark, in `en` and `id`, with composed left/right images; plus 1280, 1200, 1199, 768, 767 and 320 for overflow.
- **Measurement checklist** (`analisis-checklist.js`, generated from the D-tables): title block and first content at `y = 192`, tabs (h 40, bar 2 px), `.cols` 728 | 352 at 1440, hero (44/52), `.cmp-row` columns 1fr/120/72/120 and min-h 48, Kakeibo dots 8, chart bar width formula, gridline positions, label sizes 12 px at every width, `.grp` rail 2 px and padding 16, rows 56 (64 on the phone), `Meter` 160 × 4, cells 80/56, markers 6 × 6, heat 4 px, the panel (padding 24, radius 12), the flow's node width 8, band opacities 0.30/0.06, the gutter, the dialogs (640, 560, 480).
- **SPEC §13 rules:** only Inter and Manrope; sizes ⊆ {12, 14, 16, 20, 24, 32, 44}; weights ⊆ {400, 500, 600}; ≤ 8 text styles per tab; one 44 px figure per tab and none other ≥ 33 px; one "Rincian"; no dotted underline; no border ≥ 2 px and no coloured border except selection bars, rails, the calendar marker and focus.
- **Known items:** axe on Kalender at 1024, 768, 390 and 320 in both themes has no `color-contrast` finding; every flow label lies inside the frame at 1440, 1200 and 1024; the English stepper reads "Cycle 23 Oct – 24 Nov" and "Oct 2026" on Analisis; no horizontal overflow on any tab at 320; no dotted figure on Analisis.
- **Figure parity:** every amount, count, percent and date on the three tabs and in their dialogs, before (task 1.3) against after, and `en` against `id`.
- **Keyboard flows** (`analisis-keys.mjs`): tabs; the Rincian dialog; the chart table toggle; a category row to the ledger; Kelola kategori list → form → save → back; Atur anggaran; Lihat semua and Jadikan rencana; the calendar walk (arrows, Home, End, PageUp, PageDown, Enter) with single-key shortcuts on and off; a day row → `TransactionDialog` → focus back; Lihat di Transaksi; the flow node walk and Esc; the flow table button; the stepper and the segment.

### D27. Spec reconciliation

| Capability | Requirement | Change |
| --- | --- | --- |
| `analysis-workspace` | Analisis Tabs in the URL | MODIFIED: underline tabs with the report link; URL unchanged |
| | Period Basis for All Analisis Tabs | MODIFIED: stepper with the period's dates; replay scenario replaced |
| | Ringkasan Figures and Names | MODIFIED: hero, one line, one Rincian |
| | Ringkasan Kakeibo Comparison | MODIFIED: comparison rows and Kakeibo rows |
| | Ringkasan Daily Spending | MODIFIED: "Pengeluaran harian", SVG and legend |
| | Ringkasan Supporting Cards | REMOVED |
| | Analisis Layout and Accessibility | MODIFIED: 1200/768 breakpoints, 320 px |
| | Ringkasan Context Panel; Spending by Category on Ringkasan; Analisis in Both Languages | ADDED (the states stay in the layout requirement) |
| `cycle-calendar` | Calendar Heat Levels, Kalender Grid, Kalender Day Sheet | MODIFIED |
| | Kalender Phone Week Strip | REMOVED; Kalender on the Phone ADDED |
| `money-flow-map` | Peta Aliran Map and Keyboard, Peta Aliran Table, Peta Aliran Details Open Centered | MODIFIED |
| | Peta Aliran Replay, Peta Aliran Fold | REMOVED; Peta Aliran Figures and Peta Aliran Balances ADDED |
| `monthly-report` | Whole-Period Comparison API | MODIFIED: `progress.baseline_share` |
| `habit-learning` | Seberapa Sering Ditanya Panel | MODIFIED: in Pengaturan › Kebiasaan |
| `finance-dashboard-cockpit` | Inline Category Budget Editing | MODIFIED: the dialog |
| `custom-category-management` | Category Budget Variance Filtering | MODIFIED |
| `report-periods-and-drilldown` | Non-Editable Insight Rows | MODIFIED |
| `indonesian-daily-finance-ui` | Intuitive Daily Finance Terminology | MODIFIED: the Analisis scenario |
| `monthly-navigation-stepper` | Month-and-Year Stepper Navigation | MODIFIED: example page Pengaturan (written over phase 5's version) |
| `tactile-pulse-ui` | No Decorative Effects | MODIFIED: examples Laporan and Target & Tagihan, Analisis scenario added (written over phase 5's version) |

No delta: `observed-insights` (behaviour unchanged; "Not shown by the web" still holds), `explainable-figures` (its rules already cover a redesigned page), `interface-language`, `ui-copy-register`, `design-tokens` (the heat tokens are named by no requirement).

The `monthly-navigation-stepper` and `tactile-pulse-ui` deltas copy phase 5's merged wording; task 1.2 re-bases them on the merged text if it differs.

### D28. Dependencies and order

1. `add-language-system` (merged), `add-category-system-keys` (merged), `localize-server-text`.
2. Phase 4 `apply-markas-tinjau`: `HeroFigure` optional mark, `ListRow` `rowKey`/`current`/`selected`.
3. Phase 5 `apply-markas-transaksi`: the restyled `PeriodStepper` and its spec, `RowAmount` (red expense), `TransactionDialog` (Kalender and Peta Aliran already import it after phase 5's rename), `HeroFigure` `aside`/`line`, `FigureBlock bar={false}`, `.tbl`, `.dl`, the untranslated-test example at `/insights`.
4. **This change.** It stops at task 1.2 and reports if any of the above is not on `main`.
5. Phase 7 `apply-markas-laporan` follows and reuses D24's pieces.

`add-credit-spending-flow` is not a dependency (D29).

### D29. Credit figures — flagged: scope

The credit change (`2026-11-06-add-credit-spending-flow`, not yet merged) hands nothing to Analisis or Laporan: its blocks are H5 (Transaksi), H8 (Rekening), H9 (Target, Beranda, bell) and H11 (Catat). Its two informational figures, `spent_on_credit` ("Pengeluaran pakai kredit") and `paid_to_credit` ("Tagihan kredit dibayar"), exist only in the current cycle summary and per credit account (`GET /api/credit/accounts/{id}` `totals`). Analisis shows any period, so it would need them in the analytics window, which is backend work outside a UI phase. Default: not shown. A credit purchase already counts in Pengeluaran and a bill payment appears as Pindah saldo in Kalender and Peta Aliran, as today. Fallback: a later backend change adds both figures to the analytics window, and a UI change shows them as lines of the Pengeluaran Rincian.

### D31. One spending basis — owner decision (2026-11-08)

Found while building: the page mixed two server figures under one name. Ringkasan's hero, the comparison and the Kakeibo rows read Pengeluaran (`comparison.totals.spending`: reportable expenses whose pillar is not Tabungan, net of reimbursements per category and pillar, the rule of Beranda and Transaksi). The Kalender (`totals.spend`, `days[].spend`, the heat levels), Peta Aliran (`summary.belanja`), the daily chart (`daily_cadence[].expense`) and the category list (`category_variance[].spent`) read Belanja, which also counts expenses with the Tabungan pillar. The former page named the second group "Belanja"; D7, D8, D11 and D14 renamed it "Pengeluaran", so a period with a saving-pillar expense showed two different "Pengeluaran" amounts.

Owner decision (2026-11-08, option "one basis"): every "Pengeluaran" on Analisis is the Pengeluaran figure.
- **Server, additive and read-only:** `services/spending_basis.py` computes Pengeluaran per local day and per category for a window: the reportable expenses that are not saving-pillar expenses (the predicate `saving_pillar_sql` in `services/saving_flow.py`, which the saving direction already uses), less each category and pillar's applied reimbursement, never below zero, spread over that category and pillar's days in proportion to their amounts (`apportion`). Its total equals `comparison.totals.spending` for the same window.
- **New fields:** analytics `daily_cadence[].spending`, `category_variance[].spending` and `spending_percentage_used` (same rounding as `percentage_used`); calendar `totals.spending`, `days[].spending`, `days[].spending_heat` and `spending_heat_thresholds` (the same percentile rule on the Pengeluaran amounts); flow `summary.spending`. Every existing field keeps its value.
- **Page:** the daily chart, the category amounts and budget shares, the Kalender hero, cells, heat strip, legend and day figure, and Peta Aliran's Pengeluaran figure read the new fields. Expenses with the Tabungan pillar stay visible in Ditabung & investasi on Ringkasan and in the Kakeibo Tabungan bucket of Laporan; the flow's bands, table and link rows are unchanged (they show money moving, not a figure named Pengeluaran).
- **Proof:** pytest on an owner with a saving-pillar expense and a reimbursement: the calendar, flow, daily and category totals equal `comparison.totals.spending`, every Belanja-basis field is unchanged; a page test and the parity run find every "Pengeluaran" figure of a period equal.

Rejected: keeping the plan's labels (two different "Pengeluaran" figures); naming the Belanja-basis figures differently (a second spending concept the glossary does not have).

### D30. Capital gain

Analisis has no capital-gain field: investment top-ups appear as money moved into investments, and gains belong to Rekening (phase 8), which shows them wherever they exist. Nothing to show here.

## Decisions and their defaults

Approved under the owner's standing instruction "complete all until finished" (2026-11-06, renewed 2026-11-08). Every decision takes its recommended default. **Flag** marks a decision reviewed before implementation because it adds backend output or changes scope beyond the approved phased plan.

| # | Decision | Default (taken) | Rejected alternatives | Flag |
| --- | --- | --- | --- | --- |
| D1 | Structure | `.cols` from 1200, one column below, phone below 768, media queries only | `useWidthAtMost`; two columns at 1024 | — |
| D2 | Stepper | shared stepper with the period's dates; reset kept | page-local stepper; server label | — |
| D3 | Tabs | ARIA tab list styled as underline tabs, `?tab=` | hash links; shared Tabs primitive | — |
| D4 | Hero | Pengeluaran, one line, one Rincian | dotted figure; several Rincian | — |
| D5 | Comparison rows | progress shares while running, changes when complete; server `baseline_share` | client share; two cards; shares for complete periods | **backend addition, money presentation** |
| D6 | Kakeibo | change against the average when complete | previous amount always; `.kk-row` bars | **scope** |
| D7 | Daily chart | SVG at real width, legend, table toggle | library; scaled drawing; div bars | — |
| D8 | Categories | pillar groups, budget meters, whole-row ledger links, Tanpa kategori and archived kept | editable table | — |
| D9 | Dialogs | list and form in one dialog; shared BudgetDialog | Pengaturan link; stacked dialogs; toast-only | — |
| D10 | Panel | detected charges with "Lihat semua"; Laporan link | Rincian button; plans list | — |
| D11 | Calendar | 6 × 6 markers, server heat level strip, legend | heat fills; client heat rule | — |
| D12 | Day panel | dialog rows, ledger link for the day | drawer; period-wide link | — |
| D13 | Phone calendar | full grid | week strip | — |
| D14 | Flow figures | four supporting figures, "Pengeluaran" filter | summary cards | — |
| D15 | Flow map | four columns, gutter, patterns and legend, no fold | stations; colour only; fold kept | **scope** |
| D16 | Flow details | centered dialog; table with Transaksi column | drawer | — |
| D17 | Replay | removed; API keeps `through` | kept | — |
| D18 | System cards | QuestionRate to Pengaturan; habits card removed | both kept on Analisis | — |
| D19 | States | per tab, SPEC boxes | page-wide error | — |
| D20 | Keys | unchanged sets, independent of single-key setting | prototype's coupling | — |
| D21 | URL | unchanged | — | — |
| D22 | Language | `analysis` namespace, `/insights` translated, example to `/laporan` | — | — |
| D23 | Hidden balances | every amount masked | — | — |
| D24 | Shared pieces | Meter, ChangeFigure, ChartTable, compactMoney, BudgetDialog shared | page-local copies | — |
| D29 | Credit figures | not shown | backend field in this phase | **scope** |
| D30 | Capital gain | none on Analisis | — | — |
| D31 | Spending basis | Pengeluaran-basis server fields shown everywhere on Analisis | plan's relabelling of Belanja; another name for Belanja | **owner decision 2026-11-08** |

No default changes money behaviour: every action sends today's request, and the backend additions are read-only. By the owner's decision D31 the Kalender, Peta Aliran's Pengeluaran, the daily chart and the category amounts show the Pengeluaran figure instead of Belanja; every existing API field keeps its value.

## Risks / Trade-offs

- [Phase 4 or 5 merges with different component names] → task 1.2 records the differences and maps D24's reuse onto the merged names before any page work.
- [`baseline_share` rejected at review] → the fallback row needs no backend change; tasks 2.x are skipped and 5.x render the fallback.
- [The four-column layout changes node order for owners with many accounts] → `sankey.ts`'s model and order are unchanged; only geometry changes; the conservation tests and a 17-pocket stress capture guard it.
- [Removing the fold surprises an owner used to it] → captured for the end review with its fallback.
- [Marker colours below 3:1 on `--s2`] → the token test decides the selected-cell ink before the grid is built.
- [The calendar at 320 px] → `compactMoney`'s narrow form and a capture at 320 in both languages.
- [Pengaturan shows the moved panel in the old styling] → accepted until phase 10; captured.

## Migration and rollback

No data migration. The backend addition is a read-only field. Rollback is a revert of the merge commit; the web returns to the old page, and the field disappears without data effects. The localStorage key `analysis.flow.folded` is left in browsers and ignored.

## Open Questions

None that change the specs or tasks. The flagged decisions have fallbacks written into the tasks.

## Appendix A. Analisis copy as message keys (`analysis` namespace; Indonesian · English)

Arguments arrive formatted from `lib/format.ts`; `{count}` selects plural forms (English `one`/`other`, Indonesian `other`). Existing Indonesian is kept byte for byte where the page keeps the text; "(new)" marks new strings and "(changed)" a changed one. Task 3.1 writes the remaining keys (error and empty texts of the dialogs, every `aria-label`) into `research/evidence/apply-markas-analisis/copy-keys.md` before the page work.

### `page`, `tabs`, `hero`, `compare`

| Key | Indonesian | English |
| --- | --- | --- |
| `page.title` | Analisis | Analysis |
| `page.subtitle` | Ke mana uang digunakan, dibanding periode lalu | Where the money went, compared with the last period |
| `page.basis` | Dasar periode | Period basis |
| `page.rollingNote` (changed) | Analisis memakai siklus gaji atau bulan kalender. Yang tampil: siklus ini. | Analysis uses pay cycles or calendar months. Showing: this cycle. |
| `tabs.label` (new) | Bagian analisis | Analysis sections |
| `tabs.overview` | Ringkasan | Overview |
| `tabs.calendar` | Kalender | Calendar |
| `tabs.flow` | Peta Aliran | Money flow |
| `tabs.readReport` | Baca Laporan Bulanan | Read the monthly report |
| `hero.label` (new) | Pengeluaran · {period} | Spending · {period} |
| `hero.labelRunning` (new) | Pengeluaran · {period} sejauh ini | Spending · {period} so far |
| `hero.line` (new) | Masuk {income} · Ditabung & investasi {saved} | Income {income} · Saved & invested {saved} |
| `hero.breakdown` | Rincian | Breakdown |
| `hero.breakdownLabel` (new) | Rincian pengeluaran | Spending breakdown |
| `compare.shareOfPrevious` (new) | {share}% dari total {period} | {share}% of the total for {period} |
| `compare.shareOfAverage` (new) | {share}% dari rata-rata {count} bulan ({first}–{last}) | {share}% of the {count}-month average ({first}–{last}) |
| `compare.againstPrevious` (new) | Dibanding total {period} | Against the total for {period} |
| `compare.againstAverage` (new) | Dibanding rata-rata {count} bulan ({first}–{last}) | Against the {count}-month average ({first}–{last}) |
| `compare.wholePeriod` (new) | seluruh periode | whole period |
| `compare.perMonth` (new) | per bulan | per month |
| `compare.perCycle` (new) | per siklus | per cycle |
| `compare.runningNote` | Dibanding total periode lalu; tanpa penilaian sampai periodenya selesai. | Compared with the whole last period; no judgment until this one ends. |
| `compare.partial` | belum lengkap | incomplete |
| `compare.partialFrom` (new) | {period} tercatat mulai {date} | {period} recorded from {date} |
| `compare.noBaseline` | Rata-rata muncul setelah 2 bulan lengkap | The average appears after 2 complete months |
| `compare.noBaselineCycle` | Rata-rata muncul setelah 2 siklus lengkap | The average appears after 2 complete cycles |
| `compare.noPrevious` (new) | Belum ada periode pembanding sebelum {period}. | No earlier period to compare with {period}. |
| `compare.same` | sama | same |
| `compare.rose` (new, hidden) | naik | up |
| `compare.fell` (new, hidden) | turun | down |

### `kakeibo`, `daily`, `categories`, `bills`, `report`

| Key | Indonesian | English |
| --- | --- | --- |
| `kakeibo.title` (new) | Kakeibo · per pilar | Kakeibo · by pillar |
| `kakeibo.asideRunning` (new) | siklus ini · porsi · periode lalu | this cycle · share · last period |
| `kakeibo.asideComplete` (new) | jumlah · porsi · vs rata-rata 3 bulan | amount · share · vs 3-month average |
| `kakeibo.asideBasic` (new) | jumlah · porsi | amount · share |
| `kakeibo.needsPillar` | Perlu dipilih | Choose a pillar |
| `daily.title` (changed) | Pengeluaran harian | Daily spending |
| `daily.showTable` | Lihat sebagai tabel | Show as table |
| `daily.showChart` | Lihat sebagai grafik | Show as chart |
| `daily.peak` | Tertinggi: {date} {amount} | Highest: {date} {amount} |
| `daily.today` | Hari ini, masih berjalan: {amount} | Today, still running: {amount} |
| `daily.scope` | Di luar tabungan dan investasi | Excludes savings and investments |
| `daily.chartLabel` (new) | Pengeluaran harian, di luar tabungan dan investasi. Tertinggi {date} {amount}. | Daily spending, excluding savings and investments. Highest {date} {amount}. |
| `daily.tableCaption` (changed) | Pengeluaran per hari | Spending per day |
| `daily.date` | Tanggal | Date |
| `daily.amount` (changed) | Pengeluaran | Spending |
| `categories.title` (changed) | Pengeluaran per kategori | Spending by category |
| `categories.manage` (new) | Kelola kategori | Manage categories |
| `categories.setBudgets` | Atur anggaran | Set budgets |
| `categories.count` (new) | {count} kategori | {count, plural, one {# category} other {# categories}} |
| `categories.ofBudget` (new) | {share}% dari {budget} | {share}% of {budget} |
| `categories.noBudget` (new) | tanpa anggaran | no budget |
| `categories.archived` (new) | diarsipkan | archived |
| `categories.showZero` | Tampilkan {count} kategori tanpa pengeluaran | Show {count, plural, one {# category} other {# categories}} without spending |
| `categories.hideZero` | Sembunyikan {count} kategori tanpa pengeluaran | Hide {count, plural, one {# category} other {# categories}} without spending |
| `bills.title` (changed) | Tagihan & langganan | Bills & subscriptions |
| `bills.aside` (new) | {count} · {amount}/bln | {count} · {amount}/mo |
| `bills.monthlyNext` (new) | bulanan · berikutnya {date} | monthly · next {date} |
| `bills.weeklyNext` (new) | mingguan · berikutnya {date} | weekly · next {date} |
| `bills.seeAll` (new) | Lihat semua | See all |
| `bills.makePlan` | Jadikan rencana | Make it a plan |
| `bills.dialogTitle` | Tagihan & langganan terdeteksi | Detected bills & subscriptions |
| `bills.hasPlan` | Sudah ada rencana: {name} | Already planned: {name} |
| `bills.amountChanged` | Nominal terakhir berubah: {from} → {to} | Latest amount changed: {from} → {to} |
| `bills.empty` (new) | Belum ada tagihan berulang yang terlihat. | No repeating bills seen yet. |
| `report.title` (new) | Laporan bulanan | Monthly report |
| `report.ready` (new) | Laporan {period} siap dibaca | The {period} report is ready |
| `report.about` (new) | Empat pertanyaan Kakeibo, satu per bagian | Four Kakeibo questions, one per section |

### `calendar`, `day`, `upcoming`

| Key | Indonesian | English |
| --- | --- | --- |
| `calendar.heroLabel` (changed) | Pengeluaran periode ini | Spending this period |
| `calendar.dayOf` | Hari ke-{day} dari {total} | Day {day} of {total} |
| `calendar.untilPayday` (new) | {count} hari lagi sampai gaji {date} | {count, plural, one {# day} other {# days}} until payday on {date} |
| `calendar.paydayUnknown` | Tanggal gaji berikutnya belum bisa diperkirakan: {reason}. | The next payday cannot be estimated yet: {reason}. |
| `calendar.gridLabel` (new) | Kalender {period} | Calendar for {period} |
| `calendar.income` | Masuk | Income |
| `calendar.transfer` | Pindah | Transfer |
| `calendar.topUp` | Top up | Top-up |
| `calendar.bill` | Tagihan | Bill |
| `calendar.waiting` | Menunggu jawaban | Waiting for an answer |
| `calendar.heatLegend` (new) | Pengeluaran per hari | Spending per day |
| `calendar.heatFrom` (new) | sampai {amount} | up to {amount} |
| `calendar.heatAbove` (new) | di atas {amount} | above {amount} |
| `calendar.cellName` (new) | {date}. Pengeluaran {amount}.{waiting}{marks} | {date}. Spending {amount}.{waiting}{marks} |
| `day.today` | hari ini | today |
| `day.yesterday` (new) | kemarin | yesterday |
| `day.spentToday` (new) | pengeluaran hari ini | spending today |
| `day.spentThatDay` (new) | pengeluaran hari itu | spending that day |
| `day.waiting` | Menunggu jawaban · {count} | Waiting for an answer · {count} |
| `day.planned` (new) | {name} · {amount} terjadwal | {name} · {amount} scheduled |
| `day.openLedger` | Lihat di Transaksi | View in Transactions |
| `day.future` | Hari ini belum tiba. | This day has not come yet. |
| `day.none` | Tidak ada transaksi. | No transactions. |
| `upcoming.beforePayday` | Sebelum gaji berikutnya | Before the next payday |
| `upcoming.toPeriodEnd` | Sampai akhir periode | To the end of the period |
| `upcoming.unpaid` (new) | Tagihan yang belum dibayar | Unpaid bills |
| `upcoming.estimate` (new) | {date} · perkiraan | {date} · estimate |
| `upcoming.total` | Total | Total |

### `flow`, `flowTable`, `flowDetail`

| Key | Indonesian | English |
| --- | --- | --- |
| `flow.heroLabel` (new) | Aliran uang periode ini · masuk | Money flow this period · income |
| `flow.savedToppedUp` | Ditabung & top up | Saved & topped up |
| `flow.map` (new) | Peta | Map |
| `flow.table` | Tabel | Table |
| `flow.view` | Tampilan | View |
| `flow.highlight` | Sorot aliran | Highlight |
| `flow.all` | Semua | All |
| `flow.spending` (changed) | Pengeluaran | Spending |
| `flow.transfers` | Pindah saldo | Transfers |
| `flow.savings` | Tabungan & investasi | Savings & investments |
| `flow.from` (new) | Dari | From |
| `flow.accounts` (new) | Rekening | Accounts |
| `flow.then` (new) | Lalu ke | Then to |
| `flow.usedOrKept` | Dipakai atau disimpan | Used or kept |
| `flow.caption` (changed) | Lebar pita sebanding dengan jumlah uang. Saldo sebelum periode dan Masih ada ada di tabel. | Band width follows the amount. Opening balances and Still left are in the table. |
| `flow.mapLabel` (new) | Peta aliran uang. Tabel tersedia lewat tombol Tabel. | Money flow map. A table is available with the Table button. |
| `flow.legendTransfer` | Pindah saldo (garis miring) | Transfers (diagonal lines) |
| `flow.legendSavings` (new) | Tabungan & investasi (titik) | Savings & investments (dots) |
| `flow.legendWaiting` (changed) | Menunggu pasangan (arsir) | Waiting for its pair (hatched) |
| `flow.empty` | Belum ada uang yang bergerak. | No money has moved yet. |
| `flowTable.caption` | Semua aliran sampai {date}, termasuk saldo awal dan Masih ada | Every flow up to {date}, including opening balances and Still left |
| `flowTable.fromTo` | Dari → Ke | From → To |
| `flowTable.kind` | Jenis | Type |
| `flowTable.amount` | Jumlah | Amount |
| `flowTable.count` | Transaksi | Transactions |
| `flowTable.waitingPart` | termasuk {count} menunggu jawaban | including {count} waiting for an answer |
| `flowDetail.in` | Masuk | In |
| `flowDetail.out` (changed) | Keluar | Out |
| `flowDetail.opening` | Saldo sebelum periode | Balance before the period |
| `flowDetail.closing` | Saldo di akhir hari | Balance at the end of the day |
| `flowDetail.answer` | Jawab di Tinjau | Answer in Review |
| `flowDetail.allRows` | Lihat semua transaksi rekening ini | View all transactions of this account |
| `flowDetail.more` | Muat lagi | Load more |

"Keluar" in `flowDetail.out` names a direction of one account in the detail, not a spending figure; the copy guard's "Keluar ke" exception does not cover it, so the key is listed in the guard's allowed direction keys (task 3.4).

### `manager`, `form`, `budget`, `states`

| Key | Indonesian | English |
| --- | --- | --- |
| `manager.title` | Kelola kategori | Manage categories |
| `manager.meta` (new) | Tambah, ubah dan arsipkan kategori | Add, edit and archive categories |
| `manager.income` | Pemasukan | Income |
| `manager.archived` | Diarsipkan | Archived |
| `manager.budget` (new) | anggaran {amount} | budget {amount} |
| `manager.edit` (new) | Ubah {name} | Edit {name} |
| `manager.archive` (new) | Arsipkan {name} | Archive {name} |
| `manager.restore` | Pulihkan | Restore |
| `manager.addIncome` (changed) | Tambah kategori pemasukan | Add income category |
| `manager.add` (changed) | Tambah kategori | Add category |
| `form.back` (new) | Kembali | Back |
| `form.name` (changed) | Nama | Name |
| `form.kind` (changed) | Jenis | Type |
| `form.pillar` (changed) | Pilar | Pillar |
| `form.icon` (changed) | Ikon | Icon |
| `form.colour` (changed) | Warna | Color |
| `form.budget` (changed) | Anggaran bulanan | Monthly budget |
| `form.save` (changed) | Simpan | Save |
| `form.create` (changed) | Buat kategori | Create category |
| `budget.title` | Atur anggaran | Set budgets |
| `budget.help` | Batas pengeluaran per kategori setiap bulan. Kosongkan kategori yang tidak perlu dibatasi. | A spending limit per category each month. Leave a category empty for no limit. |
| `budget.none` | Tanpa batas | No limit |
| `budget.save` | Simpan anggaran | Save budgets |
| `states.overviewError` | Ringkasan periode ini belum bisa dimuat. | This period's overview could not be loaded. |
| `states.compareError` | Perbandingan periode belum bisa dimuat. | The period comparison could not be loaded. |
| `states.calendarError` (new) | Kalender belum bisa dimuat. | The calendar could not be loaded. |
| `states.flowError` (new) | Peta aliran belum bisa dimuat. | The money flow could not be loaded. |
| `states.empty` (new) | Belum ada pengeluaran di periode ini. | No spending in this period yet. |

## Appendix B. Analisis glossary (Indonesian → English)

Consistent with `research/2026-11-06-language-system.md` §7 and the `terms` namespace.

| Indonesian | English | Note |
| --- | --- | --- |
| Analisis | Analysis | page title and nav (`nav.analysis`) |
| Ringkasan | Overview | tab |
| Kalender | Calendar | tab |
| Peta Aliran | Money flow | tab; not "Sankey" or "Flow map" |
| Pengeluaran | Spending | the only figure name (`terms.spending`); never "Expenses", "Spent" or "Outflow" |
| Pengeluaran harian | Daily spending | was "Belanja harian" |
| Pengeluaran per kategori | Spending by category | |
| Masuk | Income | `terms.income` |
| Ditabung & investasi | Saved & invested | `terms.savedInvested` |
| Ditabung & top up | Saved & topped up | Peta Aliran only: net into savings plus net into investments |
| Masih ada | Still left | `terms.stillLeft` (new) |
| Menunggu pasangan | Waiting for its pair | `terms.waitingForPair` (new) |
| Menunggu jawaban | Waiting for an answer | |
| Pindah saldo | Transfer(s) | `terms.transfer`; never "internal movement" |
| Siklus gaji | Pay cycle | `terms.payCycle` |
| Bulan kalender | Calendar month | `terms.calendarMonth` (phase 5) |
| rata-rata 3 bulan | 3-month average | `terms.threeMonthAverage` (new) |
| seluruh periode | whole period | |
| sejauh ini | so far | |
| Kebutuhan, Keinginan, Budaya, Tak Terduga, Tabungan | Needs, Wants, Culture, Unexpected, Savings | `terms.*` |
| Belum diklasifikasi | No pillar yet | `terms.noPillarYet` |
| Perlu dipilih | Choose a pillar | an action, not a status |
| Tagihan & langganan | Bills & subscriptions | |
| Jadikan rencana | Make it a plan | |
| Kelola kategori | Manage categories | |
| Atur anggaran | Set budgets | |
| tanpa anggaran | no budget | |
| Rincian | Breakdown | `terms.breakdown` |
| Laporan Bulanan | monthly report | sentence case in English |
| Tertinggi | Highest | chart legend |
| Hari ke-{n} dari {m} | Day {n} of {m} | |
| Sorot aliran | Highlight | |
| Dari · Rekening · Lalu ke · Dipakai atau disimpan | From · Accounts · Then to · Used or kept | map column heads |
