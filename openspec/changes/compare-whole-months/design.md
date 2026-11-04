# Design: compare-whole-months

## Context

- **Report service:** `services/monthly_report.py` builds each period's report from the ledger rules. That covers the period list (calendar months and closed salary cycles, `complete`, `tracking_started_inside`), the Kakeibo breakdown, income sources, unexpected items, recurring charges and data quality.
- **Comparisons today:** they come from `observed_insights.period_comparison`, which compares the same elapsed days. On top of that sits `_median`, the median of three periods over their first `D` days.
- **Ringkasan:** Analisis Ringkasan uses the same same-day comparison for its Kakeibo marks.
- **Companion:** it reads only `GET /api/reports/monthly/latest`.
- **Approved target:** the preview at `cash-flow-tracker-prototypes/2026-11-04-workspace-refinement/` (Laporan, Analisis Ringkasan, and the "Perubahan logika" section of `compare.html`). Its build checks October = Rp 4.802.000 and September = Rp 3.218.500 against the seeded ledger.
- **Dependency:** this change uses the spending terms and the additive `spending` figure of `refine-workspace-layout`, which ships first.

## Goals

- Every comparison uses whole periods, with a baseline of the owner's usual months.
- Every figure equals the ledger summary of its window. The client computes no money value.
- A running period never receives a verdict.

## Non-goals

- Budgets, forecasts and verdict words.
- AI-written text.
- The Beranda and other pages that `refine-workspace-layout` owns.

## Decisions

### D1. Periods: completed, running, partial

- **Calendar basis:** a period is **completed** when its last local day is before today in the application timezone.
- **Salary-cycle basis:** a period is completed when it is a closed cycle in the existing period list.
- **Running:** the period that contains today.
- **Partial:** a completed period in which tracking began after its first day (`tracking_started_inside`).
- **The month view:** the "month" a page shows follows its basis switch (Bulan kalender or Siklus gaji), as today.

### D2. Baseline: average of the three completed full periods before

- **Window:** the baseline of period P is the average of the three most recent completed, non-partial periods before P, on the same basis. Oktober uses Jul–Sep, and a finished November uses Agu–Okt. P is never part of its own baseline.
- **Rounding:** averages are integer rupiah, rounded half up. A change equals P's figure minus the baseline.
- **Fewer periods:**

  | Usable periods | What is shown |
  | --- | --- |
  | 3 | "rata-rata 3 bulan" |
  | 2 | "rata-rata 2 bulan" (OD89) |
  | 1 or 0 | no baseline, and the page says "Rata-rata muncul setelah 2 bulan lengkap". Never Rp 0 |

- **What gets a baseline:** each figure, each pillar and each category. A category absent from a baseline period counts as Rp 0 in that period.
- *Rejected:* the median. With three values it is simply the middle month. The owner's example ("rata-rata") and the approved preview use the mean.
- *Rejected:* the same first days. The owner rejected it.

### D3. Previous whole period

- **The comparison:** a completed period is compared with the previous whole period on the same basis.
- **Partial previous period:** it still appears as the previous period with "tercatat sejak {date}", but its ▲/▼ verdict is replaced by "belum lengkap" (OD90).
- **No previous period:** no comparison is shown.

### D4. Running period: progress only

For the running period the page shows:
- Pengeluaran so far;
- its share of the previous whole period's Pengeluaran, as a bar and "{n}% dari total {label}";
- the same per pillar.

It shows no ▲/▼ and no words such as "naik", "turun", "lebih" or "kurang". With no previous whole period, only the amount so far is shown.

- *Rejected:* a pace projection. It would be a guess about the rest of the month.

### D5. Figures

| Figure | Definition | Source |
| --- | --- | --- |
| Pengeluaran | need, want, culture, unexpected and unclassified, net of applied reimbursements | the report's Kakeibo breakdown without the saving pillar, equal to the ledger summary's `spending` |
| Masuk | reportable inflow | report `income.total` |
| Ditabung & investasi | fresh savings: saving-classified expenses plus net saving movements | report `savings.total` |
| Selisih | Masuk − Pengeluaran | computed on the server |
| Pillar and category amounts | as the report's spending breakdown, excluding the saving pillar | report breakdown |

- **Shares** are a figure's share of Pengeluaran in whole percent. That is a share, not a change, so "no percentage change" still holds.
- **Changes** are amounts, never percentages.

### D6. "Yang bisa diperbaiki": deterministic facts

Each fact has a kind, a short neutral label, an amount and a ledger reference (`from`, `to` and the filter). The list holds at most seven facts, in this order:
1. **Category above its own baseline:** the categories whose amount exceeds their baseline by at least Rp 50.000 and at least 10% of the baseline, top three by amount above. A category with a Rp 0 baseline qualifies from Rp 100.000 and is labelled "baru" (OD91). Shown only when a baseline exists.
2. **Keinginan share:** this period's share of want spending against the previous whole period's share, in percentage points. Shown only with a non-partial previous period.
3. **Top merchant:** the merchant with the largest spending in the period.
4. **Recurring bills:** the count and monthly-equivalent total of the detected recurring charges seen in the period, using the existing rule.
5. **Unexpected spending:** the count and total of "Tak Terduga".

- **Running period:** facts 3–5 only, titled "Sejauh ini", with no baseline-based facts (OD88).
- *Rejected:* model-written advice (project LLM boundary) and verdict words.

### D7. API

**`GET /api/reports/monthly/comparison?basis=month|cycle&start_date=YYYY-MM-DD`:**
- session or Bearer authentication, owner-scoped;
- 404 `period_not_found` and 409 `cycle_unavailable` as the report does.

Returns:

```
{ "period": {label, start, end, complete, partial},
  "totals": {spending, inflow, fresh_saving, difference},
  "previous": {label, start, complete, partial, totals, change: {spending, inflow, fresh_saving, difference}} | null,
  "baseline": {period_count, labels: [...], totals, change} | null,
  "progress": {previous_spending, spending, share} | null,
  "pillars": [{pillar, amount, share, previous, change, baseline, baseline_change,
               categories: [{category_id, name, amount, share, previous, change, baseline, baseline_change}]}],
  "facts": [{kind, label, amount, ref: {from, to, ...filters}}],
  "history": [{label, start, complete, partial, spending, pillars: {need, want, culture, unexpected, unclassified}}] }
```

- `progress` is set only for the running period. `change` and `baseline_change` are `null` for the running period and wherever D2 or D3 withholds them.
- `GET /api/reports/monthly` returns the same object as `comparison`. It replaces the old `comparison.previous` and `comparison.median`, whose only consumer is the web.
- `GET /api/reports/monthly/latest` is unchanged.

**Code:** `services/monthly_comparison.py` builds one window from the earliest history or baseline period to P's end. It computes per-period pillar, category, merchant and inflow totals in grouped queries, so the statement count does not grow with the number of rows or periods. It reuses the report's reportable-flow and Kakeibo rules, and no figure uses a rule the ledger summary does not use. `_median` and the report's call to `period_comparison` are removed.

- *Rejected:* computing from five full `build_report` calls. That is too many statements, because each report runs its own data-quality and budget queries.

### D8. Laporan page

**Top bar:** the period stepper and the basis switch.

**Body, in order:**
- "Kualitas data" as one line with its links, green when nothing is open;
- KPI cards: Pengeluaran, Masuk, Ditabung & investasi and Selisih, each with "vs {previous}" and "vs rata-rata 3 bulan" chips. On a running period: progress only;
- **left:** "Pengeluaran per pilar dan kategori", with pillar rows (total, share, change) followed by their categories sorted by amount;
- **right:**
  - "Yang bisa diperbaiki";
  - "Pengeluaran per bulan", stacked bars of `history` with the running month marked "berjalan" and the line "Bulan berjalan tidak dibandingkan sampai bulannya selesai";
  - "Tempat belanja terbesar".

**Kept, compacted:**
- "Laporan sebelumnya";
- budget, only when one exists;
- the savings detail;
- the income sources.

**Removed:**
- the written lead;
- the four numbered question headings. The figures still answer them.

**Reflection:** "Apa yang mau diperbaiki bulan depan?" appears only when a note exists for this period or the previous one, read-only with "Ubah" (OD94). Its API and data are unchanged.

**Basis:** `basis` and `start` stay in the URL.

### D9. Analisis Ringkasan

**Order:**
- KPI row: Masuk, Pengeluaran, Ditabung & investasi;
- the running period's progress against the previous period, in total and per pillar (D4);
- "{last completed} vs rata-rata 3 bulan", with pillar rows and ▲/▼ changes;
- the daily chart;
- compact cards: "Tagihan & langganan terdeteksi" (count and monthly total), "Seberapa sering ditanya" (this week's share), "Kebiasaan dihafal" (ready count). Each puts its headline first, with details behind "Rincian" and no method paragraphs;
- "Kategori & anggaran", unchanged.

**Removed:** the same-day Kakeibo marks and the comparison's increase and decrease card.

### D10. The old comparison endpoint

`GET /api/dashboard/period-comparison` and its spec stay unchanged so that any Bearer consumer keeps working. The web stops calling it, and its spec says the web does not present it. Removing it is a later change (OD92).

- *Rejected:* removing it now. It is a documented authenticated endpoint, and no migration window was planned.

### D11. Direction colours

- **Pengeluaran and pillar rows:** a rise is the expense hue with ▲ and a fall the income hue with ▼.
- **Masuk, Ditabung & investasi and Selisih:** the opposite.
- **Always:** the sign and the arrow carry the meaning, never colour alone.

### D12. Verification gates

1. **Ledger equality:** for every period in `history`, and for the previous and baseline periods, the API's Pengeluaran, Masuk and pillar and category amounts equal `GET /api/transactions` summaries for the same window and filters. Tested over 6 months of the preview seed and the phase seeds.
2. **Window tests:**
   - Oktober's baseline equals the mean of Jul–Sep;
   - the running month has no `change`;
   - a partial previous month gives "belum lengkap";
   - with 2 usable periods the response says "rata-rata 2 bulan";
   - with 1 or 0 there is no baseline;
   - salary-cycle basis with a closed and an open cycle;
   - a category new this month;
   - the month boundary at 23.30 local time.
3. **Facts:** each fact's amount equals its ledger reference's summary, and thresholds are tested at the edge (Rp 49.999 and Rp 50.000).
4. **Owner scope and 401** for the new route.
5. **Speed:** the comparison median is under 150 ms on the preview seed with 6 months, the statement count is constant for 1 and 200 rows per period, and `GET /api/reports/monthly` is no slower than the baseline plus 10%.
6. **Preview match:** Laporan (complete month and running month) and Analisis Ringkasan at 1440 dark, 1440 light and 390 dark on the preview seed, compared with the approved preview images.
7. **Full matrix and zoom** on `/laporan` and `/insights?tab=ringkasan`, with axe serious 0, overflow 0 and console 0.
8. **Clean-up evidence:** `ps`, `lsof` and `docker ps`.

## Implementation notes

- **Per-period figures** are computed with the existing rule functions (window figures, the Kakeibo breakdown, and one grouped query per period for pillar and category), not one window query: the statement count is constant in rows and grows only with the at most six periods involved, and no rule is rewritten (risk "rule drift"). The speed gates hold.
- **Category rows** are grouped by effective pillar and category, so a category whose rows carry two pillars appears under both, each net of that pair's reimbursement share.
- **Category facts** carry `amount` (the period's amount, equal to the ledger) and `difference` (the amount above the average, shown on the page).
- **The recurring fact** has no ledger reference, because a monthly-equivalent total is not a ledger sum; it links to the detected bills on Ringkasan.
- **The report reuses** its own recurring charges and shops in the comparison.
- **Laporan's stepper** sends the running month's date: without `start` the API opens the latest closed period, so the running month was unreachable.
- **Below 900 px** Laporan renders one column in reading order (the facts right after the figures), so the focus order matches what is seen.
- **Ringkasan** puts the progress and average cards side by side and the three compact cards in one row; `PeriodComparison`, `CashFlowHistory` and the same-day Kakeibo card are removed with their types.

## Compatibility, migration and rollback

- **The report's `comparison` object changes shape.** Its only consumer is the web, and both ship together.
- **New route:** additive.
- **Unchanged:** `/reports/monthly/latest` and `/dashboard/period-comparison`.
- **Database:** no migration.
- **Rollback:** revert the merge.

## Risks

- [Rule drift between the new grouped queries and the report] Gate 1 compares every figure with the ledger summary, period by period.
- [A low previous month makes "vs September" look alarming] The baseline chip sits beside it. The owner's own example motivates showing both.
- [New users see little] D2 says in words when the baseline appears. Nothing shows Rp 0 as a baseline.

## Open decisions (recommended defaults)

| # | Decision | Default |
| --- | --- | --- |
| OD88 (resolved 2026-11-04: "approved, defaults ok") | "Yang bisa diperbaiki" on the running month | **Facts-only "Sejauh ini"**: top merchant, recurring bills seen and unexpected spending, without baseline facts or verdicts |
| OD89 (resolved 2026-11-04: "approved, defaults ok") | Baseline with fewer than 3 usable periods | **Show the average of 2, labelled "rata-rata 2 bulan"**; none with 1 or 0 |
| OD90 (resolved 2026-11-04: "approved, defaults ok") | Partial previous month | **Shown with "tercatat sejak {date}" and "belum lengkap" instead of ▲/▼**; partial months never enter the baseline |
| OD91 (resolved 2026-11-04: "approved, defaults ok") | "Above average" threshold | **At least Rp 50.000 and 10% above the baseline, top three**; a new category from Rp 100.000 |
| OD92 (resolved 2026-11-04: "approved, defaults ok") | `GET /api/dashboard/period-comparison` | **Keep unchanged and unused by the web**; remove in a later change |
| OD93 (resolved 2026-11-04: "approved, defaults ok") | History chart length | **Last four completed periods plus the running one** |
| OD94 (resolved 2026-11-04: "approved, defaults ok") | Reflection block | **Hidden unless a note exists for this or the previous period**, then read-only with "Ubah" |
| OD95 (resolved 2026-11-04: "approved, defaults ok") | Direction colours | **D11**: spending up red ▲, down green ▼; income and saving the opposite |
| OD110 (open, conservative default applied) | Ledger summary for a filter of both category and pillar | **Net of that pair's reimbursement share**, as the other report-shaped filters are; it was gross. Needed so every pillar and category row of the report equals its ledger link |
| OD111 (open, conservative default applied) | Link of the recurring-bills fact | **Detected bills on Ringkasan**, because the monthly equivalent is not a ledger sum |
