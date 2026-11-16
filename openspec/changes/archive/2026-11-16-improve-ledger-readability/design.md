# Design: improve-ledger-readability

## Context

The owner reviewed Transaksi in production on 2026-11-16 (dark theme, 1280 px window) and found four problems: names cut short, dots on some rows only, a redundant "Investasi" tab whose hero is always Rp 0, and the cycle label "Siklus sejak 30 Okt · berlanjut".

What the code does today:
- `ListRow` (`components/workspace/lists.tsx`) puts the title, the note (`.r-note`, with a file icon) and the tags, then the meta (account or route), on one baseline in `.r-main`, between a 40 px time and the fixed 168 px category, 136 px pillar and 120 px amount columns. On the phone `.r-main` is already a column.
- `rowStatus` (`components/transactions/rows.tsx`) gives a dot only to an expense's pillar; income, moves and waiting rows are words only, and a move that changes no saving figure reads "—".
- `type=investment` lists top-up legs and Investasi rows; `type=saving` lists rows whose `saving_flow` changes "Ditabung & investasi". A top-up from RDN (`invested`) is in both; a top-up funded from a savings pocket (`null`) is only in Investasi. `LedgerHero` shows Pengeluaran, Masuk and Bersih for every type except Tabungan & investasi.
- Cycles open and close only on confirmed salary receipts (`observed_periods.py`, `report_windows.py`). The running cycle is labelled "Siklus sejak {start}" (`cycle_since`, `cycle_ongoing` on the server, `cycleSince` on the web). `expected_next_salary` (salary-cycles, "Expected Next Salary Date") estimates the next salary from three regular receipts; the owner has two, so it is unknown. `users.payday_day` is the owner's payday setting (29 in production).

## Decisions

### 1. Two-line transaction rows on a desktop (owner, 2026-11-16)

A transaction row with any of meta, note or tags renders `.r-main` as two lines on every width: the title alone on line 1, then a muted 12 px line 2 with the meta, the note (file icon and text) and the tags (tag icon and names), separated by " · ", left-aligned under the title. Line 1 ellipsizes only when the name is longer than the whole title column; line 2 ellipsizes as one line, the tags kept readable before the note, as today. A row without extras stays one line. The row's minimum height stays 56 px; only rows with a second line grow, to 60 px at most.

The category and the pillar share one 168 px cell, the pillar under the category (owner, 2026-11-16, after the browser check showed a 200 px name column at 1280 px); the name gains the pillar column's width. This applies through `LedgerList`'s rows (a `layout="stacked"` option of `ListRow`), so Pengaturan, Tinjau and the account dialogs keep their lists.

Rejected: icons with counts in one line (owner chose two lines); wrapping the title (rows of different heights for the same content).

### 2. A dot on every row (owner, 2026-11-16)

`rowStatus` returns a colour for every row:
- expense: the pillar's colour, as today;
- income: the income colour (mint), "Uang masuk";
- move: the transfer colour (sky), with its saving direction ("Ditabung", "Diambil dari tabungan", "Bayar utang") or "Pindah saldo" instead of "—";
- top-up or investment row: the investment colour (lavender), "Investasi" or "Pindah saldo" when it changes no saving figure;
- waiting: the warning colour (amber), "Menunggu jawaban".

The dot stays decorative (`aria-hidden`); the word carries the meaning.

### 3. One "Tabungan & investasi" type

"Investasi" leaves `LEDGER_TYPES`, the rail and the type control. `type=saving` lists rows whose `saving_flow` changes the figure **or** that are top-up legs or Investasi rows, so a top-up funded from savings is still findable; its pillar column reads "Pindah saldo" to say it does not change the figure. `saving_net` keeps its rule and stays equal to Beranda. `type_counts.saving` follows the new condition; `type=investment` and `type_counts.investment` stay for API consumers. A URL with `type=investment` is read as `saving`.

Rejected: keeping "Investasi" with a hero of its own (the owner found it redundant; it held 2 rows); dropping savings-funded top-ups from every type but Semua.

### 4. A hero per type

- Semua, Pengeluaran, Kredit: unchanged.
- Masuk: one hero "Masuk" with `inflow`; Pengeluaran and Bersih hidden.
- Pindah saldo: one hero "Pindah saldo" with a new summary field `moved`: the sum of the matching movements, each counted once, by the rule of the day totals' `movements`. Over all pages it equals the sum of the day totals.
- Tabungan & investasi: unchanged (`saving_net`).

Rejected: computing the moved total in the browser from loaded rows (wrong while pages are missing).

### 5. Cycle labels as a date range (owner, 2026-11-16)

Every salary-cycle label is "{start} – {end}" in the interface language, without the "Siklus" prefix ("30 Okt – 29 Nov", "30 Oct – 29 Nov"). Cycles read payday to payday (owner, 2026-11-16, after a correction: a cycle closes at the moment its next salary arrives, so payday-morning spending belongs to it):
- a finished cycle: the day its next salary arrived, as today's `cycle_range`;
- the running cycle: `max(today, expected next salary day)`.

The expected next salary is the existing regular-receipts estimate when it is known; otherwise the payday setting: the payday-of-month date nearest the cycle's start (in its month or a neighbouring month), moved one month on and clamped to that month's length. Start 30 Okt with payday 29 gives 29 Nov, so the label reads "30 Okt – 29 Nov". On 30 Nov, with the salary not yet recorded, it reads "30 Okt – 30 Nov". When a salary is recorded on 27 Nov, that cycle is finished, "30 Okt – 27 Nov", and the next reads "27 Nov – 29 Des".

The period payloads that describe the running cycle (`summary.cycle` of the ledger, Analisis and Laporan periods, Beranda's cycle) gain `label_end_date`, and the server's `label` uses it. The web formats the same range from `start` and `label_end_date`. Report windows, figures and boundaries are unchanged: the running cycle still runs to now ("Expected Date Is Never a Boundary").

Rejected: today as the end (owner chose the estimate); the day before the expected payday (payday-morning spending would fall outside the printed range of its own cycle).

## Risks

- A savings-funded top-up under "Tabungan & investasi" does not change its hero. Its pillar column says "Pindah saldo", so the list and the figure stay explainable.
- The payday setting can be wrong; the estimate is a label only, and today keeps the end honest when the salary is late.
- Two-line rows make a busy day taller. Only rows with extras grow, by at most 4 px.

## Verification

- Backend: `type=saving` lists a savings-funded top-up while `saving_net` is unchanged; `type_counts.saving`; `moved` equals the sum of the day totals' `movements`; labels for the running cycle (setting, regular receipts, late salary, early salary), in both languages; report windows unchanged.
- Web: row tests for one and two lines and the dot on every kind; hero per type; rail without Investasi and the old link; stepper and Analisis labels.
- Browser: Transaksi at 1440, 1280 and 390, light and dark, with long names, notes and tags; axe; no horizontal scroll at 1024.
