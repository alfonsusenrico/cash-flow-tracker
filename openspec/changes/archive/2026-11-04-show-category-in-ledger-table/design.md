## Context

`LedgerTable` renders the columns Transaksi, Jam, Jenis, Kategori (the `rowStatus` pillar text), Dicatat (`provenanceText`) and Jumlah. Rows already carry `category_name`.

## Decisions

- **D1: category cell.** The category cell reads:
  - for an expense or income, the row's `category_name`, or "Tanpa kategori" when it has none;
  - for a movement, an investment top-up, or a row filed as `Internal Movement`, "—", because the Jenis pill and the pillar text already say what it is;
  - for a row waiting for an answer that has no category yet, "—", because the pillar column already says "Menunggu jawaban".
  Owner category names are shown as written. Rejected: showing "Internal Movement" for movements, which is an English system name.
- **D2: pillar heading.** The pillar column is headed "Pilar", the term Laporan uses ("Pengeluaran per pilar dan kategori"). Rejected: merging the pillar into the category cell as a coloured dot only, because then the pillar would rely on colour alone.
- **D3: provenance.** It is removed from the table. The detail already shows "Dicatat" and "Diklasifikasikan", and the "Dari HP" and "Lewat Catat" filters still find rows by source.

- **D4: day heading colours.** Pengeluaran uses `--out-fg` (`ws-amt-out`), Masuk keeps `--in-fg` and Pindah saldo uses `--tr-fg` (`ws-amt-tr`). The token test already checks these against every surface in both themes.
- **D5: chart amounts.** The chart's native `title` is replaced by a small label above the bar. A mouse shows it on hover. Touch and pen toggle it on tap. Leaving the chart or pressing Esc hides it, which meets WCAG 1.4.13 (dismissible, hoverable, persistent). The chart stays `aria-hidden` because the "Lihat sebagai tabel" table carries the same values for keyboard and screen-reader users. The label at the first and last bar is aligned to that edge, so it never leaves the card.

- **D6: narrow tables.** When the main column is 940 px or narrower, Pilar is hidden first and Kategori stays until 860 px, because the owner finds the category more useful. Provenance had held the first slot.

## Risks

- Long category names: the cell truncates with an ellipsis and keeps the full name in `title`, so the table never overflows.
