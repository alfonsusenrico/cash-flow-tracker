# Verification: show-category-in-ledger-table

**Revision under test:** the working tree on `feat/ledger-category-column`, based on `main` `847df57`. The commit holding this record contains exactly the tested tree.

## Automated checks

| Check | Result |
| --- | --- |
| `npm run lint` | exit 0 |
| `npm run type-check` | exit 0 |
| Vitest | 73 files, 663 passed, 0 unhandled errors |
| `npm run build` | compiled successfully |
| `openspec validate --all --strict` | exit 0 |
| `git diff --check` | clean |

**New tests:**
- **Ledger page** ("shows pillar and category columns instead of provenance, and colours each day figure by its kind"): column headings, a category cell with `title`, no provenance text, "—" for a waiting row, no "Internal Movement", and the red and blue day-figure classes. It fails on the old components.
- **`rowCategory`:** expense, income, uncategorised, movement, unlinked movement leg, top-up and waiting rows.
- **Belanja harian:** mouse hover shows the label and a mouse leave hides it. A touch tap shows it and keeps it after the lifted finger "leaves". A second tap, a tap outside or Esc hides it. The touch-leave case fails on the first implementation, which the browser check caught.

## Browser check

**Stack:**
- Local only: Postgres 16 restored from the synthetic seed of `show-movement-routes-and-centered-dialogs` and migrated to V45, the API on 8292, and `next start` behind the loopback proxy on 3292.
- Headless Brave on 9592.
- Synthetic owner `raka_contoh`.

| Cell | Table headings | Overflow | axe serious/critical | Console errors | Hover/tap label |
| --- | --- | --- | --- | --- | --- |
| 1440 light | Transaksi, Jam, Jenis, Pilar, Kategori, Jumlah | 0 | 0 | 0 | "30 Okt Rp 891.500", inside the card, gone after Esc |
| 1440 dark | same | 0 | 0 | 0 | same |
| 1024 light | Transaksi, Jam, Jenis, Kategori, Jumlah (Pilar hidden first, D6) | 0 | 0 | 0 | same |
| 390 dark | phone feed (no table) | 0 | 0 | 0 | same, by a touch tap |

**Day-figure colours (computed):**
- **Light:** Pengeluaran rgb(184, 20, 45), Masuk rgb(5, 101, 72), Pindah saldo rgb(7, 89, 133).
- **Dark:** rgb(255, 82, 99), rgb(16, 185, 129) and rgb(56, 189, 248).
- The token test already checks these foregrounds against every surface.

**Faults found and fixed during the check:**
- The generic red and blue classes lost to `.ws-tx-daytot b`. The day totals now have their own rules.
- A lifted finger cleared the label on touch. Only a mouse leave clears it now.
- The tallest bar's label covered the card heading. Its offset is capped inside the plot.

**Screenshots:** in the session scratchpad, `category/shots/`: `transaksi-*` and `belanja-*` for each cell, plus `report.json`.

## Not verified

- A screen reader. The chart stays `aria-hidden`, and its table carries the values.
- A real phone.
