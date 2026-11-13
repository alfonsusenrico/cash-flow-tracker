## Context

`BerandaHero` renders the 44 px "Aman dipakai sampai gajian", a split bar and four `FigureBlock`s from `GET /dashboard/cycle-summary`. Transaksi reads its filters from the URL (`period=cycle|month`, `type=income|expense|saving|credit`, `account`) and its hero shows the same Pengeluaran, Masuk and, for `type=saving`, Ditabung & investasi as Beranda for the same period. Target & Tagihan lists bills under `#tg-bills`.

Credit facts already exist: `GET /accounts` carries `credit_owed`, `limit_left`, `over_limit` and `next_due` for credit accounts; `GET /credit/accounts/{id}` returns bills with their items (each with `transaction_id` and `date`); `GET /transactions?account_id=&from_date=` lists an account's rows with `credit` facts (`role`, `settled_on`, `next_due_date`).

`rowTitle` returns `merchant || notes || category || undescribed` for non-movement rows and is shared by Transaksi, Beranda, the calendar, account detail and the transaction dialog.

## Decisions

1. **Figures are links, not buttons.** `FigureBlock` takes `href` and an accessible name ("Lihat Pengeluaran di Transaksi"), and the whole block becomes one Next link with a visible focus ring and a hover state. A link says where it goes and opens in a new tab on request. Rejected: buttons opening dialogs, which would duplicate Transaksi.
2. **Targets.** `period` is `cycle` when the summary's basis is the cycle, else `month`. Masuk → `/ledger?period=…&type=income`, Pengeluaran → `type=expense`, Ditabung & investasi → `type=saving`, Terjadwal → `/goals#tg-bills`. Transaksi's hero already shows the matching figure; the check in task 3.2 compares them.
3. **The credit figure is a button.** It opens a dialog, so it is a button with `aria-haspopup="dialog"`, shown only when the owner has an active credit account. Figure: the sum of `credit_owed`. Caption: the nearest `next_due` across accounts as "jatuh tempo {date} · {left}", or "tidak ada tagihan" when nothing is owed, or "tanggal jatuh tempo belum diisi" when owed without due dates. It takes no part of the split bar and changes no other figure. Its colour mark is `--credit-fg`, the amber of unpaid credit.
4. **Where it sits.** Fifth in the legend after Terjadwal. The legend becomes five columns on desktop; below the width where five captions fit on one line it wraps to rows, which the browser check measures. On the phone it shows with Pengeluaran and Ditabung (Masuk and Terjadwal stay phone-hidden).
5. **Dialog content per account.** Sections sorted by owed, highest first, then by name. Each: name, Utang, "Sisa limit" or "Melewati limit", next bill. Its rows: `GET /transactions?account_id={id}&from_date={start}&limit=200`, where `start` is the earliest item date among bills with something left to pay (from `GET /credit/accounts/{id}`), shown oldest first with the shared row parts (title, meta with the credit line, amount in credit amber while unpaid). Payments into the account are its inbound movement rows. With nothing owed it says "Tidak ada tagihan berjalan". Each section ends with "Lihat semua di Transaksi" → `/ledger?account={id}`. Each section loads on its own; a failure names itself with "Coba lagi" and the others stay usable. Rejected: the account's whole history, which grows without bound.
6. **Row click in the dialog** opens the shared transaction dialog, the same as "Aktivitas terbaru", where a payment shows what it paid.
7. **Title rule.** `rowTitle` for a non-movement row returns the owner's note, else the merchant, else a generic note, else the category, else undescribed. A generic note is one a notification writes when it knows nothing better (`notes.transaction`, `notes.generic_*`, `notes.sender` in either language, `GENERIC_ROW_NOTES`, checked against the backend catalogues by a test), so "Pengeluaran" never hides a merchant. `rowMeta` prefixes the merchant when the title is the note, the row has a merchant, and the note does not already contain it (case-insensitive). The edit dialog's subtitle uses `notes || merchant`. Movements and top-ups are unchanged.

## Failure behaviour

Accounts or bills that fail to load leave the other figures unchanged; the credit figure then shows "—" with no caption and stays a button whose dialog offers "Coba lagi".

## Risks

Five captions at 1024 px with the full sidebar may not fit on one line; decision 4 wraps the legend there instead of truncating. A note that is long now becomes the title; rows already truncate their title with an ellipsis and keep the full text in the dialog.

## Rollback

Revert the commits. No data or API change.
