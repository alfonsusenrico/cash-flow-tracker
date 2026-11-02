# Design

## Context

The owner approved the three follow-ups of `align-investment-and-info-colours` on 2026-11-02. Work happens on `fix/colour-followups`, branched from origin/main 7d9f935.

Live tokens: light `--surface` `#FFFFFF`, `--bg` `#E8ECEF`, `--surface-raised` `#D8DFE5`; dark `#0E1118`, `#050608`, `#181E2A`. Reserved meanings: transfer sky (`#0EA5E9`/`#38BDF8`, hue 199/198), investment lavender (`#8B5CF6`/`#A78BFA`, 258/255), income emerald (161/160), expense coral (353/355).

## Decisions

### D1. The home feed reuses the ledger's top-up marker

The ledger already returns `investment_topup_id` from `LEFT JOIN investment_topups topup ON topup.id = t.movement_id AND topup.user_id = t.user_id` (`routers/transactions.py`). The `recent_transactions` query of `/api/dashboard/overview` gets the same join and field, with the same meaning: the top-up id on both legs, `null` otherwise.

- One-to-one: a movement id belongs to at most one top-up, so the join adds no rows. The `LIMIT 10` and the order are unchanged.
- No extra query per row: the marker comes from the same statement. A test counts the statements the dashboard router sends and finds the same number with one top-up and with four.
- Owner scope: the join also matches `topup.user_id`, as in the ledger.

Rejected:
- `movement_kind: "investment_topup"`, which the ledger also returns. It is derived from the same id, and the brief asked for one field. The web only reads `investment_topup_id`.
- A second query that looks up top-ups for the ten ids. Same result, one more statement.

### D2. Home feed rows

Beranda (desktop) and `MobileHomeView` (phone width) compute `isTopup = Boolean(tx.investment_topup_id)` before the existing `isMovement` branch:

| Element | Top-up | Transfer (unchanged) |
| --- | --- | --- |
| Icon tile | `bg-violet-500/10 text-investment` (phone: `/15` and `border-violet-500/20`) | sky |
| Amount | `text-investment` | `text-sky-500` |
| Title without a note | "Top up Investasi" | "Pindah Saldo" |
| Detail line | "Top up Investasi" when the row shows a different note | — |
| Detail dialog | "Top up Investasi" title fallback, lavender amount, "Jenis Transaksi: Top up Investasi" | sky, "Pindah Saldo (Masuk/Keluar)" |

The API stores "Top up Investasi" as the note of a top-up recorded without one, so the detail line appears only for other notes. The name makes the meaning independent of colour.

### D3. Default colour of a new account

`app/services/account_colours.py` holds `DEFAULT_ACCOUNT_COLOR = "#3b82f6"`, `INVESTMENT_ACCOUNT_COLOR = "#8B5CF6"` (the light `--color-investment`) and `default_account_color(type)`.

- `AccountCreate.color` now defaults to `None`, and the insert uses `payload.color or default_account_color(payload.type)`. An investment pocket, created without a colour by the web, therefore becomes lavender too.
- The broker notification path that opens a stock position sets `INVESTMENT_ACCOUNT_COLOR`; it used the column default before.
- No migration. The `accounts.color` column default stays `#3b82f6`; every API insert now names a colour. Stored colours are never rewritten, and updates do not touch a colour they are not given.
- Web form: `lib/accountColours.ts` mirrors the defaults for the form (`#2563EB` Biru, the form's existing start colour, and `#8B5CF6`). The tag palette gains a "Lavender" swatch so the default is visible and selectable. A new account's colour follows the type until the owner picks a swatch. Editing never changes the colour by itself.

Rejected:
- Storing `var(--color-investment)` so the colour follows the theme. The column holds hex strings, the tag text colour is computed from the hex, and other clients read the value.
- Replacing the "Ungu" swatch (`#7C3AED`) with lavender. Accounts saved with it would lose their selected swatch in the edit form.

### D4. Kakeibo pillar hues

Each pillar keeps one token, `--color-kakeibo-<pillar>`, used as the chart mark. It is dark enough in the light theme and light enough in the dark theme to also pass as text, so no separate text ink is needed. Hues sit in the gaps between the reserved colours: at least 25 degrees from each reserved fill and text ink, and at least 40 degrees from each other.

| Pillar | Theme | Colour | Hue | `--surface` | `--bg` | `--surface-raised` | Soft fill | `--border` track |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Kebutuhan (need), fjord blue | light | `#2448C4` | 227 | 7.50 | 6.31 | 5.58 | 6.71 | 6.19 |
| Keinginan (want), plum | light | `#A21C7E` | 316 | 6.99 | 5.88 | 5.20 | 6.40 | 5.77 |
| Budaya (culture), olive | light | `#56640F` | 70 | 6.51 | 5.48 | 4.84 | 6.09 | 5.37 |
| Tak Terduga (unexpected), terracotta | light | `#9E440D` | 23 | 6.39 | 5.38 | 4.75 | 5.79 | 5.27 |
| Tabungan (saving), forest green | light | `#296B38` | 134 | 6.45 | 5.43 | 4.80 | 5.90 | 5.32 |
| Belum diklasifikasi, slate (unchanged) | light | `#475569` | 215 | 7.58 | 6.38 | 5.63 | 6.92 | 6.25 |
| Kebutuhan | dark | `#8099F5` | 227 | 7.01 | 7.52 | 6.19 | 5.51 | 5.76 |
| Keinginan | dark | `#E879C6` | 318 | 7.18 | 7.70 | 6.34 | 5.68 | 5.90 |
| Budaya | dark | `#B5C350` | 67 | 9.79 | 10.51 | 8.65 | 7.27 | 8.05 |
| Tak Terduga | dark | `#F0915A` | 22 | 8.01 | 8.59 | 7.08 | 6.22 | 6.58 |
| Tabungan | dark | `#7AC68A` | 133 | 9.24 | 9.92 | 8.17 | 6.93 | 7.59 |
| Belum diklasifikasi (unchanged) | dark | `#94A3B8` | 215 | 7.37 | 7.91 | 6.51 | 5.74 | 6.06 |

Light soft fills are pale tints of the same hue; dark soft fills are the colour at 16% over the card. The closest pairs are terracotta and coral (30 degrees light, 27 dark), Tabungan and emerald (27), and Kebutuhan and lavender (31 light, 28 dark). The closest pillars are Tak Terduga and Budaya (47 light, 45 dark).

Pillar colours reach components only through `KAKEIBO_PILLARS` and `KAKEIBO_BUCKETS` in `lib/kakeibo.ts`, as inline `var(--color-kakeibo-*)` styles: the "Klasifikasi Kakeibo" dots (Beranda and Analisis), the "Pilar Kakeibo" bar (Beranda) and the "Per pilar Kakeibo" dots and bars (Laporan). No Tailwind utility paints a pillar, so none is added. Each mark sits beside the pillar's name, and the bar has a text alternative listing every pillar. The unused `chartColors.cyan` alias of the "Kebutuhan" token is removed. The Android companion shows pillar names only.

Rejected:
- Teal for one pillar: the gap between emerald (161) and sky (199) is too narrow for 25 degrees on each side.
- Gold or amber for "Tak Terduga": amber means a warning ("Waspada"), and gold reads as amber in the dark theme.
- Two purples in the 265 to 345 degree gap: one of them sits within 25 degrees of lavender.
- A separate text ink per pillar: one colour already passes 4.5:1 everywhere, and the current interface paints no pillar text.

### D5. Tests

- Backend `tests/test_home_feed_colours.py`: the field on both top-up legs and `null` on an expense and both "Pindah Saldo" legs; the statement count with one and four top-ups; the default colour by type, with an explicit colour, for an investment pocket; a stored colour that survives a rename. `test_broker_buy_sell_is_deterministic_and_replay_safe` also checks the position colour.
- Web: `app/page.test.tsx` (new) and `MobileHomeView.test.tsx` render top-up, transfer and expense rows and the detail dialog; `accounts/page.test.tsx` covers the form default and an explicit pick; `tokens.test.ts` checks pillar contrast as text and mark, hue distances, the neutral "Belum diklasifikasi", that only `kakeibo.ts` names pillar tokens, the lavender account default, and the home top-up branches.

## Risks

- A pillar colour sits 25 to 31 degrees from a reserved hue. The labels name every pillar, and the pillar surfaces show no transfer, investment, income or expense colour beside them.
- A client that relied on the API default `#3b82f6` for an investment account now receives `#8B5CF6`. The web is the only client that creates accounts.

## Rollback

Revert the commit. No data changes: accounts created in the meantime keep the lavender colour they were stored with, which is a valid user colour.

## Open decisions for owner

- The Beranda detail dialog's "Hapus Transaksi" calls the generic transaction delete for every row. For any movement leg, a top-up or a "Pindah Saldo", the API answers 409 ("Linked movements must be deleted through the movement endpoint") and the dialog shows no error. This change keeps that behaviour. A follow-up could hide the button for movement legs or link to Transaksi, where top-ups and transfers have their own edit and delete paths.
