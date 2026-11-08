# Verification: apply-markas-rekening (Markas+ phase 8)

Branch `feat/markas-rekening`. Acceptance basis: the owner's standing instruction "complete all until finished"
(2026-11-06, renewed 2026-11-08); every flagged decision took its recorded default, and the coordinator's decision of
2026-11-08 keeps the label "Saldo tersedia". Local verification only, synthetic data, disposable databases. No
production access, no paid service or AI run.

## Approval, flags and dependencies (tasks 1.1, 1.2)

- **Flags (D3, D5 ×2, D6, D8, D11, D12):** all take their defaults: the summary figures move into the hero's Rincian
  and the group totals; Utang lists credit only with one Tagihan link row; Per rekening names its per-use subtotals in
  words (no stacked bar); gains read with a sign and one decimal; "Bayar tagihan" prefills Perpindahan through
  `openMovementCapture`; Grafik keeps the ring of uses; reordering only in Atur urutan. Exception (coordinator,
  2026-11-08): the Rincian line for `liquid` keeps "Saldo tersedia" ("Available balance"), not "Dana likuid".
- **Base:** stacked on phase 5 before it merges, by the coordinator's instruction: `origin/feat/markas-transaksi`
  merged at `ca3f847`, its fix `36b2113` at `1094ba7`. `origin/main` (`f855951`) holds phases 1 to 4, the language
  system, localized server text and `add-credit-spending-flow` (#91). Once PR #93 merges, `origin/main` is merged
  instead and every check below is repeated.
- **Phase 5 pieces as planned:** `RowAmount` in `components/transactions/rows.tsx` (expense `c-expense`), `.dl`,
  `.box`, `ShellDialog` with a footer, `RadioSegments`. No deviation.
- **Credit API as planned** (on the seeded disposable database): `GET /api/accounts` credit rows carry
  `statement_day`, `limit_left`, `next_due`; `GET /api/credit/accounts/{id}` returns `bills[].items[]` with
  `days_to_settle` and `settled_on_time`, all six statuses and one item with `settled_on_time` false
  (`scratchpad/markas-rekening/env/api-proof/`). Deviation: items carry no purchase name, so an item reads its kind
  and date (design "Deviations recorded at implementation").
- **`credit-accounts` "Credit Account Type"** on `main` equals this delta's text except the tile scenario, as planned.
- `openspec validate --all --strict`: see "Frontend and repository checks".

## For PROJECT_STATE.md

- 2026-11-08: phase 8 `apply-markas-rekening` built on `feat/markas-rekening`, pushed without a PR, stacked on phase 5
  (PR #93). After #93 merges: merge `origin/main`, re-run the gate, then the owner opens the PR.
- Open item: after this phase merges, check that each "Taken" part of D19 is on `main`, then delete
  `fix/selection-bars-and-tinjau` locally and on the remote and remove the `cash-flow-tracker-ui-refine` worktree.
- Phase 9 contract (`apply-markas-target`): `ProgressBar` and `GainText` (`components/workspace/figures.tsx`),
  `creditFacts`, `BILL_STATUS_KEY`, `billStatusTone`, `isUnpaid` (`components/accounts/credit.ts`),
  `AccountDetailDialog({ accountId, onClose })` (standalone, loads its own data, safe outside the shell),
  `openMovementCapture({ sourceAccountId?, targetAccountId?, amount? })` on the workspace context, and the terms
  `debt`, `limitLeft`, `overLimit`, `leftToPay`, `everydayMoney`, `billStatus.*`.
- Untranslated-page test example moved from `/accounts` to `/settings` (phases 6, 7, 8 and 9 leave Pengaturan alone).
- End-review list for the owner: the deviations below and the seven flags above.

## Environment

- Disposable `postgres:16` `cft-rekening-db` (127.0.0.1:55731), databases migrated V1 → V51, `seed_parity.py`,
  → V54 by `flyway/flyway:10`, then `seed_rekening.py`: `cft_markas_rk` (id, IDR), `cft_markas_rk_en` (en, same data),
  `cft_markas_rk_stress` (id, USD display, stress additions). Synthetic owner `dimas_contoh`.
- APIs 8731 (id), 8732 (en), 8733 (stress) from this worktree's backend; production builds of this worktree's frontend
  (after, 3751) and of `1094ba7`'s frontend exported with `git archive` (before, 3753); loopback proxies 3750, 3752,
  3754 to 3757, fault proxies 3760 to 3767 for the states. Scripts in
  `scratchpad/markas-rekening/env/` (`seed_all.sh`, `stack_up.sh`, `stack_down.sh`, `down.sh`, `pytest_run.sh`).

## Seeds (task 1.4)

`scripts/verify/markas/seed_rekening.py` (new, loopback only, through the API; one guarded SQL step sets
`reconciliation_required` on Tagihan Rumah because only the notification path sets it): 21 additions, 0 skips
(id and en), 104 with `--stress`. Kartu Kredit Contoh carries one bill of each status, an instalment settled late
and a fee; Kredivo Contoh has no due day; Kredit Lewat Limit Contoh is over its limit; Akulaku Contoh is overpaid;
Mandiri Contoh is a parent with its own balance; Pluang Contoh holds a product without cost basis. The parity seed
already holds Stockbit gains and losses, Bibit's amount-tracked funds, two obligations (Rp 12.250.000) and an
awaiting leg. `test_seed_parity.py`: 15 passed.

## What changed

- **Page:** `AccountsView` (title actions, "…" with Perbarui harga on phones, Atur urutan and Rencana; viewport
  breakpoints; `?tampilan=grafik`; `?akun=`), `RekeningHero` (HeroFigure, one Rincian with the server lines and the
  "Buka Target & Tagihan" link when an obligation exists, "dari aset", SplitBar and % legend, error box without Rp 0),
  `RekeningGroups` (2 px rails, counts, gains, totals, Investasi per-kind lines, Per rekening subtotals in words,
  Utang credit-only with the Tagihan link row, reorder handles, phone wallet), `Tile` (InstitutionMark, one meta line,
  balance, `GainText`, one note line, credit tile per H8, reorder grip and phone move buttons), `RekeningChart` (ring
  of uses beside the legend toggle group, `.hbar` per kind and per product or per kantong, grouped under rails per
  rekening), `AccountDetailDialog` (560 standalone, figure, `.dl`, credit facts and `CreditBills`, pockets or products,
  "Juga dilaporkan", recent transactions with the red `RowAmount`, primary/secondary/Lainnya footer, archive confirm in
  the footer), `AccountLinks` (translated, inline confirm).
- **Dialogs (on `ShellDialog`, translated):** account and pocket forms with Jenis as a radiogroup, nine colours as a
  radiogroup and both credit days with the past-bills hint; Perbarui nilai; Sesuaikan saldo; Beli/Jual;
  Top up; amount-tracking switch; Cocokkan saldo. Requests unchanged (asserted body by body), with two recorded
  additions: `statement_day` for credit, and `statement_day: null` when an edit leaves the credit type (otherwise the
  API refuses with `credit_fields_not_allowed`).
- **Shared:** `ProgressBar`, `GainText`, `format.percent`, `creditFacts`, `openMovementCapture` (AppLayout; it also
  resets a stale prefilled amount when the plain movement form opens), `ExplanationDialog` link optional.
- **Language:** `accounts` namespace and the new `terms` in both catalogues; `/accounts` in `TRANSLATED_ROUTES`, its
  `untranslatedPages.ts` entry removed, the title from the catalogue; copy-register roots now include `app/accounts`,
  `components/accounts` and the three `components/ui` money dialogs.
- **Removed:** `WalletSummary`, `WalletRails`, `WalletChart`, `ReorderPanel`, `GainChip`, `walletParts`, the old
  `AccountDetail`, `wallet.module.css`, the bills model (`Bill`, `BILLS_ROW`, `billsOf`, `billsIn`), `KIND` and the
  Indonesian helpers of `accountFacts.ts`, `PHONE_WIDTH` and `useWidthAtMost` on Rekening, and the Rekening-only rules
  of `money.module.css` (`notice`, `alert`, `detail*`, `check*`, `gain*`; the Target & Tagihan and Pengaturan rules
  stay).
- **/komponen:** group, tile, credit tile and bar samples.

## Side by side and measurement (tasks 5.1, 5.2)

Captures in `scratchpad/markas-rekening/` (`after/{id,en}`, `after-wide/{id,en}`, `states/`, `stress/id`,
`before/{id,en}`, `menus/`), from production builds of the branch (final build after the menu fix) and of `1094ba7`.

| Run | Captures | Checklist checks | axe serious/critical | Console errors | Horizontal overflow |
| --- | --- | --- | --- | --- | --- |
| After, Indonesian: 33 scenarios × 1440/390 light/dark | 106 | 1254 pass, 0 fail (after the sheet-padding expectation fix) | 0 | 0 | 0 |
| After, English: the same | 106 | 1254 pass, 0 fail | 0 | 0 | 0 |
| Extra widths 1280, 1200, 1024, 768, 767, 320 light/dark, 4 page scenarios, id | 48 | 1052 pass, 6 fail (see below) | 0 | 0 | 0 |
| The same, en | 48 | 1052 pass, 6 fail | 0 | 0 | 0 |
| Stress and USD seed, 1440, 1024, 390, 320 light/dark, Kartu and Grafik, id | 16 | 320 pass, 0 fail | 0 | 0 | 0 |
| States (loading, empty, accounts failure, hero failure) × 4 cells, id and en | 32 | expected fails only (no hero figure or Rincian in the loading and hero-failure states) | 0 (the loading state after its fix) | only the injected 500 responses | 0 |
| Before (`1094ba7`), Kartu, Per rekening, Grafik, id and en | 24 | 96 pass | 0 | 0 | 0 |
| Menus after the menu fix (`dlg-menu-akun`, `dlg-sesuaikan-saldo`, `dlg-menu-rekening`), id and en | 24 | 352 pass, 0 fail | 0 | 0 | 0 |

- The 6 extra-width fails are "three tile columns at 1024": at 1024 px the page column is 1024 − 240 (sidebar) − 96
  (padding) = 688 px, which holds two 240 px tiles; the prototype computes the same. D1's "three at 1024" was an
  arithmetic slip; the checklist now expects two.
- Found and fixed during the gate: long product names widened their grid cell (`.tiles > li` now `min-width: 0`);
  gains overflowed the 136 px bar column (now 176 px, deviation 5); a bill's "sisa" sat beside its amount (now under
  it); the loading skeleton named a plain `div` (axe `aria-prohibited-attr`, now a status line); and **the "Lainnya"
  menu of every dialog rendered outside the modal `<dialog>`**, so it was inert (desktop) or hidden behind the
  dialog (phone sheet). `MenuButton` now renders its menu in the dialog that holds the button
  (`components/workspace/Menu.tsx`), and `dialog.shell-dialog` keeps its rise animation with a backwards fill only,
  so a finished animation no longer makes the dialog the containing block that clipped the menu. Both are shared
  fixes that also serve any later menu opened from a dialog. jsdom has no `inert`, so only the browser run showed it.
- First content at 1440: the hero starts at y = 192 (checked in every page capture); one 44 px figure, no other text
  ≥ 33 px, sizes within {12, 14, 16, 20, 24, 32, 44}, weights within {400, 500, 600}, no border ≥ 2 px, tiles 112/16/12
  radius 12 with a 1 px edge, four columns at 1440 and one on a phone, 24 × 24 marks, the wallet tile 280 × ≥ 152 at
  390, the reorder bar 48, dialog widths 480/520/560, `.dl` 160.
- The prototype side-by-side images of task 5.1 were not composed (see Not verified); the checklist values come from
  the prototype's `styles.css`.

Key screenshots: `after/id/rekening-1440-light.png`, `after/en/rekening-1440-light.png`, `after/id/rekening-390-dark.png`,
`after/en/rekening-390-dark.png`, `after/id/dlg-rekening-kredit-tagihan-1440-light.png` (credit detail with six bill
statuses), `after/id/dlg-produk-1440-light.png` (unit-tracked stock), `after/en/rekening-grafik-1440-light.png`,
`stress/id/rekening-320-dark.png`.

## Accessibility and keyboard (tasks 5.3, 5.4)

- axe-core (wcag2a, wcag2aa, wcag21a, wcag21aa, wcag22aa): 0 serious or critical in every capture above, in both
  themes and languages, with each dialog, the "…" menu and the detail's "Lainnya" menu open, hidden balances, Atur
  urutan, and the four states. Console errors 0 outside the injected 500 responses of the failure states.
- Language: English captures have no untranslated notice and no `lang="id"` part; the before English captures show the
  notice, as the old page did.
- `rekening-keys.mjs`, the 14 flows of D22 at 1440 and 390: **Indonesian 23/23, English 23/23** (`keys/{id,en}/
  rekening-keys.json`): Tab order; Rincian and focus return; detail by Enter, Tab trapped, "Lainnya" by ArrowDown and
  Esc, focus back on the tile; a credit bill expanded by Enter, "Lihat di Transaksi", "Bayar tagihan" opening
  Perpindahan with Rp 434.000; Grafik and a legend use by Space and a product bar; the groupings; Atur urutan with Alt +
  ArrowRight on a tile and Alt + ArrowDown on a handle, the announcement, focus kept below the top and reorder bars,
  Selesai and a reload keeping the order; the phone move buttons (44 px, first disabled); the account form (Jenis by
  arrows, both credit days, 32 refused under the field, Esc); Jual's over-sale error on the units field and Batal's
  focus return; Cocokkan saldo's announced result and "Sesuaikan saldo" with the typed amount; the price toast; the
  Tagihan row to `/goals`. The saved layouts were restored after the flows.
- Unit and page tests (jsdom): `app/accounts/page.test.tsx` 19, `AccountDialogs.test.tsx` 17, `rekeningParts.test.tsx`
  8, the money dialogs 29, `AppLayout.test.tsx` "Bayar tagihan" prefill, all with axe where they render a page.

## Figure parity (task 5.5)

`rekening-parity.mjs`, same seed within the same minutes, Per kegunaan and Per rekening (`parity/*.json`):

- **Before (`1094ba7`) against after, Indonesian:** 25 tiles and 5 groups each; every balance, every gain amount and
  every gain percent value equal. The 30 listed differences are all named in advance:
  - gains now carry their sign ("+Rp 396.000 · +8,0%" was "Rp 396.000 (8,0%)"): 7 tiles under each grouping and the
    3 platform groups;
  - the 4 credit tiles show limit left and the next bill instead of the limit and "% dari limit" (H8); owed amounts
    equal;
  - Utang: −Rp 14.538.000 before, −Rp 2.288.000 (credit only) after, plus the Tagihan row −Rp 12.250.000:
    2.288.000 + 12.250.000 = 14.538.000 = `total_liabilities`;
  - the hero: net worth Rp 49.191.280 and assets Rp 63.729.280 equal, shares 22/26/1/52 % equal; the five summary
    figures left the page (Saldo tersedia stays in the Rincian);
  - Per rekening headers add the per-use subtotals in words; the totals are equal; the Tagihan group became the row.
- **Hero = `net_worth`**, Utang + Tagihan = `total_liabilities`, legend shares = the previous share bar: checked above
  against the API proof (`net_worth` 49.191.280, `total_assets` 63.729.280, `total_liabilities` 14.538.000,
  `credit_owed` 2.288.000).
- **English against Indonesian** (region ID, same data in two databases): 50 tiles over both groupings, every group
  header and the hero, **0 differences**, byte for byte.

## Copy (tasks 3.1 to 3.4)

- Catalogue parity, copy register (both languages) and "hold no literal copy" pass (frontend suite).
- Indonesian text before against after (`rekening-copy.mjs`, Per kegunaan and Per rekening): every string that left
  or arrived is an intended change: "Perlu rekonsiliasi" → "Perlu dicocokkan"; "Saldo tersedia", "Untung/Rugi" and
  the summary labels leave the page (Saldo tersedia stays in the Rincian); the drag hint only in Atur urutan; the
  obligation cards' names → the "Tagihan di Target & Tagihan" row; "% dari limit" → "Sisa limit", "Tanpa limit",
  "Tanggal jatuh tempo belum diisi"; "Tunai" → "Uang tunai" as the type line; pocket names in words
  ("Kantong di BCA · bawaan", and under Per rekening "Uang harian · Kantong di Jago"); "dari aset"; "Modal belum
  lengkap" for a platform; the old "Isi {rekening}" and "Pindah baris …" names (now only in the mode).
  Dialog text: the form labels in sentence case and the button words of Fork A and B (listed under Deviations).
- An English `/accounts` shows "Accounts & Pockets" and no "This page is in Indonesian for now." in every capture
  (`lang.notice` false); the page has no `lang="id"` part.

## Frontend and repository checks (task 5.7)

Code at `8d41704` (captures, keys, tests); `c0ed55e` only renames a component (tests, lint and type-check re-run on it); later commits are docs only.

| Check | Result |
| --- | --- |
| `npm test` (vitest) | 97 files, **969 passed**, 0 failed (one Tinjau focus-timing test failed once while the browser captures loaded the machine; it passed on every quiet run) |
| `npm run type-check` | 0 errors |
| `npm run lint` | 0 problems |
| `npm run build` (`next build`) | passes |
| Backend `pytest` on a fresh `postgres:16` (`cft-rekening-pytest`, 127.0.0.1:55732) migrated V1 → V54 by `flyway/flyway:10` | **2180 passed**, 1 warning, exit 0; container removed |
| `test_seed_parity.py` | 15 passed |
| `openspec validate --all --strict` | 85 passed, 0 failed (before the archive) |
| `git diff --check` | clean |
| `git diff --stat 1094ba7 HEAD -- backend db mobile nginx` | empty (`origin/main` differs only by phase 5's own backend files) |
| `frontend/package.json`, `package-lock.json` | unchanged |

## Deviations and notes for the owner's end review

1. Stacked on phase 5 before it merges (coordinator).
2. "Saldo tersedia" kept in the Rincian (coordinator, 2026-11-08).
3. Credit bill items show their kind and date, not a purchase name (the API has none).
4. The credit tile shows "Melewati limit" in its meta line; it is also the tile's one note, so no second note line
   shows for an over-limit credit account (the detail lists both).
5. `.hbar` fourth column is 176 px (168 px below 1024) instead of 136 px: a gain such as "+Rp 120.000 · +12,0%"
   is wider than 136 px and overflowed the page edge.
6. The detail header has no institution mark beside the title (`ShellDialog` has no leading slot); the mark is on
   every tile and group.
7. Beli/Jual have no date field (the dialog never had one; the request sends the current time). The in-dialog
   direction toggle is gone; the caller picks Beli or Jual.
8. "Sama dengan top up terakhir ke produk ini." is in the catalogue but never shown: the dialog does not know the
   last top-up.
9. Button and title words in the dialogs follow Appendix A: "Beli"/"Jual" (was "Catat Pembelian"/"Catat
   Penjualan"), "Top up", "Tambah rekening", "Simpan", "Buat kantong", "Simpan nilai", "Sesuaikan saldo";
   "Ubah rekening" with the name as the meta line (was "Ubah Rekening: X").
10. Editing a credit account to another type also clears `statement_day` (see What changed).
11. The ring dims the uses that are not chosen (opacity), besides the legend's straight bar.
12. The per-use subtotals, the counts and the type line are new words on the page; their values are sums of the
    same server balances.
13. Two tile columns at 1024 px, not three (D1's arithmetic; the prototype computes two).
14. Shared fixes outside Rekening: dialog menus render inside their dialog, and `dialog.shell-dialog`'s rise
    animation fills backwards only (see "Side by side and measurement").

## Not verified

- The prototype side-by-side composites of task 5.1 were not produced; the measurement checklist compares against
  `styles.css` values instead, and every visible difference found is fixed or listed above.
- Before captures cover the page (Kartu, Per rekening, Grafik) but not the old dialogs: the old dialogs were
  untranslated `Modal`s whose requests are asserted unchanged by the tests.
- The stress and USD seed exists for an Indonesian owner only; English stress captures were not run.
- Pointer drag and drop was not driven in the browser; reorder was verified by keyboard and the phone buttons, and
  the drag handlers are the old ones moved into the mode.
- The trade and top-up dialogs were captured from Rekening, not reopened from Transaksi and Tinjau in the browser;
  `app/ledger/page.test.tsx` (43) passes with them.
- Phase 5 is not merged yet; this gate ran on the stacked revision named above.

## Owner acceptance

Pending the owner's end review under the standing instruction.
