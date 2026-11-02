# Verification

All runs were local, on 2026-11-02, on the working tree of `fix/investment-lavender-neutral-sky` (from origin/main fb40d0a) before its commit. Data was synthetic. Nothing touched the production server.

## Frontend

- `npm ci`: exit 0.
- `npm run type-check`: exit 0.
- `npm run lint`: exit 0, 0 errors, 1 warning. The warning is the existing unused `_done` in `src/app/review/page.tsx`, which this change does not touch.
- `TZ=Asia/Jakarta npm test`: **61 files, 393 tests passed**.
- `src/app/tokens.test.ts` alone:
  - on the previous tokens, config and components: 10 of 55 fail;
  - with this change: 55 of 55 pass.
  On the previous code, the sky source check lists exactly the five non-transfer elements: the "Pilar Kakeibo" card and tile, the progress note, "Ditautkan otomatis", the phone-width "Investasi" balance and "Dipilih".
- `npm run build`: exit 0. The built CSS has:
  - `.text-violet-400{color:var(--investment-text)}`;
  - `.text-investment{color:var(--investment-text)}`;
  - `.bg-investment{background-color:var(--color-investment)}` and `.bg-investment-soft{background-color:var(--color-investment-soft)}`;
  - `.text-investment-contrast{color:var(--investment-contrast)}`;
  - `.bg-violet-500\/10{background-color:rgb(139 92 246/.1)}`, the fill unchanged;
  - `.text-sky-500{color:var(--transfer-text)}`, the transfer ink unchanged.

## Backend

No backend file changed, and the backend suite was not run locally. The Checks workflow runs it on the pushed branch.

## Browser

Setup:
- headless Brave over CDP on debug port 55754;
- `next dev` on 127.0.0.1:55753 and the API on 127.0.0.1:55752, against a fresh database migrated V1→V42 by `flyway/flyway:10` in the container `cft-lv-db`;
- one synthetic owner with income, expenses, one "Pindah Saldo" of Rp 200.000, an investment platform with a mutual-fund product, a single stock, and one top-up of Rp 250.000;
- `/`, `/ledger`, `/accounts` and `/insights` at 390 and 1280 px, light and dark: 16 views before and 16 after.

The "before" run used the previous web files and the same data.

| View | axe `color-contrast` before → after | Investment text before → after (lowest) |
| --- | --- | --- |
| `/` 390 light | 0 → 0 | sky 6.37 → lavender 7.56 |
| `/` 390 dark | 0 → 0 | sky 9.46 → lavender 7.45 |
| `/` 1280 light | 2 → 0 | blue "Invest" 3.28, amber pill 5.92 → lavender 7.94 |
| `/` 1280 dark | 0 → 0 | blue 4.62, amber 9.77 → lavender 6.33 |
| `/ledger` 390 light | 0 → 0 | purple tile 9.22, sky amount and status 6.84 → lavender 7.94 |
| `/ledger` 390 dark | 0 → 0 | purple 12.71, sky 7.77 → lavender 6.33 |
| `/ledger` 1280 light | 0 → 0 | purple badge 9.22, sky target 7.56 → lavender 7.94 |
| `/ledger` 1280 dark | 0 → 0 | purple 12.71, sky 8.82 → lavender 6.33 |
| `/accounts` 390, both themes | 0 → 0 | indigo share → lavender share (fill only) |
| `/accounts` 1280 light | 2 → 2 | indigo share → lavender share (fill only) |
| `/accounts` 1280 dark | 2 → 2 | indigo share → lavender share (fill only) |
| `/insights`, all four views | 0 → 0 | none on the page |

Findings:
- **Changed elements.** Before, both `color-contrast` nodes on `/` at 1280 px light were the blue "Invest" badges (3.28:1). After, no changed element has a finding.
- **Accounts.** The 4 nodes on `/accounts` at 1280 px are the existing muted text at 75% opacity ("0 cicilan aktif" and the runway basis note), unchanged (design.md, Follow-ups).
- **Sky.** After the change, every sky element the probe finds is a movement or "Pindah Saldo" element. The "Pilar Kakeibo" card no longer has a sky tint. The two top-up rows in "Aktivitas Terkini" stay sky, as the dashboard data cannot tell them from transfers (design.md, Open decisions 1).
- **Transfers.** Sky transfer text measures at least 6.84:1 in light and 7.77:1 in dark, and sky icons at least 5.62:1, as before. The lowest sky text before, 6.37:1, was the phone-width "Investasi" balance, which is now lavender.
- **Layout and console.** Horizontal overflow was 0 px and console errors 0 in all 32 runs.
- **Screenshots.** `before/*.png` and `after/*.png` (16 each) were kept in the local scratch folder. They are not committed.

Not opened in the browser: the account, plan, top-up, amount-tracking and trade dialogs, the progress dialog on Target & Tagihan, and the pending-plan banner. Their lavender text is covered by the token tests: `--investment-text` on the soft fill and on 10 to 25% tints, and `--investment-contrast` on the fill. Their neutral text uses `--text-secondary` or `--text` on `--surface-raised`, which measure 8.02:1 and 14.44:1 in light and 6.51:1 and 15.96:1 in dark (computed from the tokens, not measured in the browser).

## Repository

- `git diff --check`: exit 0.
- `openspec validate --all --strict`: 70 passed, 0 failed.
