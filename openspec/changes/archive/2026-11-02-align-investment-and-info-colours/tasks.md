# Tasks

## 1. Lavender investments

- [x] 1.1 D1: add `--color-investment`, `--color-investment-soft`, `--investment-text` and `--investment-contrast` in both themes, map `text-violet-400` to `text-violet-700` to `--investment-text`, and add the `investment` Tailwind colour. Evidence: the built CSS has `.text-violet-400{color:var(--investment-text)}`, `.text-investment{color:var(--investment-text)}`, `.bg-investment{background-color:var(--color-investment)}`, `.bg-investment-soft{…}` and `.text-investment-contrast{color:var(--investment-contrast)}`, and `.bg-violet-500\/10` keeps `rgb(139 92 246/.1)`.
- [x] 1.2 D2: move every investment element of the inventory to lavender. Evidence: in the browser run, every investment element on `/`, `/ledger` and `/accounts` is lavender. The other colours the probe finds next to them are the decorative amber chip that every phone-width wallet card has, the "Bank" and "Tunai" shares beside the "Investasi" share, and the top-up rows in "Aktivitas Terkini" (design.md, Open decisions 1).
- [x] 1.3 Keep transfers sky, warnings amber, income emerald and expenses coral. Evidence: the existing transfer, amber, emerald and coral token tests still pass, and the browser run still finds sky on every movement and "Pindah Saldo" element.

Expected Result: every investment element reads as lavender in both themes, its text passes 4.5:1, and the other accents keep their meaning.

## 2. Sky only for transfers

- [x] 2.1 D3: classify every remaining `*-sky-*` utility and move the four non-transfer elements to neutral tokens: the "Pilar Kakeibo" card, the progress note, "Ditautkan otomatis" and "Dipilih". Evidence: the table in design.md D3, and the browser run finds no sky element outside a transfer on `/`, `/ledger`, `/accounts` and `/insights`, apart from the top-up rows of task 1.2.

Expected Result: sky blue means a transfer and nothing else.

## 3. Tests and verification

- [x] 3.1 D4: extend `frontend/src/app/tokens.test.ts`:
  - the `--investment-text` and `--investment-contrast` pairs in both themes;
  - `--investment-text` on 10 to 25% violet tints in both themes;
  - the violet mapping and the pinned fills;
  - the lavender inventory and the solid-fill ink;
  - the sky source check and the neutral elements.

  Evidence: 10 of 55 fail on the previous tokens, config and components (the sky check lists exactly the five non-transfer elements), and 55 of 55 pass now.
- [x] 3.2 Frontend `npm ci`, type-check, lint, test (`TZ=Asia/Jakarta`) and build. Evidence: counts in verification.md.
- [x] 3.3 Headless Brave on `/`, `/ledger`, `/accounts` and `/insights` at 390 and 1280 px, light and dark, before and after, with synthetic data that includes investment accounts and a top-up. Evidence in verification.md:
  - axe `color-contrast` on changed elements goes from 2 nodes to 0;
  - no overflow and no console errors;
  - 16 screenshots before and 16 after.
- [x] 3.4 `git diff --check` and `openspec validate --all --strict`. Evidence: exit 0.

Expected Result: local checks pass on the exact working tree that is committed.

## Workflow follow-up

- Archive the change with `openspec archive align-investment-and-info-colours`.
- Push `fix/investment-lavender-neutral-sky` and report the Checks workflow result and the compare link. The owner opens and merges the pull request.
