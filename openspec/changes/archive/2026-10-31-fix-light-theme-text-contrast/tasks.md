# Tasks

## 1. Text tokens and their use

- [x] 1.1 Extend `frontend/src/app/tokens.test.ts`: `--accent-text`, `--expense-text`, `--primary-text` and `--danger-text` at 4.5:1 or better on `--surface`, `--bg` and `--surface-raised` (plus their soft or sidebar background) in both themes; the resolved Tailwind config paints coral text utilities with the text tokens and keeps the fills; the signature fills stay `#66CC55` and `#FF334B`. Evidence: 9 of 21 fail on the previous tokens and config.
- [x] 1.2 Add the text tokens to `frontend/tokens.css` (`#2D6E22` and `#B8142D` in light; the vivid lime and coral in dark) and map `text-rose-400` to `text-rose-700`, `text-expense` and `text-primary` to them in `tailwind.config.js`. Evidence: the token tests pass; the built CSS has `.text-rose-500{color:var(--expense-text)}` and `.bg-rose-500{background-color:var(--color-expense)}`.
- [x] 1.3 Switch the arbitrary-value lime and coral text classes: the two phone Beranda links to `--accent-text`; `Input`, `DevicePairingDialog`, `Badge` and `MetricTile` to `--danger-text` and `--primary-text`. Evidence: `rg "text-\[var\(--(accent-lime|danger|primary)\)\]" frontend/src` lists only the four lime icons on charcoal.

Expected Result: every lime and coral text reads through a text token that meets AA in both themes, and every fill is unchanged.

## 2. Verification and delivery

- [x] 2.1 Frontend `npm ci`, `npm test`, `npm run lint`, `npm run type-check`, `npm run build`. Evidence: exit codes and counts in verification.md.
- [x] 2.2 Headless Brave over CDP with a local backend on a fresh PostgreSQL and synthetic data: Beranda, Transaksi, Analisis, Tinjau, Rekening & Saldo, Target & Tagihan and Settings at 1280 and 390 px in light and dark, before and after. Evidence: counts, lowest ratios, overflow, console errors and screenshot folders in verification.md.
- [x] 2.3 `git diff --check`, `openspec validate fix-light-theme-text-contrast --strict`, `openspec validate --all`. Evidence: exit codes in verification.md.
- [x] 2.4 Archive, commit and push `fix/light-theme-text-contrast`. Evidence: pushed branch.

Expected Result: both light-theme defects are fixed with test and browser evidence, pushed for the owner's review.
