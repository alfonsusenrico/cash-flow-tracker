# Verification: apply-markas-foundation (Markas+ phase 1)

Date: 2026-11-05 · Branch `feat/markas-foundation` from `origin/main` `18a4477` · Working tree as committed in the
change's commit (`feat(ui): apply Markas foundation tokens, fonts and controls`).

Owner approval (2026-11-05): "defaults ok, implement", all seven plan decisions at their defaults (see
`proposal.md` › Approval).

## Environment

- Local only. PostgreSQL 16 in a disposable container `cft-markas-pg` (127.0.0.1:55446), restored from the
  synthetic audit seed (`seed.dump`, owner `raka_contoh`, Flyway V44) and migrated to V51 with `flyway/flyway:10`.
- Backend from this worktree in a uv-created `.venv` (Python 3.12), background jobs, AI calls and outbound HTTP
  off. Frontend `next start` behind a loopback `/api` proxy; prototype served statically from
  `cash-flow-tracker-prototypes/design-options/` (so `option-6` reaches `shared/data.js`).
- The "before" state is `origin/main` `18a4477`, built in a temporary detached worktree and served on its own port
  against the same database, for the before captures and figure parity.
- Browser: headless Brave over CDP (`scripts/verify/markas/cdp.mjs`), device scale 1, theme through the app's
  storage key and `prefers-color-scheme`; axe-core 4 with wcag2a, wcag2aa, wcag21a, wcag21aa, wcag22aa.
- Captures and side-by-side images are synthetic data only. The full sets stay outside the repository because of
  their size; this folder keeps the checklists and a reviewed selection (below).

## Fonts (task 2.1, 2.2)

Downloaded on 2026-11-05 from the prototype's own request
`fonts.googleapis.com/css2?family=Inter:wght@400;500;600&family=Manrope:wght@600&display=swap` (the `latin`
subset), OFL texts from `google/fonts` (`ofl/inter/OFL.txt`, `ofl/manrope/OFL.txt`). Inspected with fontTools in a
throwaway uv environment outside the repository.

| File | Source | Bytes | SHA-256 | `fvar` | `tnum` | `lnum` | `cv11` |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `frontend/src/app/fonts/Inter-latin-variable.woff2` | `fonts.gstatic.com/s/inter/v20/…` | 48,432 | `c940764593d0fe5d596be327ca7558855e018039fb78509aa21921fd3644c3e4` | `wght` 100–900, default 400 | yes | no | no |
| `frontend/src/app/fonts/Manrope-latin-600.woff2` | `fonts.gstatic.com/s/manrope/v23/…` | 14,380 | `f56b6f3cecb3da0b83075273b83c63dac19bb443a23befe18a6441a4dac8165a` | none (static 600 instance) | yes | no | no |

- Inter's GSUB features in the served file: `calt ccmp dnom frac locl numr pnum tnum`; Manrope's: `calt dnom frac liga
  locl numr pnum tnum`. Google's subsetter removes `cv11` and `lnum`, so the prototype the owner saw rendered Inter's
  default double-storey "a" and default (lining) figures. The app ships the same files, so it renders the same
  glyphs; `--features-text` and `--features-num` are kept as in `styles.css` and take effect only if the font
  files change. Finding for the owner, not a deviation.
- Manrope is a static SemiBold instance, so it is named `Manrope-latin-600.woff2` and declared `weight: "600"`;
  Inter is declared `weight: "400 600"`; `font-synthesis-weight: none` is set on `body`.
- Production build with every proxy pointed at a closed loopback port (`HTTPS_PROXY=http://127.0.0.1:9`): passed;
  `.next/static/media` holds exactly these two files (same SHA-256); no `fonts.googleapis.com` or
  `fonts.gstatic.com` reference in `.next/static`. `nginx/default.conf` unchanged; `fonts.test.ts` checks
  `font-src 'self'`.

## Figures (task 2.3)

Tabular classes (`globals.css`): `.tabular`, `.tabular-nums`, `.t-fig-l`, `.t-fig`, `.t-amt`, `.ws-cnt`, `.ws-count`,
`.ws-kbd`, `.ws-pill`, `.ws-badge`, `.ws-tbl`, `.ws-xtable`, `.ws-kpi-value`, `.ws-kcard-v`, `.ws-legend-v`,
`.ws-money-legend b`, `.ws-kk-amount`, `.ws-dana-big b`, `.ws-dana-foot b`, `.ws-act-r`, `.ws-tx-amount-v`,
`.ws-tx-daytot`, `.ws-tri-amount`, `.ws-tri-when`, `.ws-bigline`, `.ws-facts dd`, `.ws-rows .r`, `.ws-chip-delta`,
`.ws-rail-count`, `.ws-tab-badge`, `table`, `dd`, `input`, `select`, `textarea`, `svg text`; modules: analysis `.day`,
`.spend`, `.bandFigure`, `.sum b`, `.scrubRead`, `.yax`, `.xax`; report `.change`, `.progressText`; the hero role
`.t-hero` uses `--features-hero`. Body text is `lining-nums` with `--features-text`.

Browser check (`checks.mjs`, Beranda, both themes): all 23 standalone amounts read from the DOM carry
`tabular-nums`; the subtitle "Siklus sejak 30 Okt · hari ke-7" computes `lining-nums` only. Across all 143 captures
the checklist finds 9 standalone amounts still proportional, one per capture on Analisis ("Rp 0", 14/500) and
Laporan ("Rp 4.802.000", 14/400), both inside page components that their phases rebuild.

## Tokens (tasks 3.1–3.3)

- `tokens.test.ts` (83 tests) asserts every D3 value in both themes, the type, spacing, radius and motion scales,
  the retired tokens (`--radius-*`, `--shadow-card|xs|sm|md|window`, `--*-solid-fg`, `--*-solid-hover*`,
  `--font-body`, `--font-mono`) gone, Tailwind's replaced `fontSize`, `fontWeight`, `borderRadius`, `boxShadow` and
  `fontFamily` scales, no tinted semantic fill, pillar colours as marks only, text pairs at 4.5:1, marks at 3:1,
  the focus outline at 7:1 on every surface and the pillar marks at 3:1 on the canvas, surfaces and raised fill.
- Tokens kept without a prototype counterpart until their page phase: `--heat-1…5`, `--band-op`, `--kind-emas`,
  `--kind-reksa-dana`, `--kind-saham`, `--flow-tr`, `--sidebar-width`, `--rail-width`.
- **Finding:** dark `--field-focus` `#C9C9CC` against `--line-strong` `#6E7076` measures **2.996:1**. SPEC §3.3 prints
  it rounded as "3.00:1"; the spec delta says "at least 3:1". The test bounds it at the prototype's value (≥ 2.995)
  and says why. The field's focus edge is above 9:1 against its surface and the soft ring and caret also mark focus.
  The owner decides whether the spec sentence should read "3.00:1 as in SPEC §3.3" or the value should change.

## Guards (tasks 4.1, 4.2, 5.4, 6.1)

- `typeScale.test.ts`: only the seven sizes, three weights and two families in `globals.css`, the four CSS modules,
  the Tailwind configuration and every component class string and inline style; it names a planted stray first.
- `geometry.test.ts`: radii 0, 6, 12, 16 or a circle; no border ≥ 2 px; no coloured border, outline or ring; no
  shadow except the layer shadow, the field ring and the segment hairline; no `ring-*`, `shadow-*`, focus outline or
  blur utility; planted cases first.
- `selectionMarks.test.ts` (taken over from 64909ee, geometry updated to option 6): no curved left mark in any
  stylesheet or component, the shared bar, navigation bar and rail drawn as square 2 px elements, and the goal
  picker, Grafik legend and Tinjau rows on the shared bar.

## Browser gate (task 7.1)

`capture.mjs` on the final build (with `NEXT_PUBLIC_COMPONENT_SHEET=1` so `/komponen` exists):
**143 captures** = 21 routes × 1440/390 × light/dark (84), `/komponen` × 4, and 55 dialog captures (Catat
"Tulis transaksi" in all four cells, the other dialogs at 1440 light, 1440 dark and 390 light as in the audit).
Raw PNGs: `scratchpad/markas-web/shots/after/` (outside the repository); checklist: `after/checklist.json`.

| Check | Before (`18a4477`) | After |
| --- | --- | --- |
| axe-core serious or critical | 1 (Kalender dark, colour-contrast on 3 markers) | **0** in all 143 |
| Console errors | 0 | **0** |
| Horizontal overflow at 1440 and 390 | 0 | **0**; at 320 px **0** on Beranda, Tinjau, Transaksi, Analisis, Laporan, Rekening, Target, Pengaturan |
| Families | Plus Jakarta Sans, Geist Mono | **Inter, Manrope** only |
| Sizes in use | 9.5–34 px, half pixels | **12, 14, 16, 20, 24, 32, 44** only (44 only on the sheet's hero sample) |
| Weights in use | up to 800 | **400, 500, 600** only |
| Manrope outside titles and the brand | n/a | **0** |
| Border ≥ 2 px | present | **0** |
| Coloured border on a field, card, tile, dialog or chip | present | **0** |
| Coloured outline or ring outside focus | present | **0** |
| Shadow on a non-floating element | card shadows | **0**; dialogs, menus and toasts carry `--shadow-float` |
| Radius other than 0/6/12/16 or a circle | many | **0** |
| Dialog scenarios that opened a `role=dialog` | 36 of 55 | 36 of 55 (the rest are menus, popovers or the Tinjau pane, or not offered at 390: Ubah transaksi, Notifikasi and Ubah kategori, as before) |

- Text styles per page: 2 of 84 page captures exceed 8 (Analisis 1440 light and dark, 9); the ≤ 8 rule belongs to
  the Markas page layouts of phases 3–11.
- Before and after captures (`compare/before-vs-after-*.jpg`) show the same section order, grids and widths; the
  differences are type, colour, borders and control geometry.

Screenshots in this folder (JPEG, synthetic data): `before/` and `after/` for Beranda 1440 light and dark,
Beranda 390 light, Transaksi 1440 light and dark, Catat (Tulis transaksi) 1440 light and dark, Catat manual 1440
light; `after/komponen-1440-{light,dark}.jpg`; `compare/prototype-vs-app-*.jpg` (komponen, Beranda, Transaksi,
Catat) and `compare/before-vs-after-*.jpg`.

### Component sheet against the prototype (task 5.3)

`primitives.mjs` measures 27 primitives in both themes on the prototype's `#/komponen` and the app's
`/komponen` (height, four paddings, radius, border width and colour, family, size, weight, line-height,
letter-spacing, text colour, fill and shadow): **826 values equal, 14 differ** (`primitives.json`):
- the link button's radius (prototype 6 px, app 0) — it has no box or border, so nothing renders differently;
- the sheet's static dialog sample, whose paddings sit on the container in the app and on its header, body and
  footer in the prototype (219 vs 223 px). The real `Modal` uses the SPEC parts (header 24 24 16 with the Manrope
  20/600 title, body 0 24 24, radius 16, 1 px line, `--dialog`, layer shadow), measured on the page dialogs.

Fixed during the gate: the chip's right padding with a remove button (4 px), the money input's line-height, the
focus radius, the light cell of the sheet under a dark page, and `b`/`strong` rendering 700.

### Scripted checks

`checks.mjs` (`checks.json`): **33 of 35 pass, 0 fail, 2 not reachable.**

| Check | Light | Dark |
| --- | --- | --- |
| Keyboard focus, first 14 stops on Beranda, Transaksi, Pengaturan › Profil: 2 px `--focus` outline, fields the ring | pass ×3 | pass ×3 |
| Keyboard focus inside Catat (8 stops) | pass | pass |
| Keyboard focus on "+ Catat": `2px solid #45484F` / `#BDBEC2`, offset 2 px | pass | pass |
| Pointer click on a button (Filter): no outline | pass | pass |
| Clicking into the sentence field: 1 px `#2B2D32` / `#C9C9CC` edge, 3 px ring, no outline | pass | pass |
| Field error on the sheet: `aria-invalid`, `aria-describedby`, neutral field-focus edge, 12 px expense text | pass | pass |
| Field error in Catat | not reachable | not reachable |
| Open dialog carries the layer shadow | pass | pass |
| Cards flat (10 cards, `box-shadow: none`) | pass | pass |
| Primary "+ Catat": `#0A0D14` on `#FF334B` / `#FF4757` | pass | pass |
| Amounts tabular, "hari ke-" proportional | pass | pass |
| Reduced motion: dialog `animation: none`, rows `transition: 0s` | pass | — |
| No overflow at 320 px (8 pages) | pass ×8 | — |

Catat's save button stays disabled without an amount, and a movement with an amount and no accounts shows its
summary alert, so no Catat field carries `aria-invalid` in today's flows; the field state is verified on the
component sheet and by `FormField.test.tsx`. Behaviour was not changed to reach it.

### Figure parity

`parity.mjs` against the `18a4477` build on the same database: **17 page routes, 392 figures (amounts,
percentages, counts, dates and times) in document order, all identical** (`parity.json`).

### Interim look, listed for the owner's end review

Phase 1 changes look, not layout; these remain until their page phase:
- The coral active pill in the sidebar and the old top bar (phase 2).
- Rekening cards keep their 6 px coloured top strip (`wallet.module.css .card::before`), which SPEC §5 does not
  allow on a rounded element; phase 8 removes it with the card ("coloured top edges on rounded cards").
- Coral is now the only primary fill, so some pages show more than one coral button (design, Risks).
- Raw page and dialog inputs keep their 44 px height; the shared `Input`, `Select` and money input have the
  prototype's 40 and 56 px. Each page phase moves its forms to the primitives.
- Analisis and Rekening tabs take the segmented-control look; Analisis becomes underline tabs in phase 6.
- Table heads keep sentence case; the uppercase label role arrives with each page's table.
- Catat's movement validation shows its summary alert in English ("Source and target accounts must be
  different"), unchanged by this change; it belongs to the copy register and phase 11.

## Frontend checks (task 7.2)

On the committed working tree:

| Command | Result |
| --- | --- |
| `npm test` | 81 files, **738 tests passed** |
| `npm run type-check` | passed |
| `npm run lint` | passed, 0 problems |
| `npm run build` (no sheet flag, Google unreachable) | passed; `/komponen` answers 404 (proxy), no sheet code in `.next`, two fonts in `.next/static/media` |
| `git diff --check` | clean |
| `openspec validate --all --strict` | 84 passed, 0 failed |
| `git diff --stat origin/main -- backend db mobile nginx` | empty |
| `git diff origin/main -- frontend/package.json frontend/package-lock.json` | empty (no new dependency) |

Backend suite: not run, because no backend, database or companion file changed.

## Deviations from the prototype

None introduced. The findings above (the font features the served files lack, and the dark field-focus contrast
printed as 3.00:1) describe the prototype itself and are for the owner's decision.
