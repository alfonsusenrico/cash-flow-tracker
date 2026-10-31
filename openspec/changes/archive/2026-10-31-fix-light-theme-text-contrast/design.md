# Design

## Context

See proposal.md for the problem. Facts that shape the approach:

- `tailwind.config.js` maps `rose-400/500` to `--color-expense` (`#FF334B` light, `#FF4757` dark) and `rose-600/700` to `--color-expense-hover` (`#E02038` light, `#FF6675` dark). The same names colour text, fills, borders and shadows. `text-expense` and `text-primary` resolve to `--color-expense` and `--primary`, which are the same coral in light.
- Coral text appears through about 150 utility classes in some 45 files (`text-rose-400` to `text-rose-700`, `text-expense`, `text-primary`), plus five arbitrary `text-[var(--danger)]` and two `text-[var(--primary)]` classes. Lime text appears only as `text-[var(--accent-lime)]`: two links on phone Beranda, and four icons on the charcoal `#1E201E` quick-action buttons.
- Light surfaces: `--surface` `#FFFFFF`, `--bg` `#E8ECEF`, `--surface-raised` `#D8DFE5`. Dark: `#0E1118`, `#050608`, `#181E2A`.
- Before-run (d3ed9f1, 28 loads): light coral 50 of 77 elements below the minimum, lowest 2.68:1 ("(38.9%)" on the raised surface); light lime 2 of 11 below, lowest 1.71:1 ("Kelola →"). Dark: every coral element at least 5.00:1, every lime element at least 8.07:1. The lime "+" icons measure 8.07:1 on the charcoal in both themes.

## Decisions

1. **Text inks of the same hue.** `--accent-text` `#2D6E22` is the Spring Lime hue (111°) at lower lightness. `--expense-text` `#B8142D` keeps the coral's red hue (351° against 353°). On `--surface`, `--bg` and `--surface-raised` they measure 6.24, 5.25 and 4.64:1 (lime), and 6.61, 5.57 and 4.91:1 (coral); coral measures 5.67:1 on `--color-expense-soft`. Rejected: Tailwind `rose-700` `#BE123C` (pinker, 4.67:1 on the raised surface) and `#2F7A22` (4.50:1 on the paper, no margin).
2. **Dark theme keeps the vivid colours as text.** `--accent-text` is `--accent-lime` (8.21:1 or better on the dark surfaces) and `--expense-text` is `--color-expense` `#FF4757` (5.00:1 on the raised surface, 4.64:1 on the composited soft badge). Dark `text-rose-600/700` without a `dark:` override move from `#FF6675` to `#FF4757`, which still passes. Rejected: `#FF6675` for all dark coral text, which would lighten every dark expense amount for no AA gain.
3. **`--primary-text` and `--danger-text`.** Brand-coral text (the sidebar avatar initials, the "Analisis Lengkap" icon, the unused `Badge` and `MetricTile`) uses `--primary-text`, the same pair as `--expense-text` but named for its meaning, so a future change to the primary colour does not move expense text. Error and negative texts that used `--danger` use `--danger-text`, which follows `--expense-text` as `--danger` follows `--color-expense`.
4. **Map the text utilities in Tailwind instead of rewriting every class.** `theme.extend.textColor` points `rose-400` to `rose-700`, `expense` and `primary` at the text tokens. `bg-`, `border-`, `ring-` and `shadow-` utilities still read `colors`, so fills are unchanged. One mapping covers every current coral text, including dialogs that the browser run does not open, and keeps future `text-rose-500` accessible. Rejected: replacing about 150 classes across 45 files (large diff, easy to miss one, and a later `text-rose-500` would regress); remapping `colors.rose` (would darken every coral fill and chart). `rose-300` (dark-only) and `rose-800/900` are standard Tailwind values and stay.
5. **Hover distinctions.** `hover:text-rose-*` follows the same mapping. The only base-and-hover pair inside the coral scale, "Kaitkan pembayaran" (`text-rose-500 hover:text-rose-400`), already resolved both to the same colour. Icon buttons that go from `--muted` to coral on hover still change colour.
6. **Lime icons on charcoal stay `--accent-lime`.** They sit on the theme-independent `#1E201E` at 8.07:1; `#2D6E22` there would be 2.63:1.
7. **Tests.** `tokens.test.ts` checks each text token at 4.5:1 or better on `--surface`, `--bg` and `--surface-raised` in both themes, and on its soft badge or sidebar background. A second test resolves the Tailwind config and asserts that the coral text utilities read the text tokens while `bg-rose-500`, `bg-expense` and `bg-primary` keep the fills. A third pins the signature fills `--accent-lime`, `--color-expense` and `--primary`.

## Risks / Trade-offs

- [`text-rose-500` and `bg-rose-500` now paint different colours] → The comment in `tailwind.config.js` and the mapping test state it. A coral fill with coral text on it does not occur; text on coral fills uses `--primary-contrast` or `--danger-contrast`.
- [The light coral text is visibly deeper than the coral fills] → It is the same hue, and expense meaning is also carried by the minus sign and labels.
- [Category colours chosen by the user can still be low contrast] → Out of scope; not part of the lime or coral families.

## Migration Plan

Frontend-only and visual. Ships with the normal frontend release. Rollback is reverting the commit.

## Open decisions for owner

- **Unused `.kakeibo-chip-want`.** The rule in `globals.css` paints `--color-kakeibo-want` coral text on its soft fill (3.09:1). No component uses it; it is left unchanged.
