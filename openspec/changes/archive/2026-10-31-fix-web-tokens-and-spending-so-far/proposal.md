# Proposal

## Why

The review of `fix-phone-spacing-and-primary-contrast` left four defects against existing specs or WCAG 2.2 AA:

- The phone gutter of the main workspace is `px-3` (12 px), while `executive-sidebar-layout` states `px-4` (16 px).
- Destructive and debt actions with white text on rose fall below 4.5:1 in dark mode: "+ Tagihan Baru" and "Catat pembayaran manual" measure 2.83:1, and "Ya, hapus" uses the same classes. The shared "Ya, lanjutkan" confirm button measures 1.42:1 (light) and 1.01:1 (dark), because it inherits the quiet grey text of the icon button that opened it.
- Eleven custom properties are used but never defined: `--color-accent`, `--color-accent-2`, `--color-accent-ink`, `--accent-lime`, `--danger-light`, `--foreground`, `--info`, `--info-light`, `--primary-light`, `--warning`, `--warning-light`.
- On 31 October the Beranda card "Dibanding periode lalu" said "Uang keluar sejauh ini Rp 0" while the same page showed "Pengeluaran tercatat Rp 3.600.000", all spent today.

## Approval

On 2026-10-31 the coordinator relayed the owner's request to finish these on `fix/phone-spacing-tokens` because the owner wants the web done properly. Recorded 2026-10-31.

## Expected Outcome

- On phones, pages have a 16 px side gutter with no horizontal overflow or clipped content at 390 and 320 px in light and dark.
- Every destructive or debt action with a rose fill reads at 4.5:1 or better at rest and on hover in both themes, and still looks destructive.
- No stylesheet or component uses an undefined custom property, and a test keeps it that way.
- The Beranda card's "sejauh ini" figure equals the Beranda spending summary for the same period, including today's spending.

## What Changes

- `AppLayout` main gutter `px-3` → `px-4` on phones; `sm:px-6 lg:px-8` unchanged.
- New tokens `--danger-solid`, `--danger-solid-hover` and `--danger-contrast`. The `Button` danger variant, `confirmDangerClass`, `ConfirmActionButton`'s confirm button, the internal-movement "Ya, hapus", and the Target & Tagihan "+ Tagihan Baru" and "Catat pembayaran manual" buttons use them. `ConfirmActionButton` no longer passes the trigger's classes to the confirm button.
- Token definitions for `--accent-lime` (Spring Lime `#66CC55`), `--primary-light`, `--danger-light`, `--warning`, `--warning-light`, `--info`, `--info-light` and the `.card-green` accent trio. `--foreground` uses are pointed at `--text`, and the now-redundant `#66CC55` fallbacks are removed.
- `GET /api/dashboard/period-comparison` adds `current_to_date`: the selected period's inflow, outflow and net from its start to now, never shortened. The Beranda card uses it for every "sejauh ini" figure.
- `tokens.test.ts` scans components as well as stylesheets with an empty allowlist, and checks text/background token pairs in both themes.

## Non-goals

- Restyling anything else: lime used as text on light surfaces, coral expense text, and the unused `Badge`, `MetricTile`, `ProgressBar` and `Card` components are recorded as follow-ups in design.md.
- Changing the fair-comparison rule itself: shortened comparison windows stay as specified.
- The unused `.workbench-page` rule stays.
- Production access or release. Verification is local with synthetic data. Pushing the branch is authorised; opening and merging a pull request stays with the owner.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `observed-insights`: "Fair period comparison" adds `current_to_date`; "Beranda insight uses the fair comparison" requires "sejauh ini" figures to be the outflow to date.
- `theme-presets-switcher`: "Modern Button Styling System" adds destructive and debt action buttons at 4.5:1 and confirm buttons that keep their own styling.

The gutter change implements the existing `executive-sidebar-layout` requirement, and the token definitions change no requirement.

## Impact

- Backend: `app/services/observed_insights.py` adds one window query when the comparison is shortened. The response gains a field and loses none.
- Frontend: `tokens.css`, `AppLayout`, `Button`, `ConfirmActionButton`, `SettingsCard`, `InternalMovementModal`, `goals/page.tsx`, `ledger/page.tsx`, `accounts/page.tsx`, `BottomNav`, `MobileHomeView`, `NarrativeInsightCard`, the `PeriodComparisonData` type, and tests.
- No schema, migration, Android or dependency change.
