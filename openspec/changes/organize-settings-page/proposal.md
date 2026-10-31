# Proposal

## Why

Web Settings is one long modal that mixes profile, salary source, currency, budget limits, paired devices, the manual API key, notification mappings and a long sender list. One Save button sits halfway down the scroll, so it is unclear which fields it saves, and the salary controls, delete buttons and field widths are misaligned. Separately, the Beranda insight card compares the current period with the whole previous period, so on day 1 of a salary cycle it reports "pengeluaran turun 83.4%", contradicting the approved rule that comparisons use the same elapsed time.

## Approval

On 2026-10-31 the owner sent screenshots of the web Settings and asked for it to be fixed: "the webui web setting UI display is very bad all unorganized not aligned properly … it's too much to be all contained in 1 container like that". The coordinator scoped this change as a reorganisation within the existing visual system (refine, do not restyle) plus the Beranda insight-card correction. Recorded 2026-10-31.

## Expected Outcome

- The owner opens Settings from the sidebar user card (or the phone navigation drawer) and lands on a dedicated `/settings` page instead of a modal.
- On desktop, a left section list shows six sections; one section is shown at a time beside it. On a phone, `/settings` is a list of the six sections and each opens its own page with a way back.
- Every section is one card with a title, a one-line purpose and, where it has editable fields, its own Save button that saves only that section's fields.
- The salary controls are a proper select, small "Lebih baru"/"Lebih lama" buttons in one row, one primary "Jadikan sumber gaji" action, and "Ini gaji"/"Bukan gaji" buttons per review item. Every existing salary function still works.
- Sender names are a compact list with small "Ubah" and quiet "Hapus" buttons; "Hapus" asks for confirmation; a search filter appears when more than eight names exist.
- The Beranda insight card states increases and decreases as amounts over the same elapsed days, names the period, and says plainly when there is too little data instead of a percentage headline. The "HEMAT (−83.4%)" verdict badge is gone.

## What Changes

- New route `/settings` with six section routes (`/settings/profil`, `/settings/gaji`, `/settings/anggaran`, `/settings/perangkat`, `/settings/pemetaan`, `/settings/pengirim`), a shared section layout, and shared section-card primitives.
- `SettingsModal` is removed. The sidebar gear, the collapsed-sidebar menu item and the phone drawer item become links to `/settings`.
- The one combined settings form is split: Profil saves `name` and `name_aliases`; Mata uang & anggaran saves `currency`, `emergency_fund_multiplier` and `monthly_spending_budget`. The existing `PATCH /auth/settings` already updates only the fields sent, so no API change is needed.
- Salary, device, API-key, mapping and sender sections keep their endpoints and behaviour; their markup is reorganised to the shared form layout.
- Sender-name removal and notification-mapping removal gain an inline confirmation step before the existing delete call.
- `NarrativeInsightCard` on Beranda (desktop and the phone home view) reads `GET /api/dashboard/period-comparison` instead of the overview narrative. The overview's `narrative` and `comparison` fields stay in the API for compatibility.
- No backend, schema or Android change.

## Non-goals

- Restyling: tokens, colours, typography and radii stay as in `frontend/tokens.css`.
- New settings, new endpoints, or changing any validation rule or API contract.
- Changing the Analisis comparison or the overview endpoint's narrative text.
- Unsaved-change guards when leaving a section (recorded as an accepted trade-off in design.md).
- Production access or release. Verification is local with synthetic data. Pushing the branch is authorised; opening and merging a pull request stays with the owner, and release uses the approved pipeline.

## Capabilities

### New Capabilities

None. The settings page belongs to `currency-conversion-and-settings`, and the Beranda insight to `observed-insights`.

### Modified Capabilities

- `currency-conversion-and-settings`: adds "Sectioned Settings Page" (route, section navigation, per-section save, consistent form layout, sender search and delete confirmation).
- `clean-core-ledger`: "Responsive Settings Entry" opens the Settings page instead of a dialog.
- `web-api-key-management`: "API Key Section in Settings" lives in the Perangkat & akses section, collapsed by default.
- `device-pairing`: "Pairing in Web Settings" names the Perangkat & akses section as its home.
- `indonesian-daily-finance-ui`: "Friendly Form Input and Action Buttons" refers to the settings page instead of settings modals.
- `observed-insights`: adds "Beranda insight uses the fair comparison".
- `kazz-ui-and-kakeibo`: "Narrative Financial Insights" keeps the API narrative for compatibility but no longer drives the Beranda headline.

Standards: the `ui-ux-quality` skill (WCAG 2.2 AA target, 320/390/1280/1440 px, light and dark). No capability standard in `AGENTS.md` changes.

## Impact

- Frontend: `src/app/settings/**` (new), `src/components/settings/**` (new), `src/components/ui/SalarySettingsSection.tsx`, `SenderAliasesSection.tsx`, `DevicePairingSection.tsx`, `src/components/layout/AppLayout.tsx`, `Sidebar.tsx`, `src/components/dashboard/NarrativeInsightCard.tsx`, `MobileHomeView.tsx`, `src/app/page.tsx`; `SettingsModal.tsx` and its test removed; tests updated and added.
- API: none changed. The web now calls `GET /api/dashboard/period-comparison` from Beranda as well as Analisis.
- Database, Android companion, deployment: none.
