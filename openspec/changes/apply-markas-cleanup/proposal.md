## Why

Phases 4 to 11 of the Markas+ redesign are built, but they left interim machinery, dead parts and recorded
follow-ups that the implementation plan assigned to phase 12: the untranslated-page registry that no product page
needs any more, components and styles with no user, two progress bars that draw the same thing, an outdated
Beranda checklist, figures that disagree in two places, and a list of deviations the owner has not reviewed yet.
This change closes the redesign so the owner can review and accept it as a whole.

## Expected Outcome

- Every signed-in page reads fully in the chosen language with no "This page is in Indonesian for now." path left
  in the code, and nothing a user sees changes except the items listed below as visible.
- Laporan's "Rincian tabungan" adds up to "Ditabung & investasi" when money was paid from savings.
- The Kredit provider header's "Pengeluaran pakai kredit" equals the hero of the listed rows for the same period.
- English short dates read "29 Sep", as the server's labels do.
- Catat's Perpindahan never preselects a Dari/Ke pair the server refuses as `same_account`.
- `GET /api/transactions` with type counts is about 55 ms faster at p95 for an owner with 20,000 rows.
- The owner has one end-review list (`end-review.md`) of every recorded deviation across phases 4 to 11, plus the
  real-phone QR scans.

## What Changes

- **Untranslated registry removed:** `untranslatedPages.ts`, `untranslated.tsx` (`UntranslatedRegion`,
  `UntranslatedNotice`, `useUntranslated`, `useInterfaceLang`), `TRANSLATED_ROUTES`, the inert
  `UntranslatedRegion active={false}` wrappers, the notice CSS and message, `lib/untranslatedRowLabels.ts` and the
  tests that exercise them. Requirement "Untranslated Pages Inside a Translated Shell" is removed.
- **`/komponen` removed (D2, default):** the development-only component sheet, its CSS module, its proxy and
  build-config guards, its test and its verify-script parts, with the static `.ws-drawer`/`.ws-grab` sample CSS.
- **Dead code removed:** six files and the unused exports, `.ws-*` rules and tokens proven unused with `rg`
  (design D3, D5, D6), including Tailwind colour keys no class uses. `Meter` and `ProgressBar` become one
  `ProgressBar` (D4). A token guard keeps unused tokens from returning.
- **Beranda checklist:** expects the shipped 136 px status column and red (`--out-fg`) expense amounts.
- **Laporan savings breakdown (server, additive):** `GET /api/reports/monthly` `savings.paid_from_savings`; the
  page lists "Pengeluaran dari tabungan" so the parts add up to the figure.
- **Kredit header window (server, additive):** `GET /api/credit/accounts/{id}` also accepts the list's own window
  (`period`, `period_offset`, or instant `from_date`/`to_date`), and the provider header sends it.
- **Ledger list query:** `GET /api/transactions` turns PostgreSQL JIT off for its own transaction.
- **English month abbreviation:** the web formatter writes "Sep" instead of CLDR's "Sept".
- **Catat default Ke:** chosen so it can never resolve to the same account as Dari; changes the preselected pocket.
- **Credit terms count-only edit (flag F1, needs the owner's answer):** keep a system-kept first due date.
- **Spec sweep:** requirements that still describe pages "not yet redesigned" or a phone navigation drawer are
  updated to the shipped state.
- **Docs:** `AGENTS.md` and `openspec/config.yaml` facts the redesign made stale.
- **End review:** `end-review.md` in this change.

Checked and dropped (the code shows them done or wrong, see design "Investigated"): the axe `target-size` finding
on Beranda's "Rincian" behind a phone sheet (gone since Catat became a modal `ShellDialog`; re-verified only), Catat
storing `credit_terms {installment_count: 1}` (the server stores no row for it; a regression test is added), and
the held branch `fix/selection-bars-and-tinjau` and the `cash-flow-tracker-ui-refine` worktree (both already gone).

## Changes to money figures, stored data or API requests

- **Figures:** the Kredit header's "Pengeluaran pakai kredit" and "Tagihan kredit dibayar" stop counting rows
  timed after "now" or before the cycle's starting instant on its first day, as the list already does (D9). No
  other figure value changes; Laporan gains a line whose amount the figure already contained (D8).
- **Stored data:** none by default. Only F1, if accepted, keeps a stored `system` first due date on a count-only
  edit where today it is erased and re-derived from the account's current days (which can move an already-due bill).
- **API requests:** the provider header's request carries the list's window parameters (additive server
  parameters); Catat's default Perpindahan sends a different target pocket when the owner keeps the default (D13);
  F1 changes the count-only terms `PUT` body from `first_due_date: null` to the stored date. Responses gain
  `savings.paid_from_savings` (additive). No migration.

## Non-goals

- The AI `provider_invalid_output` follow-up, the English benchmark, the companion app and notification capture.
- Behaviour changes found but not requested: opening public sign-up (the language plan's "until phase 12" note),
  the recurring-charge detection basis for Tabungan-pillar charges, a full catalogue-key prune. They are listed
  as follow-ups for the owner.
- Production: local verification only. Release goes through the designated workflow after the owner merges.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `interface-language`: untranslated-page requirement removed; English short months.
- `monthly-report`: question 2 lists spending paid from savings; the report's `savings` block carries it.
- `credit-accounts`: the overview accepts the ledger list's window; F1 keeps a kept due date on a count-only edit.
- `transaction-ledger-management`: provider header uses the list's window; F1 request; Perpindahan default accounts.
- `design-tokens`: the final token set has no unused token or Tailwind colour key.
- `explainable-figures`, `monthly-navigation-stepper`, `tactile-pulse-ui`, `theme-presets-switcher`,
  `clean-core-ledger`: interim "not yet redesigned/translated" and drawer wording updated to the shipped state.

No project capability is added (`AGENTS.md` capabilities unchanged). Standards that apply: money (figure parity
before/after, en vs id), api-consumers (additive fields and parameters only), database (no migration; disposable
PostgreSQL V1 → V54 for tests), multi-user (owner scoping of the new parameters).

## Impact

- Frontend: `components/workspace/*`, `components/layout/AppLayout.tsx`, dialogs that wrap `UntranslatedRegion`,
  `components/report/SavingQuestion.tsx`, `app/ledger/CreditHeader.tsx` and `page.tsx`,
  `components/capture/ManualView.tsx`, `lib/format.ts`, `app/globals.css`, `tokens.css`, `tailwind.config.js`,
  `messages/{en,id}.json`, `proxy.ts`, `next.config.mjs`, `app/komponen/` (removed).
- Backend: `routers/credit.py`, `routers/transactions.py`, `services/monthly_report.py`; tests.
- Tooling: `scripts/verify/markas/beranda-checklist.js`, `checks.mjs`, `primitives.mjs` (removed), scenarios, a new
  `pixel-diff.mjs`.
- Docs and specs: `AGENTS.md`, `openspec/config.yaml`, the spec deltas above.

## Approval

Pending. The owner answers the flagged defaults (design "Decisions to confirm") and flag F1.
