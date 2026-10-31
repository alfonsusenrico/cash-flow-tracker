# Proposal

## Why

The changes archived on 2026-10-30 and 2026-10-31 left small follow-ups open. The phone needs the top spending categories for its summary. The Tinjau habit guess hides its basis. `PLAN_AUTO_LINK_ENABLED` cannot be set through Compose. Four web components and some CSS rules are unused. Several main-spec clauses no longer describe the code, and `openspec validate --all --strict` still fails on three over-long requirements.

## Approval

On 2026-10-31 the owner approved implementing all the small items ("do all the small items too"). The contract for the phone's top categories was fixed by the coordinator so that the Android companion can be built against it in parallel. Recorded 2026-10-31.

## Expected Outcome

- `GET /api/dashboard/cash-flow-summary` returns `top_categories`: up to three categories with the most net spending in the summary's own window, largest first, whose amounts never add up to more than the summary's `outflow`. Older clients keep working because every existing field is unchanged.
- On Tinjau, the "Sepertinya …?" chip says how many earlier answers back it (for example "3 dari 4 jawaban sebelumnya"), and a screen reader announces this as part of the button name.
- The owner can switch plan auto-linking off in a Compose deployment with `PLAN_AUTO_LINK_ENABLED=false`, as already documented in `.env.example`.
- The web bundle no longer ships the unused `Badge`, `MetricTile`, `ProgressBar` and `Card` components, the CSS rules only they or nothing used, or the tokens that only those rules used.
- Main specs describe the real public transaction contract, the real default-pocket flow and the current surface and accent tokens, and `openspec validate --all --strict` exits 0.

## What Changes

- Add the additive `top_categories` field to the cash-flow summary, computed from the same category breakdown the reports use.
- Show the habit basis inside the Tinjau suggestion button. The API already returns it.
- Pass `PLAN_AUTO_LINK_ENABLED` (default `true`) through `docker-compose.yml`.
- Delete the four unused components, the unused `.workbench-page`, `.card-green` and `.kakeibo-chip-*` rules, and the tokens that become unused. Update `tokens.test.ts`.
- Correct stale spec text in `idempotent-public-transactions`, `default-pocket` and `tactile-pulse-ui`. Split the over-long requirements in `transaction-provenance` and `observed-insights` without dropping any rule or scenario.

## Non-goals

- Android companion changes. The companion is built against the contract in parallel by another agent.
- Restyling the UI. Where the audit found the UI in tension with the design standard (blurred glow decorations, `shadow-xs` classes that render nothing), it is recorded under "Open decisions for owner", not changed.
- Listing uncategorised spending in `top_categories`; see design.md D2.
- Production access or release. Verification is local, on disposable PostgreSQL with synthetic data. Pushing the branch is authorised; opening and merging the pull request stay with the owner, and release uses the approved pipeline.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `financial-reporting-consistency`: the cash-flow summary lists its top spending categories.
- `habit-learning`: the habit guess on Tinjau shows its basis in the button name.
- `idempotent-public-transactions`: the ingestion requirement states the accepted types and the HTTP 200 answers.
- `default-pocket`: the scenarios describe the phone's notification flow and name callers.
- `tactile-pulse-ui`: surface elevation and accent clauses match the live tokens.
- `transaction-provenance`: the recording-path requirement is split in two.
- `observed-insights`: the comparison and Beranda insight requirements are split.

Standards: api-consumers (additive field, existing fields unchanged), money invariants (no figure changes; the listed amounts reuse the report breakdown), multi-user (owner-scoped), WCAG 2.2 AA for the changed chip. No project capability is added.

## Impact

- Backend: `services/report_figures.py` (`top_categories`), `routers/dashboard.py`. No migration.
- API: additive `top_categories` in `GET /api/dashboard/cash-flow-summary`.
- Web: `components/review/PurposeQuestionCard.tsx`, `lib/review.ts`; deleted `components/ui/{Badge,MetricTile,ProgressBar,Card}.tsx`; `app/globals.css`, `tokens.css`, `app/tokens.test.ts`.
- Deployment: `docker-compose.yml`.
- Specs: the seven capabilities above.
- Android: none in this repository.
