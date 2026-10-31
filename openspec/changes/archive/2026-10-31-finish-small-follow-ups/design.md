# Design

## Context

Facts from the code at origin/main 26e04af:

- `GET /api/dashboard/cash-flow-summary` computes `inflow`, `outflow` and the reimbursement figures for `today` or `month` from local midnight (or the month's first local day) to now, under one read-only repeatable-read transaction. The Android companion reads only this endpoint.
- `category_breakdown` (`services/report_figures.py`) is the report breakdown every other report uses. Its rows add up to the total outflow: reportable categories net of `load_spending_offsets`, plus a "Tanpa kategori" bucket with `id` null. An unset category classification is reported as `need`.
- Habit suggestions already carry `basis {agreeing, support}` in the stored question (`purpose_questions.habit_suggestion`), and `GET /api/review/queue` returns the stored question unchanged. Only the web chip ignored it.
- `PLAN_AUTO_LINK_ENABLED` is read by `core/config.py` (default true) and documented in `.env.example`, but `docker-compose.yml` did not pass it, so a Compose deployment could not switch it off.
- `Badge`, `MetricTile`, `ProgressBar` and `Card` (with `StatusBadge` and `SectionTitle`) are imported nowhere. `.workbench-page` and `.kakeibo-chip-need|want|saving` are used by no component, and `.card-green` only by `Card`.
- `POST /api/transactions` accepts `type` `expense` or `income` only (422 otherwise), answers 200 for both new and replayed rows, and rejects a non-trade target account with 400.
- The companion posts raw notification events to `POST /api/ingest/notifications`; it sends no account names. The server maps the institution to its account and picks the pocket in `notification_resolution.main_endpoint` / `pocket_endpoint`.
- `openspec validate --all --strict` failed on three requirements over 500 characters: one in `transaction-provenance`, two in `observed-insights`.

## Goals / Non-Goals

Goals: the fixed `top_categories` contract, the habit basis on Tinjau, the Compose switch, removal of proven-unused web code, and main specs that match the code with a clean strict validation.

Non-goals: Android work, UI restyling, new figures or changed totals, production.

## Decisions

1. **`top_categories` reuses `category_breakdown`.** The summary passes its own window and the offsets it already loaded to `category_breakdown`, and `top_categories` keeps rows with `spent > 0`, sorts by `(-spent, name, id)` and takes 3. The same query and offsets produce `outflow`, so the inclusion rules (movements, "Investasi", excluded categories, fees counted once, reimbursements per category) cannot drift. Rejected: a dedicated `GROUP BY category` query (a second definition of spending to keep in sync), and computing it in the phone (the phone has no ledger).
2. **Uncategorised spending is not listed.** The fixed contract types `category_id` as a UUID, and the Android client is built against it in parallel. "Tanpa kategori" stays counted in `outflow`, so the listed amounts can be less than `outflow` but never more. Rejected: `category_id: null` for the bucket (would break a client that parses a UUID). Recorded under "Open decisions for owner".
3. **`kakeibo_type` is the breakdown's classification.** An unset category classification reads `need`, as in every other report, so the phone and web agree. The contract's `null` stays allowed but is not produced today.
4. **Ties sort by name, then id.** The contract fixes "outflow descending, then name". The id makes the order total for two categories with the same name (only possible across kinds); it never changes the order the contract defines.
5. **The basis is inside the button.** `PurposeQuestionCard` renders `habitBasisText(basis)` ("3 dari 4 jawaban sebelumnya", shared with the ledger) as a third line of the `habit` chip, after a whitespace text node, so the accessible name reads "Sepertinya …? Makanan & Minuman · Kebutuhan 3 dari 4 jawaban sebelumnya". Rejected: `aria-describedby` (descriptions are announced inconsistently and the basis is part of what the owner chooses), and an `aria-label` override (would drift from the visible text). No API change was needed; a backend assertion now pins that `/api/review/queue` returns the basis.
6. **Compose passes `PLAN_AUTO_LINK_ENABLED` with default `true`**, next to `HABIT_AUTO_RECORD_ENABLED`. `.env.example` already documents it, so it is unchanged.
7. **Delete only what is proven unused, then what that deletion orphans.** Evidence: `rg` for imports and names (including `StatusBadge`, `SectionTitle`, `card-green`, `kakeibo-chip`), no test files for the four components, and a token scan (`var(--x)` or quoted `--x` across `tokens.css`, `globals.css`, `tailwind.config.js` and non-test sources) before and after. The scan found 11 tokens orphaned by the deletion (`--color-accent`, `--color-accent-2`, `--color-accent-ink`, `--color-paper-3`, `--danger-light`, `--primary-light`, `--warning`, `--warning-light`, `--info`, `--info-light`, `--space-md`) and, on a second pass, `--danger-soft` (only `--danger-light` used it). Six tokens that were already unused before this change (`--danger-hover`, `--font-body`, `--font-display`, `--radius-xl`, `--radius-2xl`, `--radius-3xl`) are left alone. The `.kakeibo-chip-need` and `.kakeibo-chip-saving` siblings of the named `.kakeibo-chip-want` are removed with it: no class string, static or built, refers to any of them. `tokens.test.ts` drops the `--warning`, `--info` and `--color-accent-ink` contrast pairs and the `--danger-soft` background; `--danger-text` is still checked on the three surfaces and `--expense-text` still on `--color-expense-soft`, the same colour.
8. **Spec corrections follow the code, verified by tests.**
   - `idempotent-public-transactions`: types `expense|income`, 200 for new and replayed rows, 400 for a non-trade target account. New `test_public_transaction_contract.py`.
   - `default-pocket`: the three scenarios keep their names and now describe the notification flow (no pocket named → default pocket; named pocket wins; account without pockets is itself), with name callers as the same rule. New unit test in `test_ingest.py`; existing `test_notification_sender.py` (receiving account becomes the new default pocket) and `test_entity_name_resolution.py` (parent name routed to its default pocket).
   - `tactile-pulse-ui` item 2: the shadow values in the spec (`0 4px 20px -2px rgba(0,0,0,0.03)` / `0.35`) are in no stylesheet. Cards are separated by `--surface` on `--bg` and the 1px `--border`; raised surfaces use `--shadow-sm`/`--shadow-md`; dialogs, menus, the phone navigation bar and the selection bar use Tailwind's `shadow-lg`/`xl`/`2xl`. Item 3: transfers are sky blue (`sky-500` on the ledger chip and icon, desktop and phone), not cobalt; investment top-ups are lavender (`purple-100/900`). Emerald income, coral expense and the lime/coral text-ink clause match `tokens.css` and are unchanged. Item 4: no rainbow gradient (only single-hue tonal card gradients), no purple glow drop-shadow, no 0–100 meter was found, so the text is unchanged; see "Open decisions for owner" for two borderline decorations.
9. **Long requirements are split as the OpenSpec CLI instructs.** Under MODIFIED each requirement keeps its header and every scenario, and its description is cut to one behaviour without changing its meaning. Each removed behaviour becomes an ADDED requirement with its own scenario: "Provenance of Notification Records" and "Provenance of Plan and Payroll Records" (the recording-path requirement now points to both), "Period outflow to date" and "Beranda spending so far". `observed-insights` was not in the briefed list, but its two requirements also failed `--strict`, and a clean strict run was an acceptance criterion.

## API

```
GET /api/dashboard/cash-flow-summary?period=today|month
  … existing fields unchanged …
  "top_categories": [
    {"category_id": "<uuid>", "name": "<string>", "kakeibo_type": "need|want|culture|unexpected|saving|null", "outflow": <int>}
  ]
```

At most 3 entries; `[]` without spending; never `null` in a successful response. Failure behaviour is unchanged: 401 without valid session or Bearer key, 422 for another `period`.

## Risks / Trade-offs

- Each summary request runs two more owner-scoped queries (the grouped breakdown and the uncategorised total) inside the same read-only transaction, over the same window the totals already scan.
- A deleted token could be referenced by a future class string built at runtime. The scan covers quoted names, and `tokens.test.ts` fails on any `var()` of an undefined property in stylesheets and components.

## Migration Plan

No schema or data change. Rollback is a revert of the commit; older phones ignore the new field, and the companion that reads it must treat a missing field as "not available".

## Open decisions for owner

- **Uncategorised spending in `top_categories`.** It is not listed because the fixed contract types `category_id` as a UUID. If the owner wants "Tanpa kategori" on the phone, the contract needs `category_id: null` (or a separate `uncategorized_outflow` field) and an Android change.
- **`shadow-xs` renders nothing.** 82 elements use `shadow-xs` (and one `shadow-2xs`), which Tailwind 3.4 does not generate; the built CSS has no rule for them. Most cards therefore have no shadow, which matches the corrected item 2. Either drop the class or define `boxShadow.xs`; both are visual decisions left unchanged here.
- **Glow decorations.** The Beranda hero wallet card and the Rekening net-worth card carry two blurred background orbs each (`blur-3xl`, emerald and indigo at 10–15% opacity). They are not drop shadows, so item 4 holds as written, but they sit close to its intent.
- **`--color-transfer` is violet** (`#7C3AED` / `#8B5CF6`) and feeds only the chart palette's `transfer` and `accent` entries and the Tailwind `transfer` colour, none of which a component uses, while the ledger paints transfers sky blue. Aligning the token is a separate visual change.
