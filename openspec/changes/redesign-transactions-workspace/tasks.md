# Tasks

The owner approved this plan on 2026-11-03 ("approved, defaults ok") and set the copy register (proposal.md, "Approval" item 7; design D20).

The owner tests nothing until every phase is done. The evidence here is therefore the whole runtime check of this phase, and each screenshot named below is read by the agent before the task is checked.

Evidence for every task goes into `verification.md`: commands, counts, skips with reasons, screenshots and measured values. Use synthetic data only, for example "Kedai Awan", "Toko Serba Ada", "Dompet Digital", "Bank Utama", "RDN Contoh" and "Bibit Contoh · Reksa Dana A".

Backend tests run on a disposable PostgreSQL 16 migrated by Flyway V1 to V44 with `REQUIRE_TEST_DATABASE=1`, as `.github/workflows/checks.yml` does.

The browser matrix means 1440, 1280, 1024, 768 and 390 px, each in light and dark, in a headless browser on a local production build behind the loopback API proxy.

## 1. Baseline and the URL defect

- [ ] 1.1 **Baseline.** Before any change, on `feat/transactions-workspace` at the plan commit, record:
  - `npm test`, `npm run type-check`, `npm run lint` and `npm run build`;
  - `/ledger` first-load JS and the shared first-load JS from the build output;
  - the full backend suite with its skips;
  - the median of 20 local `GET /api/transactions` calls (no filter, and `period=cycle`) for a synthetic owner with 5 000 rows over 18 months;
  - axe results and screenshots of `/ledger`, the edit modal and `QuickCaptureModal` at 1280 and 390 px in both themes.

  Evidence: the baseline table in verification.md.
- [ ] 1.2 **Re-establish the URL defect** (design, "The filter URL defect"; D13) with the seeded backend and a real session. Record:
  - the first navigation from `/ledger?type=income` to `/ledger` by the "Semua" chip;
  - the same from `/laporan?…`;
  - the sidebar "Transaksi" link from `/ledger?type=income`;
  - the stepper and the type chips from a plain `/ledger` (phase 1's fingerprint);
  - where the npm registry is reachable, the same steps on a scratch copy with the newest Next.js 16.2 patch.

  Update design D13 with the results before any fix. A change of the Next.js version needs the owner's go-ahead. Evidence: the before table (URL, list request, result per step).

Expected Result: the numbers that the gates compare against exist, and the defect's reach is known before it is fixed.

## 2. Backend

- [ ] 2.1 **`include`, `local_date` and the tie-break** in `routers/transactions.py` (D3, D4).
  - Add `include` (comma list: `daily_totals`, `type_counts`; anything else is 422), `local_date` on each row, and the order tie-break `t.id DESC`.
  - Tests:
    - the transaction-ledger-management scenarios "Day totals add up to the summary", "Split day", "Movement counted once", "Local day boundary", "Tab counts", "Only the owner's rows" and "Unknown include value";
    - "Stable order across pages", with 120 rows sharing `date` and `created_at`;
    - a waiting row counted in its day with `waiting_count`;
    - offsets: the KPI's `outflow` equals the sum of the gross day totals minus `reimbursement_offset`;
    - a response without `include` equal to today's keys plus `local_date`.

  Evidence: pytest counts.
- [ ] 2.2 **New filters:** `type=movement`, `type=investment` and `waiting=true` (D2), with `type=transfer` unchanged. Tests:
  - "Waiting filter matches the badge", against `review_counts`;
  - "Investment tab" after a conversion through `/investment-topups/from-transaction`;
  - the movement filter excludes top-up legs;
  - `type=transfer` returns the same rows as before, checked against a fixture.

  Evidence: pytest counts.
- [ ] 2.3 **`POST /api/capture/parse`** (D11) in `routers/capture.py`, with a read-only preview service over `parse_capture`, owner accounts and aliases, and owner merchant memory.
  - Its own rate-limit bucket (120 per 60 s).
  - The text bounds of `validate_text_request`.
  - Logs with codes only.
  - Tests:
    - the text-capture scenarios "Preview of a short expense", "Missing amount", "Preview writes nothing and asks no model" (statement capture shows no INSERT, UPDATE or DELETE, and the provider mock records no call), "Another owner's memory and accounts", "Bounds and rate limit" (the `/capture/text` bucket is unchanged after 120 previews) and "Preview logs";
    - an ambiguous time that returns `time_options` and a `null` `occurred_at`.

  Evidence: pytest counts.
- [ ] 2.4 **Delete paths behind the drawer** (D10). Tests:
  - deleting a top-up converted from a debit through `DELETE /investment-topups/{id}` restores the expense and the product's value and cost;
  - `DELETE /movements/{id}` on a top-up still answers 409 `investment_topup_lifecycle_required`;
  - deleting a goal-linked or debt-allocated expense reverses its progress, as in the existing tests.

  Evidence: pytest counts.
- [ ] 2.5 **Performance** (D17): the median of 20 `GET /api/transactions` calls without and with `include=daily_totals,type_counts`, the statement count of one request with both includes, and the median of 20 `POST /capture/parse` calls. Evidence: the numbers against the gates and the baseline from 1.1.
- [ ] 2.6 **Full suite:** run the full backend suite on a disposable Flyway-migrated PostgreSQL 16. Evidence: pass, fail and skip counts, each skip with its reason.

Expected Result: day totals, tab counts, the new filters and the preview are correct, owner-scoped and within budget, with every existing contract unchanged.

## 3. Shared pieces

- [ ] 3.1 **`lib/urlState.ts`** (`replaceSearch`, `pushSearch`), and its use in `updateFilters`, the drawer's open and close, and `/laporan`'s one URL write (OD21).
  - Tests: the report-periods-and-drilldown scenarios "Clearing back to the plain ledger" and "Back and reload keep the filters", in Vitest with history mocked.
  - Browser: repeat the steps of 1.2 against the new build.

  Evidence: Vitest counts and the after table next to the before table.
- [ ] 3.2 **Filters in `lib/ledgerFilters.ts`:** the tabs (`movement`, `investment`), `source`, `waiting`, the nav link `/ledger?period=cycle` (OD20), and path-based active state in `SideNav` and `PhoneTabBar`. Tests:
  - "Navigation opens this cycle";
  - "Unknown filter values";
  - every existing `ledgerHref` caller keeps its query (a table test over the call sites in `monthlyReport`, `ObservedInsights`, `EmergencyCoverage`, `insights/page` and `TodoCard`).

  Evidence: Vitest counts.
- [ ] 3.3 **`TransactionDrawer`** in `components/transactions/` (D6–D10) on the `Drawer` primitive: detail, provenance, original text, "Ubah" with the inline form, the movement and top-up modals, the type switch, "Tandai sudah dicek" through `/review/bulk` with the shared `reviewUndo` stack, and delete with its confirmation. Tests:
  - the transaction-ledger-management scenarios: all eight original "Transaction Detail & Inline Editing" scenarios, now in the drawer, plus "Delete asks first", "Deleting a top-up row", "Waiting row points to Tinjau" and "Drawer on a phone";
  - "Confirm and undo", "Undo after a later edit", "Convert to a top-up", "Not eligible for a top-up" and "Pindah saldo needs a pair";
  - a 404 for a deleted row;
  - axe on each state in both themes.

  Evidence: Vitest counts.

Expected Result: one drawer serves every row with the approved actions, and the URL always tells the truth.

## 4. Transaksi

- [ ] 4.1 **Rebuild `app/ledger/page.tsx`** (D1, D2, D5) with the toolbar (tabs with counts, chips, search, "Filter" with account, category and pillar, active-filter chips), the `KpiStrip` (Masuk, Belanja, Bersih, and the offset line), the day-grouped table, "Muat 50 lagi", selection mode with "Jadikan Pindah Saldo", `PendingScheduledBanner` and the recurring rules modal. Tests:
  - the "Searchable & Filterable Ledger View" scenarios, the original four plus "Day heading covers the whole day", "Waiting row is counted in its day" and "Empty result";
  - "Load more announces the rows" and "Load more fails";
  - table semantics: one `tbody` per day with a `th scope="rowgroup"`;
  - no client-side day sums: the heading shows only the date when `daily_totals` is absent (D18).

  Evidence: Vitest counts.
- [ ] 4.2 **Phone feed** (replaces `MobileLedgerFeed`): sticky day headings, rows of 56 px or more, the tab chip row, long-press selection, and the sheet with back closing it. Tests: the phone layout at a mocked container width of 390 px, and "Drawer on a phone". Evidence: Vitest counts.
- [ ] 4.3 **Keys** (D12): J, K and Z as the "Di Transaksi" page group, and ⌘K focusing the page search on `/ledger`. Tests: the "Transaksi Keys" scenarios "Walk the list", "Typing in search" and "Switched off", and the help listing the group only on `/ledger`. Evidence: Vitest counts.
- [ ] 4.4 **Remove what the old page alone used:** the numbered pagination, the edit modal, `components/ui/DetailDrawer.tsx`, and dead helpers. Update `app/ledger/page.test.tsx`. Evidence: `rg` finds no import of the removed code; type-check, lint and build pass.

Expected Result: Transaksi is the approved workspace at every width, and its totals and actions are consistent with the server.

## 5. Catat, Beranda and Tinjau

- [ ] 5.1 **`components/capture/CatatDialog.tsx`** (D11), lazily loaded and opened by "+ Catat", N and the phone tab. It has the live preview with debounce and abort, the refusal without an amount, recording through `/capture/text` with `question_types`, inline questions through `/answer`, the toast with "Urungkan" through `/undo`, and "Isi manual" with the amount prefilled. Tests: every "Web Catat Dialog" scenario, and focus moving into the dialog and back to the opener. Evidence: Vitest counts.
- [ ] 5.2 **Beranda opens the drawer** (D15): `BerandaView` uses `TransactionDrawer`, and `TransactionDetailDialog.tsx` is removed. Tests: the beranda-cycle-summary scenarios "Row opens the transaction drawer" and "Row deleted elsewhere", with the existing "Waiting row in the list" and "Movement row" still passing. Evidence: Vitest counts.
- [ ] 5.3 **Tinjau links and the return** (D14): the links carry `return=review&folder=<key>`, and closing returns to `/review?item=<id>&folder=<key>`. Tests: "Back to the same item", "Item resolved meanwhile" and "Tampered return". Evidence: Vitest counts.
- [ ] 5.4 **Spec tidy:** the notification-purpose-questions delta of this change ("Purpose Answer Undo"). Check that the phase 2 tests behind its scenarios exist and pass, naming them. Evidence: pytest node ids and counts. `openspec validate redesign-transactions-workspace --strict` exits 0. Until the archive, `--all --strict` keeps failing only on the main spec's existing length warning, which the archive removes.
- [ ] 5.5 **Docs:** the Transaksi and Catat parts of `docs/NEW_USER_GUIDE.md` (tabs, chips, day totals, drawer, "Tandai sudah dicek", typed Catat). Evidence: the diff.

- [ ] 5.6 **Copy register sweep** (D20, OD27):
  - Re-run the survey over `frontend/src` (tests excluded) and change every label-type string to the neutral register. The connect-phone guide prose, the AI and suggestion questions, and Beranda's summary sentence keep their voice.
  - Update the tests that quote changed strings, the label quotes in `docs/NEW_USER_GUIDE.md`, and add the copy rule to AGENTS.md.
  - Add `src/copyRegister.test.ts`: it fails on a listed slang token in the label-type sources outside the conversational allowlist.
  - Evidence: the survey before and after (counts), the guard test failing on a planted token and passing after, Vitest counts, and the rename table in verification.md.
Expected Result: recording by text, Beranda's rows and Tinjau's edits all go through the same drawer and capture paths.

## 6. Integration verification

- [ ] 6.1 **Full checks:** run `npm ci`, `npm test`, `npm run type-check`, `npm run lint` and `npm run build` in `frontend`, the backend suite on a disposable Flyway-migrated PostgreSQL 16, `openspec validate --all` (exit 0), `openspec validate --all --strict` (only the main-spec warning that 5.4 fixes may remain before the archive), and `git diff --check`. Evidence: pass, fail and skip counts tied to the commit.
- [ ] 6.2 **Browser matrix** with a seeded synthetic owner (D16, D17) on `/ledger` (list), `/ledger?edit=<id>` (drawer: side, overlay or sheet), the Catat dialog (empty, preview, refused, question) and `/ledger?period=cycle&type=investment`. Check per cell:
  - axe with zero serious or critical violations;
  - measured contrast of tabs, chips, day headings, pills, amounts and captions;
  - no horizontal overflow, no console errors, nothing clipped, and no target under 24 px;
  - one screenshot, read by the agent, with any defect fixed before the task is checked.

  Also run:
  - 200% and 400% zoom at 1280 px;
  - reduced motion;
  - a keyboard-only walk (J/K, Enter, the drawer's controls, Esc, Z, N, ⌘K, ?).

  Evidence: the matrix table and its screenshots.
- [ ] 6.3 **Real-interface flows** on the local build, with values before, after and after undo where one applies:
  1. **Day totals:** the day headings of every loaded page add up to the KPIs for this cycle, also after "Muat lagi" across a split day.
  2. **"Tandai sudah dicek" then Z:** the Belum dicek tab count and the sidebar figures go down and come back.
  3. **Top-up conversion from the drawer:** the RDN Contoh balance is unchanged, the product's value rises, Beranda's saving line rises, and "Urungkan" restores all three.
  4. **Keluar to Masuk and back:** the KPIs move by the amount and return.
  5. **Delete with confirmation:** a goal-linked expense; the goal progress falls by the amount.
  6. **Catat:** "parkir 5rb jam 2" shows the preview, records at 14.00, appears in today's group and the KPIs, and "Urungkan" removes it. "bayar parkir" is refused. A question flow records after the answer.
  7. **Tinjau:** "Ubah" on a "Perlu jawaban" row, save, return to the same item.
  8. **Beranda:** a row opens the drawer, and a delete there updates Beranda.
  9. **Copy register:** screenshots of the shell, Beranda, Tinjau, Transaksi and Catat after the sweep, read for leftover informal labels.

  Evidence: the values and the screenshots.
- [ ] 6.4 **Gates** (D17): the bundle and timing numbers against the baseline. Evidence: before and after.
- [ ] 6.5 **Regression pass:** `/`, `/review`, `/insights`, `/laporan` (including the URL fix), `/accounts`, `/goals` and `/settings` at 1280 and 390 px, on this build and on a build of `origin/main`. Evidence: no new serious or critical axe violation, no console errors, and screenshots read.

Expected Result: phase 3 is proven locally at all five widths in both themes, with money and counts consistent before and after each action and undo.

## 7. Acceptance

- [ ] 7.1 Present to the owner:
  - verification.md;
  - the matrix and flow screenshots, which the agent has read;
  - the preview conflicts;
  - OD20–OD26 and any new decisions;
  - what was not verified.

  The owner accepts on this evidence and does not test in person (proposal "Approval" item 5). Record the acceptance with the owner's words and date in proposal.md, and keep the list of things the owner may want to check at the end of all phases in verification.md. Evidence: the owner's words quoted with the date.

## Workflow follow-up

- **After acceptance:** run `openspec archive redesign-transactions-workspace -y` on `feat/transactions-workspace`, then confirm that `openspec validate --all --strict` exits 0. Push the branch and report the compare link.
- **Merge and release:** the owner opens and merges the pull request, and the release runs only through `deploy.yml`.
- **After the deploy:** with the owner's standing read-only permission, check production. Health returns 200, the containers come from the release, `GET /api/transactions?include=daily_totals` answers, and `/ledger` loads. Then delete the branch locally and on the remote.
- **Next phase:** start phase 4 (`redesign-analysis-and-reports`) from the merged `main`.
