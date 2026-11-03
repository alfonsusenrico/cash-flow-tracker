# Design

## Context

See proposal.md for why and what. This section records the trace of 2026-11-03 that the plan rests on.

- **Base:** branch `feat/transactions-workspace` at `origin/main` 861212b, with phases 1 and 2 merged.
- **Visual target:** the combined preview, `cash-flow-tracker-prototypes/2026-11-02-web-redesign/src/combined/`:
  - `views.js` `viewTransactions` (tabs, chips, day rows);
  - `app.js` `txDrawer`, `saveEdit`, `parseCatat`, `catatPreview` and `catatSave`;
  - `styles.css`.
- Observations come from code and from the local experiments named. Assumptions are marked.

### Transaksi today (observed)

- **The page.** `frontend/src/app/ledger/page.tsx` (2 145 lines) is one client component inside `Suspense`.
  - **Filters:** `parseLedgerFilters` reads the period, offset, category, pillar, type (`expense` or `income`), account, review, `from`/`to`, `q` and `dup` from the URL. `updateFilters` writes them back with `router.replace`.
  - **Data:** it reads `GET /transactions` 50 rows at a time with numbered pages. It sets `logical_movements=true` only when no filter is active.
  - **Layout and summary:** a desktop table and `MobileLedgerFeed` below `lg`. Summary cards show cycle or cumulative figures from `summary`, or the period block when a period or range filters the list. When `summary` is missing it falls back to sums of the loaded page.
- **The edit modal.** Opened by `?edit=<id>` or a row, it holds:
  - provenance (source, review status, classifier, habit basis);
  - "Tandai sudah ditinjau", which sends `PATCH review_status=reviewed`;
  - the Pengeluaran/Pemasukan switch, "Jadikan Top up Investasi" (`ExpenseTopupConversionModal`), amount, account, category, pillar, notes, merchant, date, offset category, goal, debt allocations and receipt;
  - delete with confirmation (`DELETE /transactions/{id}`, or `/movements/{id}` for a movement).

  Movement rows open `InternalMovementModal` instead, and top-up rows open `InvestmentTopupModal`. With `return=review`, closing pushes `/review`. Long-press or selection mode picks two rows for "Jadikan Pindah Saldo" (`POST /movements/merge`).
- **Beranda** opens `components/beranda/TransactionDetailDialog.tsx` for a row of "Aktivitas terbaru": a modal with the amount and delete, labelled as kept "until phase 3 brings the drawer". `components/ui/DetailDrawer.tsx` has no importer.
- **"+ Catat" and N** open `QuickCaptureModal` (905 lines), a manual expense, income or movement form. The web never calls `/capture/*`. The phone does, with `/capture/text`, `/{id}/answer`, `/{id}/undo` and `GET /{id}`.
- **Phase 1 and 2 primitives.**
  - `Drawer`: side column above 1180 px, overlay from 761 to 1180 px, sheet at 760 px and below. The overlay and the sheet trap focus, Esc closes them, and focus returns to the opener.
  - `ToastProvider` with "Urungkan", `ChipGroup`, `Segmented`, `KpiStrip`, `EmptyState`, `ErrorState` and `Skeleton`.
  - The shortcut registry with page groups and the off switch.
  - `TopBarControls`, with the period stepper already in the top bar.
  - Phase 2's `lib/reviewUndo.ts`: an in-memory undo stack per tab.

### Backend contracts (observed)

- **`GET /api/transactions`** (`routers/transactions.py`) builds one `WHERE` from the filters.
  - **First query:** returns `total` and the cumulative and cycle sums with `reportable_income_sql` and `reportable_expense_sql`. Movements, Investasi rows, excluded categories and reimbursement income are not inflow or outflow.
  - **Offsets:** `ledger_cash_flow` subtracts reimbursement offsets from the outflow when the filters equal a report figure. Offsets come from `load_spending_offsets`, which works per category and window and caps the amount by the category's spending, so they cannot be placed on a day.
  - **Second query:** the page, ordered `t.date DESC, t.created_at DESC`. The order has no unique tie-break.
  - **Index:** `idx_transactions_user_date (user_id, date DESC)` exists (V1).
- **Local day:** the backend uses `settings.tz`. The frontend hardcodes `Asia/Jakarta` in a few formatters.
- **Badge rows:** "Waiting" rows are the badge's `ATTENTION_SQL` with `OPEN_QUESTION_JOIN_SQL` and `REVIEWABLE_ROW_SQL` (`services/monthly_report.py`).
- **Capture.** `services/capture_parsing.parse_capture` is deterministic. It returns the amount, an amount issue, the kind, the time reading (`time_facts`), account mentions and a phrase.
  - `/capture/text` runs it, then may ask the model (`provider.normalize_capture`) for category and description under the text-capture rules.
  - It is rate-limited at 30 per 60 s per owner (`capture:user:{id}`).
  - Questions come in the order amount, time, kind, account, category. Time questions need `question_types`.
  - Undo works within 10 s plus 5 s of grace.
- **Companion** (sibling repo, read only): it calls `PATCH` and `DELETE /api/transactions/{id}` and the capture endpoints. It never calls `GET /api/transactions`.

### The filter URL defect (reproduced 2026-11-03)

- **Set-up.** The existing production build in the worktree (`.next` `BUILD_ID` T9MMzx3hrTlTV5UMXJRuU, built from `3145b53`, Next.js 16.2.6) was served on loopback. `/ledger`, `/laporan`, `/insights`, the layout and the lockfile are unchanged between that commit and `main`. Headless Brave over CDP called `window.next.router` (scripts in the session scratchpad, `p3-plan/repro*.mjs`).
- **Fails:** the first navigation from the document's initial URL with a query to the same path without one. Examples: `/ledger?type=income` → `/ledger`, `/laporan?x=1` → `/laporan`, `/insights?x=1` → `/insights`. No RSC request is sent, nothing commits, and the URL and the list stay as they were. `push` behaves the same, and so does a call 200 ms after load (before idle prefetch).
- **Works:**
  - `/ledger` → `?type=expense` → `?type=income` → `/ledger`;
  - `/ledger?type=income` → `?period=cycle` → `/ledger`: once any other navigation has happened, the plain path works;
  - `window.history.replaceState(null, "", "/ledger")` in the failing case.
- **Supported explanation:** a defect in the Next.js client router for this one transition. The page code is not involved, because a direct router call fails the same way. Its internal cause is not established.
- **Not reproduced yet:** phase 1's fingerprint that "every chip and the stepper" fail with a real session. In this run the API was absent and the shell did not render, so sidebar links could not be tested. Task 1.2 re-establishes both with a seeded backend before the fix is chosen.

## Goals / Non-Goals

**Goals:**

- One place to see, correct and confirm any row, the same from Transaksi, Beranda and Tinjau.
- Day totals the owner can trust: they cover the whole day under the active filters and add up to the period's figures.
- Typing a sentence is the fastest way to record on the web. The preview shows what will be read, and nothing is written or guessed until "Catat".

**Non-Goals:**

- No change to what any figure counts, to review states, to movement pairing or to top-up rules.
- No delete undo, soft delete or edit history (OD6).
- No model call outside today's `/capture/text`.

## Decisions

### D1. Page composition

- **Desktop:**
  - The top bar carries the title, the period stepper (existing `TopBarControls`), search and "+ Catat".
  - The page has a toolbar with type tabs (`Segmented`, each with its count), a single-select chip row (`ChipGroup`), a "Filter" button for account, category and pillar, and active-filter chips with remove buttons.
  - Below it, a `KpiStrip` with Masuk, Belanja (OD24) and Bersih for the filtered period, a caption, and the day-grouped table.
  - The drawer opens in the window's third column.
- **Table:** one `<table>` with a `<caption>`. Each day is a `<tbody>` whose first row is `<th scope="rowgroup" colspan>`, holding the day name and the day totals.
  - Columns: Transaksi (avatar, title, sub-line), Jam, Jenis (pill), Kategori (pillar pill), Dicatat (provenance) and Jumlah (semantic colour, `tabular-nums`).
  - Columns hide by container width, as the preview does.
  - Each row's title is a `<button>` that opens the drawer, and the whole row is clickable for the mouse.
- **Phone (container width ≤ 760 px):** a purpose-built feed, not the table.
  - At the top: the search field, the tabs as a scrolling chip row, then the filter chips.
  - The Masuk / Belanja / Bersih figures appear as one compact line.
  - Each day is a section with a sticky heading. Rows are 56 px or taller, with title, sub-line, time and amount.
  - The drawer becomes a bottom sheet. The long-press selection for "Jadikan Pindah Saldo" stays.
- **Kept, restyled on the primitives:**
  - `PendingScheduledBanner` and `RecurringRulesModal`;
  - the "Kemungkinan dobel" (`dup=1`) filter, as a chip shown only when active;
  - the uncategorised filter, under the "Filter" button.

*Rejected:* keeping the legacy table and only restyling it. Day groups and the drawer change the structure, and the restyle-only path was phase 1's compatibility layer.

### D2. Tabs, chips and their URL and API mapping

| UI | URL | API |
| --- | --- | --- |
| Tab Semua | (none) | (none) |
| Tab Keluar | `type=expense` | `type=expense` (existing: expense, not movement or investment or excluded) |
| Tab Masuk | `type=income` | `type=income` (existing) |
| Tab Pindah saldo | `type=movement` | new `type=movement`: rows with category Internal Movement or an excluded category, not part of a top-up |
| Tab Investasi | `type=investment` | new `type=investment`: legs of an `investment_topups` movement, and rows in the Investasi category (trades) |
| Chip Nunggu jawaban | `waiting=1` | new `waiting=true`: `ATTENTION_SQL` with the open-question join, the same rows as the badge |
| Chip Belum dicek | `review=auto` | `review_status=auto` (existing) |
| Chip Dari HP | `source=notification` | `source=notification` (existing) |
| Chip Lewat Catat | `source=capture` | `source=capture` (existing) |

- The API's existing `type=transfer` stays unchanged for compatibility. The web stops using it.
- `parseLedgerFilters` accepts the new values. Unknown values fall back to "all", as today.
- Chips are single-select, as in the preview. The chip row also shows one removable chip each for an active account, category, pillar, range or `dup` filter.
- Tab counts come from `type_counts` (D4). Chip counts are not shown: one more aggregate per chip is not worth its cost (the preview shows them, and this is recorded under "Preview conflicts").

*Rejected:* a separate `chip=` URL key. The existing `review` and `source` keys already mean these filters, and report links already use them.

### D3. Day totals (`include=daily_totals`)

`GET /api/transactions?…&include=daily_totals` adds:

```json
"daily_totals": [
  {"date": "2026-11-02", "count": 7, "inflow": 0, "outflow": 109000, "movements": 500000,
   "waiting_count": 1, "waiting_amount": 45000}
]
```

- **Days covered:** every local day (`settings.tz`) that at least one row of the returned page falls on. Each day's figures cover every row that matches the request's filters on that day, whether or not those rows are on this page. A day split across pages therefore shows the same totals on both pages.
- **Figures:**
  - `inflow` and `outflow` use `reportable_income_sql()` and `reportable_expense_sql()`. That is the summary's gross rule.
  - `movements` adds each matching movement once: the outbound leg, or the only leg present.
  - `count` is the number of matching rows that day.
  - `waiting_count` and `waiting_amount` count the matching rows that are in the badge's attention set.
- **Equality:** over all pages of a filtered window, the sum of `inflow` equals `summary.period.inflow` (or `cumulative.inflow` without a period), and the sum of `outflow` equals `gross_outflow`. The summary's `outflow` is `gross_outflow − reimbursement_offset`. The KPI shows the net figure with the line "sudah dikurangi penggantian Rp X" when X > 0. Day headings show gross amounts, because an offset is capped per category and window and cannot be placed on a day.
- **Implementation:** one grouped statement over the same `WHERE`, limited to the local dates of the page's rows: `(t.date AT TIME ZONE tz)::date = ANY(%s)`. It is served by `idx_transactions_user_date`.
- **Rows gain `local_date`** (`YYYY-MM-DD` in `settings.tz`), so the client groups by the server's day and never converts time zones itself.
- **Order:** the page gets a unique tie-break, `ORDER BY t.date DESC, t.created_at DESC, t.id DESC`, so pages never repeat or skip a row.
- **Waiting rows:** they appear in their day and are counted in that day's `inflow` or `outflow` like any row (D9 of the phase plan). The heading words them "{n} nunggu jawabanmu", never "belum dihitung".

*Rejected:*
- Totals for the whole filtered window: they are unbounded for "Semua waktu", years of days.
- Sums computed on the client from loaded rows: they are wrong for split days, and the phase plan's gate forbids them.
- Placing offsets on days by the income date: it breaks the per-category cap and would not add up.

### D4. Tab counts (`include=type_counts`)

- `type_counts: {"all": n, "expense": n, "income": n, "movement": n, "investment": n}` uses the request's filters except `type`, in one grouped statement.
- A tab with 0 stays visible and selectable, so an empty tab shows its empty state.
- An investment row is counted only under `investment`, never also under `movement`.
- `all` equals `total` when no type filter is active.

### D5. Loading more instead of pages (OD22)

- The list uses `useInfiniteQuery` with 50 rows per request, and "Muat 50 lagi" appends the next page. The phone shows the same button, not infinite scroll, so the tab bar and the footer stay reachable.
- After loading, a polite live region says "{n} transaksi lagi dimuat". Focus stays on the button, or moves to the first new row when the button disappears.
- Rows are de-duplicated by id across pages in case a new row arrives while paging. Any mutation refetches from the first page.
- **A split day:** its heading appears once. When its `count` exceeds the rows loaded so far, the last loaded row of that day is followed by the caption "{k} lagi di hari ini · Muat lagi".
- The URL keeps filters, not the page.

*Rejected:* numbered pages, as today. They cut a day in two with no visible continuation and are slower on a phone.

### D6. The transaction drawer

- **Component and modes:** `components/transactions/TransactionDrawer.tsx` is the shared `Drawer` with the row's title as heading. It is a side column, an overlay or a bottom sheet by width. It loads `GET /transactions/{id}` by id, so Transaksi, Beranda and Tinjau links open the same thing.
- **Detail, top to bottom:**
  - the amount with its type pill;
  - the date and time and the account, with "Dari"/"Ke" for movements and top-ups;
  - "Dicatat": source ("Dari HP", "Lewat Catat", "Dicatat manual", "Terjadwal" and so on) and classifier ("AI", "Kebiasaan" with the habit basis, "Kamu", "Aturan");
  - Kakeibo and category, notes and merchant, the debt and goal links, the receipt;
  - the original notification text or the typed capture text, read from the existing event or capture link where the row has one;
  - "Dicek": "Udah kamu cek" or "Belum".
- **Actions:** "Ubah" (D7), "Tandai sudah dicek" (D9), "Hapus" (D10), and for waiting rows "Jawab di Tinjau" (`/review?item=<id>`).
- **URL and history:**
  - The open row is `edit=<id>`, the existing parameter, so old links open the drawer.
  - Opening from a row on one-column widths pushes a history entry, and back closes the sheet. On wider screens opening replaces the URL.
  - Closing restores focus to the row's button, or to the list heading if the row is gone.
- **Loading and errors:** the drawer shows `Skeleton` while loading, and `ErrorState` with "Coba lagi" on failure. A 404 shows "Transaksi ini udah nggak ada" and clears `edit`.

*Rejected:* keeping the modal. The approved target is the drawer, and a modal hides the list the owner is checking against.

### D7. Editing in the drawer

- **Ordinary rows** edit inline in the drawer with today's fields and validation: amount, account, category and pillar, notes, merchant, date and time, offset category, goal, debt allocations with the existing `DebtAllocationEditor`, and receipt. Saving sends the same `PATCH /transactions/{id}` as today, and companion behaviour does not change.
- **Movement and top-up rows** keep their existing modals, `InternalMovementModal` and `InvestmentTopupModal`, opened from the drawer's "Ubah". Their rules (paired legs, product value and cost) live there, and rewriting them is not in scope.
- **After a save:** the toast "Perubahan disimpan", the views of "Consistent Post-Mutation Refresh" are invalidated, and the drawer stays open showing the saved row.

### D8. Type switch

The drawer's "Jenis" control offers Keluar, Masuk, Pindah saldo and Top up Investasi.

- **Keluar ↔ Masuk:** the existing form switch. The category resets, a goal or debt link is removed when switching to Masuk, and the change is saved with `PATCH`.
- **Top up Investasi:** offered only where `canConvertExpenseToTopup` allows it today (an expense on a liquid account, not linked, with an eligible amount-tracked product).
  - The drawer shows the product picker ("Parent · Product"), then calls `POST /investment-topups/from-transaction`. The existing conversion turns the debit into the outbound leg and creates the inbound leg, so both legs stay paired. The idempotency key and the converted-from snapshot are kept.
  - The response's `undo_token` from phase 2 drives "Urungkan".
  - Unit-tracked funds keep the existing explanation and link.
- **Pindah saldo (OD23):**
  - For an ordinary row, "Pilih pasangannya" closes the drawer and enters selection mode with this row selected, ready for the existing "Jadikan Pindah Saldo".
  - A row that is already an unpaired leg shows "Jawab di Tinjau".
  - No endpoint turns a single row into a movement.

### D9. "Tandai sudah dicek" with undo

- **When:** offered when `review_status` is `auto`, or `needs_review` without an open question and not an awaiting leg.
- **How:** it calls `POST /api/review/bulk` with `{"action": "accept", "transaction_ids": [id]}`, the owner-edit path that phase 2 made undoable.
  - The toast reads "Ditandai sudah dicek" with "Urungkan".
  - Z inside the list or the drawer undoes the newest entry of the shared `reviewUndo` stack.
  - The undo calls `POST /api/review/undo`, and its 404, 409 and 410 messages are reused from Tinjau.
- **Waiting rows** (in the attention set with an open question, or an awaiting leg) are not checked here. They show "Jawab di Tinjau".
- **The wording follows the preview:** "Tandai sudah dicek" and "Udah dicek". The ledger's review filter caption says "Belum dicek" for `auto`. "Sudah ditinjau" remains the status name in provenance (OD25).
- `PATCH review_status` keeps working for the companion and for owner edits.

*Rejected:* keeping `PATCH`. It returns no undo token, so "Urungkan" could not be offered.

### D10. Delete asks first (OD6)

- **Confirmation:** "Hapus" opens an inline confirmation inside the drawer. It names the amount and, where they apply, the effects: "Saldo {akun} balik", "Progres {target} berkurang", "Cicilan {utang} balik ke Rp X". Then come "Hapus" (danger) and "Batal", and focus moves to "Batal".
- **Paths:** the existing ones.
  - `DELETE /transactions/{id}` for a single row.
  - `DELETE /movements/{movement_id}` for a movement.
  - `DELETE /investment-topups/{id}` for a top-up. The movements path answers 409 `investment_topup_lifecycle_required` for a top-up, and today only `InvestmentTopupModal` reaches the top-up path. The drawer chooses the path by `investment_topup_id`, and a test proves the restore of a converted expense.
- **After delete:** the toast "Transaksi dihapus" without "Urungkan", focus on the next row or the list heading, and a refresh as today.
- **Failure:** the reason stays in the drawer in a `role="alert"` region.

### D11. The Catat dialog

- **Component:** `components/capture/CatatDialog.tsx` is a modal dialog, a bottom sheet at 760 px and below. It is loaded with `next/dynamic` on first open (N or "+ Catat") and prefetched on idle after the shell mounts.
- **Field:** one text field, "Tulis aja, misal: parkir 5rb jam 2", 1–200 characters.
- **Live preview:**
  - Typing calls `POST /api/capture/parse` 300 ms after the last keystroke and aborts the earlier request.
  - The preview shows chips for each fact: amount, time, account, and category with pillar.
  - When a fact is missing it shows a plain note: "jam: nanti aku tanya" when the time is ambiguous, "rekening: nanti aku tanya", "kategori: nanti aku kenali".
  - An empty field shows the hint line instead.
- **Contract:**

  ```
  POST /api/capture/parse {"text": "parkir 5rb jam 2", "device_time": "2026-11-02T10:00:00+07:00"}
  → 200 {"ok": true, "preview": {
       "amount": 5000 | null, "problems": ["missing_amount" | "several_amounts" | "unusable_date"],
       "kind": "expense" | "income" | "movement" | null, "description": "Parkir",
       "occurred_at": "2026-11-02T14:00:00+07:00" | null, "time_phrase": "jam 2", "time_options": ["…"],
       "account_hint": {"account_id": "…", "label": "Dompet Digital"} | null,
       "category_hint": {"category_id": "…", "label": "Transportasi", "pillar": "need"} | null}}
  ```

- **Parse rules:**
  - Deterministic only: `parse_capture` for amount, kind and time; account mentions resolved to one owner account only when a single candidate remains; and a category only from the owner's own merchant memory for the parsed phrase.
  - It writes nothing (no capture, no row, no memory, no event) and calls no model.
  - It is bounded like `/capture/text`: 1–200 characters and owner-scoped.
  - Rate limit: its own bucket `capture-parse:user:{id}` at 120 per 60 s, so typing never uses up the submit budget. Over it, the answer is 429 `capture_rate_limited` and the dialog keeps the last preview.
  - Logs hold the user id, outcome and problem codes only.
- **Submitting:** "Catat" is disabled while the preview reports `missing_amount` or `several_amounts`. The note under the field then says: "Jumlahnya belum ada. Aku nggak nebak, tulis jumlahnya ya (misal 5rb)." Otherwise "Catat" sends `POST /api/capture/text` with:
  - a new `client_capture_id`;
  - `device_time`;
  - `question_types: ["amount", "time", "kind", "account", "category"]`.
- **Results:**
  - **`needs_detail`:** the dialog shows the one question with its chips or input, and posts `/capture/{id}/answer` with a fresh `reply_id`.
  - **Recorded:** the dialog closes and the toast "Dicatat: Parkir Rp 5.000, 14.00" offers "Urungkan" (`/capture/{id}/undo`) for the capture's own window of 10 s. After that the button is gone, and the drawer is the way to change the row.
  - **Errors:** a 409 or 410 from undo shows the server's reason.
- **Model use:** recording keeps today's bounded model use of `/capture/text` for category and description. The preview never uses a model (OD26).
- **Fallback:** "Isi manual" opens `QuickCaptureModal`, with the parsed amount prefilled when there is one. The dialog's state is kept until the form saves or is cancelled.

*Rejected:*
- A live preview through `/capture/text`: it writes, and the phase plan forbids that.
- Client-side parsing as in the preview: a second parser would drift from the server's.
- Model calls while typing: cost, privacy, and the phase plan's no-LLM rule.

### D12. Keys

- **Page group:** Transaksi registers "Di Transaksi" with `useShortcut(…, listRegionRef)`.
  - J and K move focus to the next or previous row button, and across day groups.
  - Z undoes the newest "Tandai sudah dicek" or top-up conversion.
- **Native keys:** Enter opens a row natively, and Esc closes the drawer through the layer stack.
- **Rules:** the keys are skipped in fields, the existing switch turns them off, and help lists them while the page is mounted.
- **Global keys stay:** N, ⌘K and ?. On `/ledger`, ⌘K focuses the page's search field instead of navigating.

### D13. URL writes for filters

- **Helper:** new `lib/urlState.ts` exports `replaceSearch(path, params)`, which calls `window.history.replaceState` with Next's state kept. `pushSearch` does the same with `pushState` for the phone sheet.
  - Next.js documents native history calls as supported and synchronised with `useSearchParams` and `usePathname`, and phase 2's Tinjau already uses them.
  - Filter changes need no server round trip, because the page reads every filter on the client.
- **Fixes:** this fixes the reproduced defect and makes filter changes faster. `updateFilters` and `closeEdit` use it.
- **Scope:**
  - OD21 applies the same helper to `/laporan`'s single URL write.
  - `/insights` writes no URL today.
  - If task 1.2 shows that a sidebar `<Link>` to the current path also fails from a query URL, the nav link calls the helper when the target path equals the current one. That is recorded in this design before the change is made.
- **Next.js patch:** where the npm registry is reachable, task 1.2 also tests the reproduction against the newest Next.js 16.2 patch. A version bump is proposed to the owner only if it fixes the defect and every check passes. It is a dependency change and needs their go-ahead. The helper stays either way, because filter writes need no server.

*Rejected:* `router.refresh()` after `router.replace`, which adds a server round trip and does not fix the uncommitted navigation. A hard `location` navigation, which reloads the page and loses state.

### D14. Return to the Tinjau item

- **Links:** Tinjau's row links become `/ledger?edit=<transaction id>&return=review&folder=<folder key>`. The links are built by the triage list, `ReviewGroupCard`, `MovementReviewCards` and the undo failure message.
- **Closing the drawer** (after save, cancel, Esc or delete) with `return=review` navigates to `/review?item=<the edit id>&folder=<folder>`. That is Tinjau's existing deep link, which resolves a transaction id to the item that holds it.
  - Only the literal `review` is accepted as a return target.
  - `edit` must be a UUID and `folder` one of `FOLDER_PARAMS` (`apa`, `jawab`, `pindah`, `saran`, `otomatis`). An invalid folder is dropped.
  - Tinjau then selects the item by its existing deep-link rule, or the next item if the row was resolved meanwhile.
- **Old links** with only `return=review` return to `/review?item=<edit id>`, which is better than today's plain `/review`.

### D15. Beranda opens the same drawer

- `BerandaView` replaces `TransactionDetailDialog` with `TransactionDrawer` for the selected row id, and `TransactionDetailDialog.tsx` is removed.
- The drawer's actions behave as on Transaksi. After an edit or delete, Beranda's queries refresh as today.

### D16. Hidden balances, themes and accessibility

- **Hidden balances:** every amount (rows, day headings, KPIs, drawer, Catat preview, toasts) goes through `bal()`. When balances are hidden, the Catat preview chip shows "Rp •••".
- **Colour and pills:**
  - Semantic colours come from the phase 1 tokens: inflow mint, outflow coral, movement sky, investment lavender, waiting amber.
  - A type is never shown by colour alone: there is always a pill or text.
  - Both themes are checked separately.
- **Targets and focus:**
  - Targets are at least 24 × 24 px (2.5.8). Phone rows are at least 56 px tall.
  - A focus indicator is always visible and is not covered by the sticky day headings or the tab bar (2.4.11). `scroll-margin` is set on rows.
- **Live regions and motion:** load-more, save and delete messages are polite and errors are alerts. Reduced motion turns off the sheet slide.
- **Reflow:** at 400% zoom (320 CSS px) the table becomes the phone feed with no horizontal page scroll.

### D17. Performance and bundle gates

Baselines are measured first, at the plan commit.

- **`GET /transactions` without `include`:** median of 20 calls no more than 10% slower than the baseline, on a synthetic owner with 5 000 rows over 18 months.
- **With `include=daily_totals,type_counts`:** median under baseline + 25 ms. The statement count is fixed at 4: summary, page, days and types, plus the existing allocation lookup.
- **`POST /capture/parse`:** median under 50 ms.
- **`/ledger` first-load JS:** no more than +15 kB gzip over baseline.
- **Shared first-load JS (every route):** no more than +3 kB gzip. The Catat dialog is a separate chunk.

### D18. Failure behaviour

- **List request fails:** `ErrorState` with "Coba lagi". Loaded pages stay when only "Muat lagi" fails, with the error beside the button.
- **`daily_totals` absent** (an older API during a rolling deploy): day headings show the date only, never page sums labelled as day totals.
- **Parse fails or hits 429:** the preview says "Belum bisa baca sekarang". "Catat" stays enabled, because the server decides on submit.
- **Capture submit fails:** the text stays in the field with the server's reason. A retry reuses the same `client_capture_id`, so it is idempotent.
- **Drawer action 409 or 422:** the server's message is shown in the drawer, and the row is refetched.

### D19. Compatibility, migration and rollback

- **No database migration.** Response fields and query values are additive: `include`, `local_date`, `type=movement|investment`, `waiting`. `type=transfer` and every other parameter keep their meaning.
- **Rollback:** revert the pull request. Nothing persistent depends on this phase.

## Preview conflicts (the approved rule wins)

| Preview | Rule | This change |
| --- | --- | --- |
| Waiting rows are "belum dihitung", and the day heading and KPIs leave them out | Phase plan D9, OD7: waiting rows stay recorded and counted | Counted in their day and in the KPIs; worded "{n} nunggu jawabanmu" |
| Transaksi "Keluar" equals Beranda's Keluar, including Tabungan | The ledger summary counts spending only; Beranda's Keluar adds net fresh saving | Labelled "Belanja", with the caption "Di Beranda, Keluar = Belanja + yang ditabung" (OD24) |
| Day "keluar" is one net number | Offsets are per category and window and cannot be placed on a day | Day headings gross; the KPI net, with "sudah dikurangi penggantian Rp X" |
| Any row can switch to "Pindah saldo" | A movement needs two legs; no single-row endpoint | Selection-mode merge (OD23) |
| Catat guesses a pillar from keywords | Amounts and facts are deterministic; category comes from memory or the capture path; no guessing in the preview | Server parse; "kategori: nanti aku kenali" |
| Chip counts | One aggregate per chip per request | Tab counts only |
| "Ekspor" button | Not an existing feature | Not built |

## Open decisions for owner (each with the applied default)

| ID | Question | Recommended default |
| --- | --- | --- |
| OD20 | What opens when the owner goes to Transaksi | **The nav link opens `/ledger?period=cycle` (this cycle, as in the preview).** Plain `/ledger` and every existing link (`?q=`, `?review=auto`, report ranges, `?edit=`) keep their all-time meaning, so no link changes what it shows. The tab bar and sidebar mark Transaksi active by path |
| OD21 | Fix `/laporan`'s URL write with the same helper now | **Yes.** It is the same defect and a one-call change. Laporan's layout waits for phase 4 |
| OD22 | Pages or "Muat lagi" | **"Muat 50 lagi"**, so day groups stay continuous (D5) |
| OD23 | "Pindah saldo" in the type switch for an ordinary row | **Selection-mode merge with this row preselected.** No new endpoint |
| OD24 | Name of the spending figure on Transaksi | **"Belanja"**, with a caption linking it to Beranda's Keluar. The alternative is to compute Beranda's Keluar here, which needs per-day saving figures and is not in this phase |
| OD25 | Wording for confirming a row | **"Tandai sudah dicek" / "Udah dicek"** as in the preview. "Belum dicek" is the chip for automatic rows. "Sudah ditinjau" stays the provenance status name |
| OD26 | What "Catat" records through | **`/capture/text`, the phone's path**, including its existing bounded model use for category. The alternative, a web-only deterministic save, would split the capture rules |

## Risks / Trade-offs

- **The URL defect's internal cause is in Next.js and not established.** The history-API writer avoids the failing transition, and the plan re-checks sidebar links and the newest patch (task 1.2). A future Next.js upgrade may fix it upstream. The helper stays correct either way.
- **"Belanja" on Transaksi and "Keluar" on Beranda differ by design (OD24).** The caption and Beranda's explanation name the difference. The alternative changes the ledger's money rules and is out of scope.
- **A row inserted while the owner pages** shifts offsets. De-duplication by id prevents repeats. A row could be skipped until the next refetch, which every mutation and window focus trigger.
- **The Catat dialog brings typed text to the web.** It is bounded by the same 200-character limit, logs only codes, and the preview never stores anything.
- **Debt carried over:** the capture undo still leaves its `capture` feedback row (phase 2 Risks). Delete has no undo (OD6).

## Migration Plan

None for the database. Deploy is the normal `deploy.yml` release after the owner merges. The frontend and the API ship together. During a rolling deploy, an older API without `daily_totals` is handled by D18.
