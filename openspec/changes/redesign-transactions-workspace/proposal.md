# Proposal

## Why

Transaksi is the last daily page that still looks and works as it did before the redesign. Rows sit in one long table without day headings, the summary cards are the only totals, editing opens a tall modal with the delete and review buttons at the bottom, and "+ Catat" opens a manual form, although the phone already records from one typed sentence. The approved phase plan (`redesign-web-workspace`, phase P3) replaces this with the combined preview's Transaksi: type tabs, filter chips, day groups with day totals, a detail drawer (a bottom sheet on a phone), and a text-first Catat dialog. Day totals and the live Catat preview need two server contracts, which is why phase 3 gets its own approved plan.

## Approval

Binding earlier approvals:

1. **Phase plan, 2026-11-02.** The owner approved `redesign-web-workspace` with "approved with defaults, start phase 1". Its design is binding for this phase. That covers:
   - the P3 row: tabs, chips, search, the period stepper in the header, day groups with day totals, the drawer or bottom sheet with edit and a type switch including Top up Investasi, provenance, "Tandai sudah dicek" and delete; Beranda's row detail moving to this drawer; and the text-first Catat dialog with a live preview and "Isi manual";
   - the P3 contracts: `daily_totals` in `GET /transactions`, and a deterministic `POST /api/capture/parse` that writes nothing and calls no model;
   - OD6: delete asks for confirmation first, with no undo and no soft delete in this phase;
   - D7 and D8: shared primitives and one shortcut registry;
   - D9: rows waiting in Tinjau stay recorded and counted;
   - D14: no new paths;
   - D17: WCAG 2.2 AA verification.
2. **Phase 1 decisions that apply here.** OD1: the product keeps the name "CashFlow". OD7: waiting items are worded "nunggu jawabanmu", and no wording may suggest that waiting rows are left out of totals.
3. **Combined preview, 2026-11-02.** The owner approved `cash-flow-tracker-prototypes/2026-11-02-web-redesign/combined.html` as the definition of the redesign. Its Transaksi view, its transaction drawer and its Catat dialog are the visual and interaction target. Where the preview conflicts with an approved money rule, the rule wins. Each conflict is recorded in design.md.
4. **Phase 2, 2026-11-03.** The owner accepted `redesign-review-triage` ("accepted, defaults ok"). Its design hands two things to this phase:
   - returning from `/ledger?edit=…&return=review` to the exact Tinjau item;
   - the review undo contract (`undo_token`, `POST /api/review/undo`), which this phase reuses for "Tandai sudah dicek".
5. **Verification by the agent, 2026-11-03.** The owner said "I'm not going to test anything until all phases done implemented properly and approved". The agent's own checks are therefore the whole runtime evidence for this phase: the browser matrix, the real-interface flows, and screenshots the agent has read.
6. **Start of phase 3, 2026-11-03.** The owner said "start phase 3". This authorised planning.
7. **Phase 3 plan and the copy register, 2026-11-03.** The owner approved this plan with "approved, defaults ok": OD20–OD26 take their recommended defaults, except OD25, which the next sentence changes. The owner added: "for all labels shown in the app it should be not that informal terms or language for example "Udah dicek" it's good for onboarding interactive messages, but for label or term used in the UI should not use that kind of terms. I hope you can tell the difference and decide which is when". The lead's decision is design D20 and OD27: labels in neutral standard Indonesian, the casual voice only in conversation, and a sweep of the existing web copy in this phase.

## Expected Outcome

- **Opening Transaksi** shows this cycle (OD20) with the period stepper in the top bar.
  - Tabs: "Semua", "Keluar", "Masuk", "Pindah saldo" and "Investasi", each with its row count.
  - Chips: "Menunggu jawaban", "Belum dicek", "Dari HP" and "Lewat Catat".
  - A search field.
  - The Masuk, Keluar and Bersih figures for the filtered rows.
- **Day groups.** Rows are grouped by day, newest first. Each day heading shows that day's Keluar and Masuk for every matching row of the day, also rows on a page not yet loaded, and how many rows of the day are "menunggu jawaban". The day totals add up to the period's figures.
- **Drawer.** Selecting a row opens a drawer: the third column on a wide window, an overlay on a narrower one, and a bottom sheet on a phone. It shows:
  - the amount, the type and the account;
  - where the row came from ("Dari HP", "Lewat Catat" or "Dicatat manual") and who classified it ("AI", "Kebiasaan" or "Pengguna");
  - the original notification or the typed text.

  From the drawer the owner can:
  - edit the row;
  - switch its type, including "Top up Investasi";
  - mark it "sudah dicek", with "Urungkan";
  - delete it after a confirmation.

  A waiting row shows "Jawab di Tinjau" instead.
- **Catat.** "+ Catat" and N open a dialog with one text field. While the owner types "parkir 5rb jam 2", the dialog shows what it read: the amount, the time, the account and the category when known. It refuses text without an amount and says why. "Catat" records through the same capture path as the phone and asks the phone's follow-up questions inline, with "Urungkan" for 10 seconds. "Isi manual" opens the existing form.
- **Beranda.** Selecting a row in Beranda's "Aktivitas terbaru" opens the same drawer.
- **Back to Tinjau.** "Ubah" on a Tinjau row opens the drawer in Transaksi. Closing it returns to the same Tinjau item.
- **Filters in the URL.** Every filter change, including clearing back to the plain `/ledger`, updates the URL. Reload and back keep the place.
- **Neutral labels.** Every label, heading, button, status, toast and empty state on the web reads in neutral standard Indonesian ("Perlu tindakan", "Semua sudah ditinjau", "Sudah dicek", "HP terhubung"). The casual voice stays in conversation: the connect-phone guide, AI and suggestion questions, and Beranda's summary sentence (D20).
- **Keys and accessibility.** J and K move between rows and Z undoes the last "sudah dicek". The keys work only while focus is in the list, and the existing switch turns them off. `/ledger`, the drawer or sheet and the Catat dialog pass WCAG 2.2 AA checks at 1440, 1280, 1024, 768 and 390 px in light and dark.

## What Changes

- **Transaksi page:** `app/ledger/page.tsx` is rebuilt on the phase 1 primitives, and `MobileLedgerFeed` is redesigned for the phone. The page has tabs, chips, search, the period stepper, day groups, "Muat lagi" (OD22), and a selection mode that keeps "Jadikan Pindah Saldo". Edit, review and delete move from the modal into a shared `TransactionDrawer` on the `Drawer` primitive. The movement and top-up edit modals stay and open from the drawer. The unused `components/ui/DetailDrawer.tsx` is removed.
- **Type switch:** "Keluar" and "Masuk" use the existing update. "Top up Investasi" uses the existing conversion (`POST /investment-topups/from-transaction`), which keeps both legs paired. "Pindah saldo" starts the existing two-row merge with this row selected (OD23).
- **"Tandai sudah dicek":** this uses `POST /api/review/bulk` `accept` for the row, which returns an undo token, instead of `PATCH review_status`. The toast offers "Urungkan". `PATCH` keeps working for the companion.
- **Delete:** asks for confirmation first (OD6). It uses the existing paths: a single row, a movement, or a top-up through its own delete.
- **Day totals:** `GET /api/transactions` gains `include=daily_totals,type_counts`. The `daily_totals` cover every day that the returned page touches, computed over every row matching the filters. The `type_counts` count the rows behind each tab. Each row gains `local_date`. Without `include`, the response is unchanged apart from `local_date`.
- **Catat dialog:** a new text-first dialog, loaded on first open. New `POST /api/capture/parse` returns the deterministic reading of the text and writes nothing. Recording uses the existing `/capture/text`, `/capture/{id}/answer` and `/capture/{id}/undo`, with `question_types` declared. `QuickCaptureModal` stays as "Isi manual".
- **Beranda:** `TransactionDetailDialog` is replaced by the shared drawer.
- **Return to Tinjau:** Tinjau links carry `return=review&folder=<key>`. Closing the drawer goes back to `/review?item=<edited row>&folder=<key>`, and only that return target is accepted.
- **Filter URL defect:**
  - **Defect (reproduced locally):** the first navigation from `/ledger?…` back to plain `/ledger` changes neither the URL nor the list.
  - **Fix:** filter changes write the URL with the browser history API, which Next.js keeps in sync with `useSearchParams`, so no server round trip is needed.
  - **`/laporan`:** it has the same defect, and its one URL write is fixed with the same helper (OD21).
- **Keys:** Transaksi registers J, K and Z as a page group in the shortcut registry, scoped to the list region.
- **Spec tidy:** the description of `notification-purpose-questions` "Answer a Purpose Question" loses its undo sentence, and that behaviour becomes the new requirement "Purpose Answer Undo" with its own scenarios. Every existing scenario stays in place, as the native OpenSpec rule for splitting requires. `openspec validate --all --strict` then passes again.

## Non-goals

- **No undo after delete** and no soft delete (OD6). No undo of edits made in the drawer: the owner edits again.
- **No new transaction types and no new money rules.** Day totals use the same reportable rules as `summary`. Waiting rows are counted, as everywhere.
- **Features of the preview that the app does not have:**
  - CSV export ("Ekspor");
  - client-side keyword guessing of a pillar in Catat;
  - free switching of any row into a "Pindah saldo" without a counterpart row.
- **Catat's preview never asks a model.** Recording through `/capture/text` keeps today's bounded model use for captures, and no new model use is added.
- **Other pages stay as they are:** Analisis, Laporan's layout, Rekening & Kantong, Target & Tagihan and Pengaturan keep their phase 1 state. On Laporan only the URL fix changes (OD21).
- **The phone companion is unchanged.** Its endpoints gain optional fields only.
- **Production.** All verification is local, against a disposable migrated PostgreSQL with synthetic data and a local build. Merging and releasing stay with the owner and the `deploy.yml` pipeline. A read-only production check follows the deploy only on the owner's instruction.

## Capabilities

### New Capabilities

- `ui-copy-register`: the register rule for visible web text (D20), with a regression guard.

### Modified Capabilities

- `transaction-ledger-management`: changes to the ledger view (tabs, chips, day groups with day totals, "Muat lagi", phone feed) and to row detail and editing (drawer or sheet, provenance, type switch, "Tandai sudah dicek" with undo, delete confirmed first). It also gains new requirements for the day totals and type counts contract, the Transaksi keys, returning to the Tinjau item, and the shared drawer on Beranda.
- `report-periods-and-drilldown`: "Ledger Filters in the URL" also covers clearing back to plain `/ledger`, back and reload, and the default period (OD20).
- `text-capture`: a parse-only preview endpoint and the web Catat dialog that records through the existing capture path.
- `beranda-cycle-summary`: selecting a row in "Recent Activity on Beranda" opens the shared transaction drawer.
- `review-queue`, `review-undo`, `history-suggestions`, `mobile-native-layout` and `explainable-figures`: only the quoted labels change to the neutral register ("Belum dikenali", "Semua sudah ditinjau", "Perlu tindakan", "Rincian hitungan"). `beranda-cycle-summary` also changes "Sudah dipakai" and "Perlu tindakan".
- `notification-purpose-questions`: the undo behaviour of "Answer a Purpose Question" becomes its own requirement, "Purpose Answer Undo". No behaviour changes.

Standards that apply:
- web UI and accessibility (WCAG 2.2 AA, including 2.1.4, 2.4.11 and 2.5.8);
- money correctness: day totals equal the summary, conversion keeps paired legs, delete reverses its effects;
- api-consumers compatibility for the companion: additive fields only;
- multi-user isolation;
- personal-data logging: the parse and capture logs hold ids and reason codes only;
- llm boundaries: the preview calls no model.

The capability list in AGENTS.md does not change. AGENTS.md gains the copy rule. No database migration is needed.

## Impact

- **Backend:**
  - `routers/transactions.py`: the `include` parameter, `daily_totals`, `type_counts`, `local_date`, and an `investment` tab filter that adds to the existing type filter (`type=investment`).
  - `routers/capture.py`: `POST /capture/parse` with its own rate-limit bucket.
  - `services/capture_parsing.py` or a small `services/capture_preview.py`: the read-only preview over `parse_capture`, account mentions and owner merchant memory.
  - Tests cover equality with the summary, day splits across pages, filters, owner isolation, the time zone, that parse never writes, the rate limit and the logs.
- **Database:** none. The `idx_transactions_user_date` index from V1 serves the per-day aggregates.
- **Frontend:**
  - `app/ledger/page.tsx` is rewritten.
  - New `components/transactions/*`: the drawer, day groups, rows, toolbar and phone feed.
  - New `components/capture/CatatDialog.tsx`.
  - `lib/ledgerFilters.ts` gains tabs, chips and the default period, and the new `lib/urlState.ts` holds the history-API writer.
  - `app/laporan/page.tsx`: one URL write changes (OD21).
  - Beranda's `BerandaView` uses the drawer, and `TransactionDetailDialog.tsx` and `components/ui/DetailDrawer.tsx` are removed.
  - The Tinjau row links in `components/review/triage/*` change.
  - `AppLayout` opens the Catat dialog.
  - Tests replace `app/ledger/page.test.tsx` and add tests for the drawer, day groups, the dialog, keys and the URL.
- **API consumers:** responses only gain fields. The companion calls `PATCH` and `DELETE /transactions/{id}` and the capture endpoints, all unchanged. It does not call `GET /transactions`.
- **Copy sweep (D20):** strings in `components/beranda/*`, `components/review/*`, `components/workspace/*`, `components/ui/Device*`, `lib/deviceHealth.ts`, `lib/reviewUndo.ts`, `lib/provenance.ts` and other routes' informal strings; a guard test.
- **Docs:** the Transaksi and Catat parts of `docs/NEW_USER_GUIDE.md`, its quoted labels, and the copy rule in AGENTS.md.
