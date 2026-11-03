# Proposal: add-tanya-conversation

## Why

The approved web redesign ends with Tanya, the sixth and last phase. Tanya is a page where the owner records a transaction or asks about their money in plain words. Today a typed capture lives only in the Catat dialog (phase 3), and questions such as "how much is safe to spend today?" or "which bills are due this week?" mean opening Beranda, Tinjau or Target & Tagihan one at a time. The combined preview (option B) showed a conversation that answers those questions with the same figures the pages show, and records "parkir 5rb jam 2 tadi" as a transaction. The phase plan scopes it to deterministic answers only. Free-form model answers are a later, separate change with its own prerequisites.

## Approval

Binding earlier approvals:

1. **Phase plan, 2026-11-02.** The owner approved `redesign-web-workspace` with "approved with defaults, start phase 1". The following parts of it are binding for this phase:
   - **The P6 row:**
     - a Tanya page and nav entry;
     - Catat in the composer through the capture endpoints;
     - deterministic intents rendered as cards: cycle summary, waiting items, upcoming bills and Kakeibo;
     - suggestion chips;
     - unknown text answered with the chips and no guess;
     - the conversation kept in memory for the session only;
     - no new endpoint expected;
     - no network call to any model.
   - **The note on `add-tanya-llm-answers`:** free-form answers are a new LLM use with their own prerequisites, and are out of scope here.
   - **D7:** shared primitives.
   - **D8:** one shortcut registry, with an off switch.
   - **D9:** rows waiting in Tinjau stay recorded and counted.
   - **D14:** Tanya gets `/tanya`.
   - **D17:** WCAG 2.2 AA verification.
2. **Combined preview, 2026-11-02.** The owner approved `cash-flow-tracker-prototypes/2026-11-02-web-redesign/combined.html` as the definition of the redesign. Its Tanya view (option B) is the visual and interaction target: the greeting, suggestion cards, the thread with the parkir → 14.00 card, and the composer with the soft yellow glow. Where it conflicts with an approved money rule, the rule wins, as recorded in design.md.
3. **Phase 3 plan, 2026-11-03.** The owner approved `redesign-transactions-workspace` with "approved, defaults ok". This phase reuses the following from it:
   - **Capture:** the text-first capture contract (`POST /api/capture/parse`, parse-only and deterministic; recording through `POST /api/capture/text` with "Urungkan"). OD26: recording keeps the capture's existing bounded model use, and the preview never uses a model.
   - **The shared transaction drawer.**
   - **OD24:** "Belanja" on Transaksi versus "Keluar" on Beranda.
4. **Copy register, 2026-11-03.** The owner said: "for all labels shown in the app it should be not that informal terms or language for example "Udah dicek" it's good for onboarding interactive messages, but for label or term used in the UI should not use that kind of terms. I hope you can tell the difference and decide which is when". The lead's rule (phase 3 design D20, capability `ui-copy-register`) applies here:
   - Tanya's conversation messages are the one place where the casual assistant voice belongs.
   - Tanya's interface chrome stays neutral standard Indonesian.
5. **Phase 0, 2026-11-02.**
   - The product is public and multi-user, and the design is not specific to the owner.
   - The AI asks only when a human would ask.
   - Amounts are never guessed.
6. **Start of planning for phases 4–6, 2026-11-03.** The owner said "yes, plan P4–P6 now". This authorises planning. It does not approve this plan.

Approval of this plan is pending.

## Expected Outcome

- **The Tanya entry:** a "Tanya" entry in the sidebar (group AI, between Harian and Uang), the rail and the phone's Lainnya sheet opens `/tanya`.
- **The opening view:** a greeting with today's safe-to-spend figure and the number of items waiting, four suggestion cards and an empty thread.
- **Questions:** typing "aman hari ini berapa?", "kakeibo siklus ini", "tagihan minggu ini apa aja?" or "apa yang nunggu jawaban?" (or English variants) answers with a card. The card holds the same figures the server gives Beranda and Tinjau.
- **Recording:** typing "parkir 5rb jam 2 tadi" records the transaction through the capture path and shows a card with the amount, time, account and category, plus "Urungkan" while the capture's undo window is open and "Ubah" afterwards. Text without an amount is never recorded. Tanya asks for the amount instead of guessing.
- **Unknown text:** Tanya says it cannot answer that yet, offers the suggestion chips, and makes no guess and no model call.
- **The conversation's lifetime:** it lasts while the tab is open. A reload, signing out or another user signing in starts it empty. Nothing is written to browser storage or logged.

## What Changes

- **Route and nav:** a new route `frontend/src/app/tanya/page.tsx` with a client view in `components/tanya/`. Nav entry in `NAV_GROUPS` (group "AI"), the top bar title and the Lainnya sheet. A new icon in `Icon.tsx`.
- **Intent layer:** `lib/tanya/intents.ts` holds a pure, table-driven matcher (Indonesian and English patterns, an explicit unknown result). A resolver chain in `lib/tanya/resolve.ts` turns text into a reply: intent table, then the capture route, then unknown. The chain is shaped so that a later server resolver can be inserted without rewriting the page.
- **Cards** read existing endpoints only:
  - `GET /api/dashboard/cycle-summary` (cycle, Kakeibo, bills);
  - `GET /api/review/queue` and `/api/review/summary` (waiting);
  - `GET /api/reports/monthly/latest` (latest report link).
  - Each card stores a snapshot of the figures at the moment it answered, stamped with the time.
- **Capture in the composer:** reuses phase 3's capture client. The capture types and calls used by `CatatDialog` move into a shared `lib/capture.ts`, so both use one implementation. Follow-up questions (`needs_detail`) are answered inside the thread through `POST /api/capture/{id}/answer`. "Ubah" opens the shared transaction drawer.
- **Thread store:** `lib/tanya/thread.ts` is an in-memory store tagged with the signed-in user id, capped at 100 messages, with "Hapus percakapan". There is no `localStorage`, `sessionStorage` or IndexedDB.
- **Accessibility:** the thread is a `role="log"` region, so new replies are announced politely, each with a one-sentence spoken summary of its card. Focus stays in the composer after sending. Enter sends and Shift+Enter adds a line, listed in the help under "Di Tanya".
- **Phone:** at 760 px and below, a compact greeting, a horizontally scrolling suggestion row, the thread, and the composer kept above the tab bar.
- **Docs:** `docs/NEW_USER_GUIDE.md` gains a Tanya section. The specs change: `tanya-conversation` (new) and `executive-sidebar-layout` (Tanya entry, route and Lainnya list).

## Non-goals

- **Model calls from Tanya:** free-form or model answers of any kind belong to `add-tanya-llm-answers`. Tanya sends no text to a model and adds no prompt. Capture recording keeps the existing server-side bounded model use of `/capture/text` (phase 3 OD26). That is the capture feature, not a Tanya answer.
- **Answering Tinjau items inside Tanya:** the waiting card links to the exact Tinjau item instead (OD62).
- **A new backend endpoint, a migration or new persisted data.**
- **Conversation history across reloads or devices,** and any server-side storage of what the owner typed.
- **Voice input, attachments, push or proactive messages,** and a Tanya entry on the phone tab bar itself.
- **Per-pillar comparison with the previous cycle in the Kakeibo card** (OD61). Analisis (phase 4) owns that view.
- **The Android companion.**

## Capabilities

### New Capabilities

- `tanya-conversation`: the Tanya page, the deterministic intent table, the card contracts, capture in the composer, the session-only thread, the copy register inside Tanya, and its failure and privacy rules.

### Modified Capabilities

- `executive-sidebar-layout`:
  - the sidebar lists the Tanya link (`/tanya`) in group AI;
  - the phone Lainnya sheet lists Tanya;
  - `/tanya` joins the routes kept working.

No new project capability is added. Of the capabilities listed in `AGENTS.md` (`database`, `deployed`, `money`, `personal-data`, `background-jobs`, `api-consumers`, `multi-user`, `llm`), this change touches:
- `money` (figures shown, never computed on the client);
- `personal-data` (typed text held in memory only);
- `multi-user` (thread reset per user);
- `llm`, by explicitly not using it.

## Impact

- **Frontend, new:**
  - `app/tanya/page.tsx`;
  - `components/tanya/*` (view, thread, cards, composer, CSS module);
  - `lib/tanya/intents.ts`, `lib/tanya/resolve.ts` and `lib/tanya/thread.ts`, with tests;
  - `lib/capture.ts` (moved from `CatatDialog`).
- **Frontend, shared edits:**
  - `components/workspace/navigation.ts`;
  - `components/ui/Icon.tsx`;
  - `components/capture/CatatDialog.tsx` (imports the moved client);
  - `components/workspace/workspace.test.tsx`;
  - the phase 3 copy-register test.
  - The full list is in design.md D15.
- **Backend:** none. Contracts used are unchanged, so the companion app is unaffected.
- **Dependencies:** none added. No migration. Rollback is reverting the pull request.
- **Ordering:** depends on phase 3 (`redesign-transactions-workspace`) being merged, for `POST /api/capture/parse`, the capture client and the shared drawer. It can be implemented in parallel with phase 4 after that, because it touches different pages.
