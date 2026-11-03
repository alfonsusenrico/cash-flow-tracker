# Design: add-tanya-conversation

## Context

Phase 6 of the approved web redesign adds Tanya. The phase plan
(`archive/2026-11-02-redesign-web-workspace/design.md`, row P6) fixes its scope:
- a page and a nav entry;
- Catat in the composer through the capture endpoints;
- deterministic intents rendered as cards (cycle summary, waiting items, upcoming bills, Kakeibo);
- suggestion chips;
- unknown text answered with the chips and no guess;
- the conversation kept in memory for the session only;
- no new endpoint and no network call to any model.

Free-form answers are a later change, `add-tanya-llm-answers`, which needs before any code: an eval set with refusal cases, numbers only from deterministic tool results, a cost budget with a per-user limit, a privacy statement with redaction and retention, provider mocking in tests, and owner approval of provider and spend.

### What exists (observed on origin/main `861212b` and the phase 3 branch `feat/transactions-workspace`)

- **Shell.**
  - `components/workspace/navigation.ts` holds `NAV_GROUPS` (Harian, Uang, Laporan), `SETTINGS_LINK`, `PHONE_TAB_HREFS` and `PAGE_META`. A comment there reserves the AI group for Tanya.
  - `PhoneNav.tsx` builds the Lainnya sheet from every `NAV_GROUPS` link outside the tab bar, so a new link appears there without editing that file.
  - The specs `tactile-pulse-ui` and `executive-sidebar-layout` already order the groups Harian, AI (Tanya, "once it exists"), Uang, Laporan.
  - `workspace.test.tsx` asserts that no Tanya link exists yet.
  - Phase 3 changes `navigation.ts` (`LEDGER_NAV_HREF`, `isActivePath` ignoring the query, neutral subtitles).
- **Data already loaded by the shell** (`components/layout/AppLayout.tsx`):
  - `GET /auth/me` (user id, `name`, `username`);
  - `GET /review/summary` (`attention`, `unreviewed`);
  - `GET /reports/monthly/latest` (`ready`, `period`, `url`);
  - `GET /recurring`.
  - Sign-out calls `POST /auth/logout`, then `window.location.replace("/auth/login")`, a full page load.
- **`GET /api/dashboard/cycle-summary`** (key `dashboard.cycleSummary`, type `components/beranda/types.ts`). One read-only snapshot with:
  - `period`;
  - `safe_to_spend` (`available` with `per_day` and `days_left`, or `unavailable` with a reason);
  - `keluar` (`total`, `offsets_total`, lines per Kakeibo bucket including `unclassified`);
  - `masuk`;
  - `waiting` (count, total, lines);
  - `scheduled` (plan and goal lines due in the window up to the expected salary or the next 30 days);
  - `remaining`, `net_worth` and `comparison` (`keluar`, `masuk`, `belanja`).
  - It fails whole rather than partially.
- **Tinjau:** `GET /api/review/queue` (key `review.queue`) and `lib/triage.ts` (`buildTriage`, `neededCount`, `itemTitle`, `itemTransactionId`, `triageSearch`). These give the items the badge counts and their deep links `/review?item=<id>&folder=<key>`.
- **Capture**, phase 3 (`components/capture/CatatDialog.tsx`, design D11):
  - `POST /api/capture/parse` is deterministic, writes nothing, calls no model, and has its own bucket of 120 per 60 s.
  - `POST /api/capture/text` takes `client_capture_id`, `device_time` and `question_types`, and returns `recorded` or `needs_detail` with one question.
  - `POST /api/capture/{id}/answer` takes a fresh `reply_id`.
  - `POST /api/capture/{id}/undo` works inside `undo_until`, about 10 s.
  - The capture types and helpers (`deviceTime`, `newId`, `timeText`, `undoStillOpen`) live inside `CatatDialog.tsx`.
  - Recording keeps the capture's existing bounded server-side model use for category and description (OD26).
- **Phase 3 also adds:**
  - the shared `TransactionDrawer`, opened from Beranda, Transaksi and Tinjau;
  - `lib/urlState.ts`;
  - the copy register (design D20, capability `ui-copy-register`).
- **Preview (option B)**, `src/combined/views.js` `viewAsk` and `askCards`, `app.js` `askReply`:
  - a large greeting "Sore, Raka." and a lead line;
  - four suggestion cards;
  - a "Hari ini" separator;
  - a thread of user bubbles and assistant messages with cards (cycle, Kakeibo with last-cycle marks, bills, a recorded capture with "Ubah", an "Apa ini?" question with answer buttons);
  - a sticky composer with a soft yellow radial glow (`--glow`), placeholder "Catat atau tanya apa aja…" and an example caption;
  - regex-based intent matching.

## Goals / Non-Goals

**Goals**
- A Tanya page that answers four question kinds and a latest-report link with the server's own figures. It records typed transactions through the existing capture path and refuses to guess.
- An intent layer that is a pure, tested table. A later change can add a server resolver to it without a rewrite.
- Thread content held only in memory, cleared on reload and per user.
- WCAG 2.2 AA at the five widths in both themes. Replies are announced, and focus stays where the user types.
- Additive: a new route and component folder, and only small edits to shared files, so phase 4 can run in parallel.

**Non-goals**
- Model answers, server storage of typed text, and a new endpoint or migration.
- Answering Tinjau items inline (OD62).
- Per-pillar previous-cycle marks (OD61).
- A phone tab-bar slot for Tanya.
- The companion app.

## Decisions

### D1. Route, nav entry and title

- **Route:** `/tanya` (phase plan D14) is a client page inside the workspace shell.
- **Nav entry:** `NAV_GROUPS` gains `{ label: "AI", links: [{ href: "/tanya", label: "Tanya", icon: "message-circle" }] }` between Harian and Uang. The order is already fixed by `tactile-pulse-ui`.
- **Icon:** `Icon.tsx` gains `message-circle`, the Lucide path, with no new dependency.
- **Phone:** the Lainnya sheet lists Tanya automatically, because it is built from `NAV_GROUPS`. The tab bar keeps Beranda, Tinjau, Catat, Transaksi and Lainnya.
- **Title:** `PAGE_META` gains `["/tanya", { title: "Tanya", subtitle: "Catat transaksi atau tanyakan ringkasan keuangan" }]`. The subtitle is a label, so it is neutral (D10).

*Rejected:*
- A sixth tab on the phone bar: five is the approved bar, and Catat is already one tap.
- A floating chat bubble on every page: it would add code to every page's first load and compete with "+ Catat".

### D2. Resolver chain and intent table

`lib/tanya/resolve.ts` turns one sent text into one reply through an ordered chain of resolvers. Each resolver returns a reply or passes:

1. **Normalise.** Trim, lower-case with `id-ID` rules, collapse whitespace, strip trailing punctuation except "?", and cap the length at 200 characters as capture does. Text that is empty after trimming is not sent.
2. **Intent table** (`lib/tanya/intents.ts`). This is a pure `matchIntent(normalised) → IntentMatch | null`, made of ordered entries `{ intent, patterns: RegExp[], params? }` with word-boundary patterns. The first entry with any matching pattern wins.

   | Intent | Indonesian patterns (examples) | English patterns (examples) | Params |
   | --- | --- | --- | --- |
   | `cycle` | aman (hari ini), sisa (uang), ringkas(an), siklus (ini), posisi (uang\|keuangan), bisa pakai berapa | safe to spend, summary, how much left, this cycle | — |
   | `waiting` | nunggu\|menunggu (jawaban), tinjau, yang perlu (di)jawab, belum dikenali | waiting, to review, pending questions | — |
   | `bills` | tagihan, langganan, cicilan, jatuh tempo, setoran | bills, due, upcoming payments, subscriptions | `range`: `week` when "minggu ini", "7 hari" or "this week" matches, `window` otherwise |
   | `kakeibo` | kakeibo, pilar, kebutuhan, keinginan, budaya, tak terduga, ke mana uang | kakeibo, pillars, needs and wants, where did my money go | — |
   | `report` | laporan, rapor bulanan | report, monthly report | — |
   | `help` | bantuan, bisa apa, contoh | help, what can you do | — |

   - Patterns are tested both ways in the table tests: every listed phrase matches its intent, and a set of near misses matches nothing.
   - "bayar" is deliberately absent from `bills`, so "bayar listrik 200rb" reaches capture.
3. **Capture route.** If no intent matched, the text goes to `POST /api/capture/parse` through the shared client (D6). What happens next depends on the result:

   | Parse result | What Tanya does |
   | --- | --- |
   | Exactly one amount and no blocking problem | The text is recorded (D6) |
   | `missing_amount` or `several_amounts`, and the parse found something to record (`kind` or `description` not null) | Tanya asks for the amount: "Jumlahnya berapa? Aku nggak nebak. Tulis juga angkanya, misalnya \"parkir 5rb jam 2\"." |
   | Anything else | Falls through to unknown |

   A question marker blocks the capture route even when an amount is present: a trailing "?", or a leading "berapa", "apa", "kapan", "gimana", "bagaimana", "how", "what" or "when". So "berapa keluar 2 hari ini?" is never recorded.
4. **Unknown.** Tanya replies "Aku belum bisa jawab itu. Coba salah satu ini:" with the suggestion chips (D4). It makes no guess, writes nothing and sends no further request.

- **Mixed text:** when an intent matched and the text also holds a digit, the reply adds the chip "Catat sebagai transaksi". It sends the same text straight to the capture route, so "tagihan listrik 200rb" can still be recorded on purpose.
- **Amounts are never guessed:** the client never extracts an amount itself. It only reads `preview.amount` from the server parser, the one parser (phase 3 D11 rejected a second, client-side parser).
- **Extension point:** a resolver has the signature `(input: ResolverInput) => Promise<Reply | null>`. `add-tanya-llm-answers` can insert a server resolver between the capture route and unknown, returning only the same card kinds built from deterministic tool results. The page, thread and cards stay unchanged.

*Rejected:*
- Parsing first and the intent table second: questions containing numbers would be sent to capture more often.
- Fuzzy matching or a scoring classifier: harder to test and to explain, and it guesses.
- Recognising intent on the client from the parse response: it couples two contracts for no gain.

### D3. Card contracts

Each reply is `{ id, role: "assistant", at, text?, card?, chips? }`. A card holds a snapshot of the figures it was built from, plus `as_of`. It is shown with "Per 14.03" and a "Perbarui" button that rebuilds the card in place from a fresh fetch. A snapshot keeps an old answer true to the moment it was given. Live-updating earlier messages would make the thread contradict itself.

| Card | Source (query key) | Shows | Links |
| --- | --- | --- | --- |
| `cycle` | `GET /dashboard/cycle-summary` (`dashboard.cycleSummary`) | **Aman dipakai hari ini:** `per_day` with `days_left` when available; otherwise the unavailable reason worded as on Beranda, never 0. **Keluar**, **Masuk**, **Kekayaan bersih**. **Menunggu jawaban:** count and total, labelled as already inside Keluar per D9, never "belum dihitung". **Siklus** label and day | "Buka Beranda", "Lihat transaksi" (`LEDGER_NAV_HREF`) |
| `kakeibo` | same | Keluar lines per bucket in Beranda's order, including **Belum diklasifikasi** with its dashed marker and the reimbursement offset line when `offsets_total > 0`. The total equals `keluar.total`. The `comparison.belanja` sentence appears when `comparison.status = available` | "Buka Analisis" |
| `bills` | same, `scheduled` | Plan and goal lines in the window, or only those due within 7 days of `period.today` for `range=week`, each with label, date and amount, and the sum of the shown lines. The heading names the window: "Sampai gaji berikutnya" or "30 hari ke depan" from `window_basis` | "Buka Target & Tagihan" |
| `waiting` | `GET /review/summary` (`review.summary`, shell cache) for the count; `GET /review/queue` with `buildTriage` for items | **N perlu ditinjau**, where N is the badge's `attention`. Up to 3 items with `itemTitle`, amount and folder label, each linking to `/review?item=<id>&folder=<key>`. With N = 0: "Tidak ada yang perlu ditinjau." | "Buka Tinjau" |
| `report` | `GET /reports/monthly/latest` (`reports.latest`, shell cache) | The latest report's label and period when `ready`, otherwise "Laporan berikutnya belum siap." | the report `url` |
| `capture` | the capture response (D6) | Description, amount, time, account, and category with pillar, or "Menunggu jawaban" when the row went to Tinjau. A status pill "Dicatat" or "Masuk Tinjau" | "Urungkan" while `undo_until` is open, "Ubah" (shared drawer) |

- **Rules for every card:**
  - The client does no money arithmetic beyond formatting server values and summing the lines it shows for `range=week`. That sum uses the same integers the server returned. All amounts go through `fmtMoney` or `bal`, so "Sembunyikan saldo" hides them.
  - Hidden balances also hide amounts in assistant text.
- **Greeting** (conversational, D10): "Pagi/Siang/Sore/Malam, {name}." from the device's local hour (04–10, 11–14, 15–18, 19–03). The name comes from `/auth/me` `name`, then `username`, then no name. A lead line follows: "Aman dipakai hari ini {per_day} · {n} hal nunggu jawabanmu." When safe-to-spend is unavailable, the line names the remaining amount instead, as Beranda does.

*Rejected:*
- New aggregate endpoints for Tanya: the phase plan expects none, and the cycle summary already has every figure.
- Live cards.
- Showing waiting money as "belum dihitung", as the preview does: it contradicts D9.

### D4. Suggestion cards and chips

- **Above the empty thread,** four suggestion cards. Each one sends its fixed query text, so it goes through the same resolver and the same tests:
  - "Ringkasan siklus ini" (Aman, Masuk, Keluar);
  - "Menunggu jawaban (N)";
  - "Laporan terbaru";
  - "Tagihan minggu ini".
- **After the first message,** the cards collapse into a chip row under the latest reply.
- **The unknown reply** repeats the chips.
- **Wording:** card and chip labels are labels, so they are neutral (D10).
- **Counts** come from the shell's cached summary. A count that has not loaded yet is left out of the label, never shown as 0.

### D5. Thread, focus and announcements

- **Thread markup:**
  - The thread is an `ol` inside a `section` with `role="log"` and the label "Percakapan". `log` implies `aria-live="polite"`.
  - Each message is an `li`, with a visually hidden speaker and time label: "Pesan terkirim, 14.03" or "CashFlow, 14.03". The assistant's name is `PRODUCT_NAME`.
  - Each card has a one-sentence `aria-label` summary, for example "Aman dipakai hari ini Rp 232.000, Keluar Rp 3.688.680, 3 menunggu jawaban". The announcement is short and the card's table is still reachable.
- **Focus:**
  - After sending, the composer is cleared and keeps focus, so the user can keep typing.
  - New content is announced, not focused.
  - Shift+Tab from the composer reaches the latest message's buttons.
  - "Urungkan" on a capture card returns focus to the composer and announces "Catatan dibatalkan.".
  - Following a link leaves the page normally.
- **Scrolling:** the view scrolls to the newest message unless the user has scrolled up more than 120 px, in which case a "Pesan baru" button appears. Smooth scrolling is off under reduced motion.
- **Keys:** Enter sends and Shift+Enter adds a line. Both are native-like and active only in the composer. They are listed in the help under the page group "Di Tanya" through `usePageShortcutGroup`. They are not single-key shortcuts, so the off switch does not apply. Global N, ⌘K and ? keep working. N is skipped inside the composer as in every field.
- **Sending state:** "Kirim" is a button with `aria-label="Kirim"` and a 44 px target. It is disabled while a reply is pending. The pending reply shows a typing placeholder with `aria-busy` and no announcement until it resolves.

*Rejected:*
- Moving focus to each new reply: it breaks typing flow and repeats long cards to screen readers.
- `aria-live="assertive"`: it interrupts the user.

### D6. Capture in the composer

- **Shared client:** the capture types and calls used by `CatatDialog` move to `lib/capture.ts`:
  - the types `CapturePreview`, `Capture` and `CaptureQuestion`;
  - `parseCapture(text, signal)`, `submitCapture(text)`, `answerCapture(id, answer)` and `undoCapture(id)`;
  - the helpers `deviceTime`, `newId`, `timeText` and `undoStillOpen`.
  - `CatatDialog` imports them, with no behaviour change, proven by its existing tests passing unchanged.
- **Recording on send (OD60):** when the capture route resolves to "record", Tanya calls `POST /capture/text` at once with a new `client_capture_id`, `device_time` and `question_types: ["amount", "time", "kind", "account", "category"]`. Sending the message is the confirmation, as on the phone's text capture and in the preview.
  - **Misrouting** is limited by the question-marker rule (D2), the parse checks and "Urungkan".
  - **Idempotency:** the `client_capture_id` is generated once per sent message, so a retry after a network error never records twice.
- **`needs_detail`:**
  - The server's question prompt appears as an assistant message. It is server text, so it is conversational. Its options appear as choice buttons, plus a short text input when `allows_text`.
  - An answer posts `/capture/{id}/answer` with a fresh `reply_id`, and the card updates in place.
  - Typing a new message instead leaves the question open, with "Jawab nanti di Tinjau" shown under it.
- **Recorded:**
  - The capture card (D3) appears.
  - The queries under `dashboard.all`, `transactions`, `review.summary` and `review.queue` are invalidated as the Catat dialog does.
  - "Urungkan" shows while `undoStillOpen(undo_until)`. A 409 or 410 from undo shows the server's reason as a neutral notice.
  - "Ubah" opens the shared `TransactionDrawer` for the recorded row.
- **Errors:**
  - **429 `capture_rate_limited`:** the notice "Terlalu banyak catatan dalam satu menit. Coba lagi sebentar lagi.", and the text stays in the composer.
  - **Network failure:** the message is marked "Gagal terkirim" with "Kirim ulang", which reuses the same `client_capture_id`.
- **Model use:** Tanya's answering never calls a model, and parse never does. Recording keeps `/capture/text`'s existing bounded server-side model use for category and description (phase 3 OD26, OD66). The browser itself only ever calls same-origin `/api/*`.

*Rejected:*
- A "Catat?" confirmation card before every recording: one more step on the commonest action, for a misroute that the question-marker rule and Urungkan already cover. It is kept as the alternative in OD60.
- Answering through a separate Tanya endpoint: a new endpoint, against the phase plan.

### D7. Thread store and privacy

- **The store:** `lib/tanya/thread.ts` is a small external store used through `useSyncExternalStore`, not a dependency. It holds `{ userId, messages }`.
- **Lifetime:**
  - It lives in module memory. It survives in-app navigation within the tab and is gone on reload or tab close.
  - Sign-out already performs a full page load, so the store is gone then too.
  - The store resets whenever the signed-in user id differs from `userId`, for example on a session change without a reload.
- **Limits:**
  - At most 100 messages; the oldest drop first.
  - "Hapus percakapan" in the page header empties the store after a confirm dialog.
- **Never persisted:**
  - The store never touches `localStorage`, `sessionStorage`, IndexedDB, cookies or the URL.
  - Typed text is not put in query strings, analytics or logs.
  - The resolver never sends unknown text anywhere: the only request made for unmatched text is `POST /capture/parse`, which the backend handles without logging the text (phase 3 D11).
- **Multi-user:** each card fetch goes through the session-scoped API, so a user only ever sees their own figures.

*Rejected:*
- `sessionStorage` to survive reloads: the phase plan says memory only, and text and figures would then sit on disk on shared devices.
- A React context in the workspace provider: it would edit a shared file for no gain over a module store.

### D8. Page composition

- **Wide (≥1181 and the rail at 761–1180):** a single centred column with a maximum width of 760 px inside `main`. From the top:
  - the greeting (`h1`, 44 px, 34 px under 1180);
  - the lead line;
  - a 2×2 grid of suggestion cards (four in a row from a 1100 px container);
  - the "Hari ini" separator;
  - the thread;
  - the composer.
- **Message shapes:**
  - User messages are right-aligned bubbles of at most 78% width.
  - Assistant messages have the product mark, then text and card.
  - Cards use the `Card` primitive vocabulary and `tabular-nums`.
- **Composer:**
  - It is sticky at the bottom of the main column, in a wrapper with a soft radial glow.
  - It holds a two-line auto-growing textarea (up to 6 lines), a caption with two examples, and the send button.
  - The glow is a module-local custom property per theme: light `rgb(250 222 90 / .38)`, dark `rgb(214 186 60 / .20)`, from the preview. It is decorative, behind the composer only, and has no animation.
- **Phone (≤760):**
  - The greeting is 28 px.
  - The suggestion cards become one horizontal row of chips in an `overflow-x: auto` strip, with a scroll snap. The strip scrolls; the page never overflows.
  - Messages may be up to 88% wide.
  - The example caption is hidden, and the placeholder carries one example.
  - The composer sits at `bottom: calc(72px + env(safe-area-inset-bottom))`, above the tab bar. The thread gets matching bottom padding, so the last message and focused buttons are never hidden behind the composer or the tab bar (2.4.11).
  - On browsers with `visualViewport`, the composer stays above the on-screen keyboard. This cannot be proven in headless Chrome and is recorded as not verified.
- **Styles:** a CSS module `components/tanya/Tanya.module.css` using existing tokens. `globals.css` and `tokens.css` are not edited.

*Rejected:*
- Full-bleed chat across the main column: lines grow too long to read at 1440 px.
- Editing `tokens.css` for one glow token: it conflicts with phase 5's alias removal for no gain.

### D9. Failure behaviour

| Situation | What Tanya shows |
| --- | --- |
| A card's fetch fails | The card is replaced by `ErrorState`: "Data ini tidak dapat dimuat." with "Coba lagi" (refetch, card rebuilt in place). Never a figure of 0 |
| `safe_to_spend.unavailable` | Beranda's reason wording, and the remaining amount where Beranda shows it |
| `scheduled.lines` empty | "Tidak ada tagihan atau setoran dalam periode ini." |
| `waiting` with an unloaded count | The label without a count, and the card loads the queue |
| `/capture/parse` fails (network or 5xx) | "Pesan ini tidak dapat diproses. Coba lagi.", nothing recorded, the text kept |
| `/capture/parse` returns 429 | The same 429 notice as capture; nothing recorded |
| `/capture/text` fails after send | D6 "Gagal terkirim" and resend with the same `client_capture_id` |
| A 401 anywhere | The app's existing redirect to sign-in. The thread is then gone with the page |
| Nothing loads at all (offline) | The composer stays usable. Each card shows its own error with retry |

### D10. Copy register inside Tanya

The phase 3 rule applies element by element. Tanya is where the assistant speaks, so only what the assistant says in the conversation is casual:

| Element | Register | Why |
| --- | --- | --- |
| Assistant message text (greeting, lead line, replies, "Aku belum bisa jawab itu…", "Jumlahnya berapa? Aku nggak nebak…") | Casual assistant voice | The assistant speaking in a conversation, the one place the owner kept the voice |
| Server question prompts in `needs_detail` | As the server wrote them | Conversational server text (`ui-copy-register`) |
| Page title "Tanya", subtitle, "Hapus percakapan", "Kirim", "Perbarui", "Coba lagi", "Kirim ulang", "Pesan baru" | Neutral | Buttons and titles |
| Suggestion card and chip labels ("Ringkasan siklus ini", "Menunggu jawaban (3)", "Tagihan minggu ini", "Catat sebagai transaksi") | Neutral | Labels of actions, not speech |
| Composer label and placeholder ("Tulis transaksi atau pertanyaan", "Contoh: kopi 18rb tadi pagi") | Neutral | Form label and placeholder |
| Card headings and figure names ("Aman dipakai hari ini", "Keluar", "Menunggu jawaban", "Sampai gaji berikutnya", "Dicatat", "Masuk Tinjau") | Neutral | Figure names and status pills |
| Notices and errors (429, failure, "Gagal terkirim", "Catatan dibatalkan.") | Neutral | System feedback |
| Speaker labels ("Pesan terkirim", "CashFlow") | Neutral, impersonal | Labels. No "Kamu" as the preview had |
| User's own text, and category, account and merchant names | Unchanged | User data |

- **The guard:** phase 3's copy-register test gains `components/tanya/` with an allowlist. It allows only the assistant-message string module `lib/tanya/replies.ts`, where every conversational string lives. Every other Tanya source is held to the neutral token list.

### D11. Bundle and performance gates

- **Baseline first:** before any change, the `next build` first-load JS of every route and the shared chunk, as phase 2 did.
- **`/tanya` route JS:** at most 30 kB gzip on top of the shared chunk. The route is code-split by Next automatically.
- **Code every page loads** (shared chunk, layout): at most +1 kB gzip. Only the nav entry and the icon path are added there.
- **No new dependency.**
- **Speed:** intent matching under 1 ms for a 200-character text (unit benchmark), and the parse call is debounced to one per send. A card renders from the cache without a request when the shell already holds fresh data (`staleTime` as Beranda).

### D12. Tests

- **Intent table** (`lib/tanya/intents.test.ts):
  - every listed Indonesian and English phrase hits its intent, including the `range=week` parameter;
  - near misses ("bayar listrik 200rb", "kopi 18rb", "halo") hit nothing;
  - question markers block the capture route;
  - normalisation of case, spacing and punctuation;
  - the 200-character cap.
- **Resolver** (`resolve.test.ts`, API mocked):
  - each route;
  - unknown text yields the unknown reply after exactly one `POST /capture/parse` and nothing else;
  - `missing_amount` and `several_amounts` ask and never call `/capture/text`;
  - mixed text offers "Catat sebagai transaksi";
  - a resend reuses the `client_capture_id`.
- **No model host:** a test wraps `fetch` for a full scripted session covering every intent, a capture, unknown text and errors. It asserts that every request URL is same-origin `/api/` and on the allowlist `{cycle-summary, review/summary, review/queue, reports/monthly/latest, auth/me, capture/parse, capture/text, capture/{id}/answer, capture/{id}/undo}`. No host of any model provider appears, nor any absolute URL.
- **Store:**
  - memory only (spies on `localStorage`, `sessionStorage` and `indexedDB` record no call);
  - reset on a user id change;
  - the 100-message cap;
  - "Hapus percakapan".
- **Cards:** each card from a fixture equals the fixture's server values:
  - the Kakeibo lines sum to `keluar.total`;
  - unavailable safe-to-spend never renders 0;
  - a failed fetch renders the error with retry;
  - hidden balances mask every amount, including assistant text.
- **View** (Testing Library with `vitest-axe`):
  - the roles (`log`, `listitem`, buttons by name);
  - focus stays in the composer after sending;
  - Enter and Shift+Enter;
  - the help lists "Di Tanya";
  - the nav shows Tanya, and the "no Tanya" test is replaced.
- **`CatatDialog`:** its existing tests pass unchanged after the client moves.
- **Copy-register guard,** as in D10.

### D13. Rollback

- There is no migration, endpoint or persisted data, so reverting the pull request removes the page, the nav entry and the moved client in one step.
- The `lib/capture.ts` move is the only refactor of existing code. Its revert restores `CatatDialog`'s inline client, and that dialog's tests guard both directions.

### D14. Dependency on phase 3 and parallel work

- **Order:** implementation starts from `main` after `redesign-transactions-workspace` is merged. Task 1.1 checks that `POST /api/capture/parse`, `CatatDialog`, `TransactionDrawer` and the `ui-copy-register` guard exist there.
- **Running beside phase 4:** P4 (`redesign-analysis-and-reports`) may be implemented at the same time. The two changes share no page. Overlaps are limited to the files in D15, and each edit is a few appended lines.

### D15. Shared files this change edits

| File | Edit |
| --- | --- |
| `frontend/src/components/workspace/navigation.ts` | Append the AI group to `NAV_GROUPS` and a `PAGE_META` entry. Update the AI-group comment |
| `frontend/src/components/ui/Icon.tsx` | Add `message-circle` to the name union and paths |
| `frontend/src/components/workspace/workspace.test.tsx` | Replace the "no Tanya" assertion with the group order including AI and Tanya |
| `frontend/src/components/capture/CatatDialog.tsx` | Import the client from `lib/capture.ts` instead of defining it |
| The phase 3 copy-register test file | Add `components/tanya/` and `lib/tanya/replies.ts` to its scope and allowlist |
| `docs/NEW_USER_GUIDE.md` | A Tanya section |
| `openspec/specs/executive-sidebar-layout` (at archive) | Via this change's delta |

Everything else is new: `app/tanya/`, `components/tanya/`, `lib/tanya/` and `lib/capture.ts`. The existing keys `queryKeys.auth.me`, `queryKeys.review.summary` and `queryKeys.reports.latest` are reused, so `queryKeys.ts` and `AppLayout.tsx` are not edited.

## Preview conflicts (the approved rule wins)

| Preview | This change | Rule |
| --- | --- | --- |
| The cycle card says "{amount} belum dihitung, masih nunggu jawabanmu" | "Menunggu jawaban", shown as already inside Keluar | D9 of the phase plan: waiting rows are counted |
| The Kakeibo card has previous-cycle marks per pillar | Lines and the `belanja` comparison sentence only (OD61) | No per-pillar previous data in the cycle summary; no new endpoint |
| The "Apa ini?" card answers inside Tanya | It links to the exact Tinjau item (OD62) | Undo, learning and counts stay in Tinjau's one path |
| Placeholder "Catat atau tanya apa aja…", stamp "Kamu · 14.03", bills heading "Tagihan sampai gajian", the "udah dipisahin" caption | Neutral labels (D10) | Copy register, 2026-11-03 |
| Regex intents and client-side amount parsing | Table intents plus the server parser for amounts | Phase 3 D11: one parser |
| The name "Arus" in stamps | `PRODUCT_NAME` ("CashFlow") | Phase plan OD1 |

## Risks / Trade-offs

- **A question with a number is recorded as a transaction** → mitigated by the question-marker rule, the narrow intent table, Urungkan on the card and the drawer afterwards. Tests cover the known phrasings. OD60's alternative, confirm first, is one constant away.
- **Snapshot cards go stale** → each card is stamped "Per 14.03" and has "Perbarui".
- **Intent coverage is narrow** → unknown text gets the chips instead of a guess, by design. The intent table is easy to extend in later changes.
- **A module-level store keeps data until reload if the user changes without a reload** → it resets on any user id change. Sign-out reloads the page.
- **Phase 3 is still in flight** → this plan names its contracts. Task 1.1 re-checks them on the merged `main` before code, and any drift is surfaced before implementing.
- **The on-screen keyboard on real phones** is not verifiable headless → recorded as not verified, and added to the owner's end-of-redesign check list.

## Migration Plan

- No database migration, endpoint or configuration.
- Deploy through `deploy.yml` as usual. After the deploy, a read-only production check confirms that `/tanya` returns the page for a signed-in session and that the nav shows Tanya.
- Rollback is reverting the pull request.

## Open decisions for owner

| ID | Decision | Default |
| --- | --- | --- |
| OD60 | Record on send, or show a "Catat?" confirmation card first | **Record on send**, with Urungkan and Ubah on the card, as the preview and the phone do. Questions with a question marker are never recorded |
| OD61 | Per-pillar previous-cycle marks in the Kakeibo card | **Not in this phase.** Show the `belanja` comparison sentence. Analisis (phase 4) owns pillar comparisons |
| OD62 | Answer Tinjau items inside Tanya | **No.** Link to the exact item (`/review?item=…&folder=…`) |
| OD63 | Include the latest-report intent and suggestion card (not in the P6 row's four) | **Yes, as a link card** using the shell's cached `/reports/monthly/latest` |
| OD64 | Conversation lifetime | **Memory only, per tab, 100 messages, cleared on reload, sign-out and user change,** plus "Hapus percakapan" |
| OD65 | Phone entry | **Lainnya sheet only.** The tab bar is unchanged |
| OD66 | Recording keeps `/capture/text`'s bounded server-side model use | **Yes,** consistent with phase 3 OD26. Tanya's answers never use a model |
| OD67 | Bills card range | **"minggu ini" or "this week" shows the next 7 days; otherwise the cycle-summary window** |

## Open Questions

None beyond the open decisions above.
