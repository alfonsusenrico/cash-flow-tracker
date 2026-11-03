# Tasks

The owner approved this plan on 2026-11-03 ("approved, defaults ok", proposal.md, "Approval"). Phase 3 (`redesign-transactions-workspace`) is pushed but not merged yet, so implementation starts on its pushed tip and the branch is rebased onto `main` once phase 3 merges.

The owner tests nothing until every phase is done (2026-11-03). The agent's own evidence is therefore the only runtime check, and every task's evidence must be complete in `verification.md`: commands, counts, skips with reasons, screenshots the agent has looked at, and measured values.

Data is synthetic only (for example "beranda_contoh", "Kedai Awan", "Dompet Digital", "Tunai"). The backend runs on a disposable PostgreSQL 16 migrated by Flyway, with AI off and outbound HTTP blocked, as in phase 2. The browser matrix means 1440, 1280, 1024, 768 and 390 px, each in light and dark, in a headless browser on a local production build.

## 1. Prerequisites and baseline

- [x] 1.1 Rebase `feat/tanya-conversation` onto the `main` that contains phase 3. Check that phase 3's contracts exist as this plan names them:
  - `POST /api/capture/parse` (request and response fields, the 120-per-60 s bucket);
  - `CatatDialog` with its inline capture client;
  - `TransactionDrawer` and how a row is opened in it;
  - the copy-register test and its allowlist mechanism;
  - `LEDGER_NAV_HREF`.

  Record any drift and update design.md before code. Evidence: a short table in verification.md, and the design diff if any.
- [x] 1.2 Record the baseline before any change:
  - `npm ci`, `npm run type-check`, `npm run lint`, `npm test` and `npm run build` results;
  - first-load JS of every route and of the shared chunk (raw and gzip);
  - the full backend suite with its skips (expected unchanged by this phase);
  - screenshots of the sidebar at 1440 and of the Lainnya sheet at 390 in both themes.

  Evidence: the baseline table in verification.md.

Expected Result: the plan matches the merged phase 3 code, and every later gate has its baseline.

## 2. Shared pieces

- [x] 2.1 Move the capture types and calls from `CatatDialog.tsx` to `lib/capture.ts` (design D6): `parseCapture`, `submitCapture`, `answerCapture`, `undoCapture`, `deviceTime`, `newId`, `timeText` and `undoStillOpen`. `CatatDialog` imports them. Add unit tests for the moved helpers. Evidence: `CatatDialog`'s existing tests pass unchanged (count before and after), and the new helper tests pass.
- [x] 2.2 Add the nav entry (design D1):
  - the AI group with "Tanya" `/tanya` in `NAV_GROUPS`, between Harian and Uang;
  - `message-circle` in `Icon.tsx`;
  - the `PAGE_META` entry.

  Replace the "no Tanya" test with the order Harian, AI, Uang, Laporan. Add a test that the Lainnya sheet lists Tanya and that the tab bar is unchanged. Evidence: workspace tests pass.

Expected Result: Catat and Tanya share one capture client, and the shell links to `/tanya`.

## 3. Intent layer and store

- [x] 3.1 Write `lib/tanya/intents.ts` (design D2), the normalisation and the intent table with `range` for bills. Tests:
  - every listed Indonesian and English phrase per intent, at least 6 per intent;
  - near misses that must match nothing ("bayar listrik 200rb", "kopi 18rb", "halo", "parkir tadi");
  - case, spacing and punctuation;
  - the 200-character cap;
  - a micro-benchmark under 1 ms for 200 characters.

  Evidence: Vitest counts.
- [x] 3.2 Write `lib/tanya/resolve.ts`, the resolver chain, and `lib/tanya/replies.ts`, which holds every assistant string. Tests with the API mocked:
  - each intent;
  - capture with one amount (records once, with one `client_capture_id`);
  - question markers block capture;
  - `missing_amount` and `several_amounts` ask and never submit;
  - unknown text gives the unknown reply after exactly one parse call and nothing else;
  - mixed text offers "Catat sebagai transaksi";
  - parse 429 and network failure;
  - a resend reuses the id.

  Evidence: Vitest counts.
- [x] 3.3 Write `lib/tanya/thread.ts` (design D7): the store tagged with the user id, the 100-message cap and "Hapus percakapan". Tests:
  - spies on `localStorage`, `sessionStorage`, `indexedDB` and `document.cookie` record no write;
  - reset when the user id changes;
  - the cap drops the oldest;
  - clearing.

  Evidence: Vitest counts.

Expected Result: the conversation logic is pure, deterministic and memory-only, with tests that fail on any guess, persistence or extra request.

## 4. Tanya page

- [ ] 4.1 Build `app/tanya/page.tsx` and `components/tanya/` (design D3, D4, D8):
  - the greeting, with the name fallback and time-of-day buckets;
  - the lead line, including the unavailable safe-to-spend wording;
  - the four suggestion cards, with counts only when loaded;
  - the thread and the composer;
  - `Tanya.module.css` with the per-theme glow.

  Evidence: component tests (greeting buckets, name fallback, no 0 for unloaded counts), plus screenshots at 1440 light and dark.
- [x] 4.2 Build the cards `cycle`, `kakeibo`, `bills`, `waiting` and `report` (design D3):
  - each holds a snapshot with "Per hh.mm" and "Perbarui";
  - hidden balances mask every amount, including assistant text;
  - the waiting card uses `buildTriage` and deep links.

  Tests from fixtures:
  - figures equal the server values;
  - Kakeibo lines sum to `keluar.total`;
  - week range filtering and its sum;
  - waiting count equals `attention`, at most 3 items, correct `/review?item=…&folder=…` links;
  - the report card when ready and when not;
  - no "belum dihitung".

  Evidence: Vitest counts.
- [x] 4.3 Add capture in the composer (design D6):
  - recording on send;
  - the capture card;
  - `needs_detail` question and answer with a fresh `reply_id`;
  - "Urungkan" only inside `undo_until`, with 409 and 410 notices;
  - "Ubah" opens the shared drawer;
  - query invalidation as `CatatDialog` does;
  - 429 and network errors with "Kirim ulang" reusing the id.

  Evidence: component tests with the API mocked, covering each branch.
- [x] 4.4 Accessibility (design D5):
  - `role="log"`, message `li`s with speaker and time labels;
  - card summaries;
  - focus stays in the composer, and Shift+Tab reaches the reply;
  - "Pesan baru" when scrolled up;
  - reduced motion;
  - Enter and Shift+Enter;
  - the "Di Tanya" help group.

  Evidence: Testing Library role queries plus `vitest-axe`, with 0 violations.
- [ ] 4.5 Phone composition (design D8):
  - the compact greeting;
  - the suggestion strip scrolls inside itself;
  - the composer sits above the tab bar;
  - bottom padding keeps the last message and focused buttons visible.

  Evidence: covered in 5.3 at 390, plus a focused test of the padding custom property.
- [x] 4.6 Failure states (design D9):
  - card fetch failure shows the error with retry, never 0;
  - empty bills;
  - offline;
  - 401 follows the app's redirect.

  Evidence: component tests per row of the D9 table.
- [x] 4.7 Copy register (design D10): add `components/tanya/` to the phase 3 guard, with only `lib/tanya/replies.ts` allowlisted for the assistant voice. Evidence: the guard passes, and a planted slang label in a Tanya component makes it fail (shown once, then removed).
- [x] 4.8 Add a Tanya section to `docs/NEW_USER_GUIDE.md`: what it answers, how recording works, that it never guesses, and that the conversation is gone after a reload. Evidence: the diff.

Expected Result: `/tanya` answers, records and fails honestly, and works by keyboard, by screen-reader semantics and at every width.

## 5. Integration verification

- [ ] 5.1 Run the full checks as CI does:
  - frontend `npm ci`, type-check, lint with 0 warnings, `npm test` and build;
  - the backend suite on Flyway-migrated PostgreSQL 16 with `REQUIRE_TEST_DATABASE=1`, expected unchanged.

  Evidence: counts in verification.md.
- [ ] 5.2 Prove no model call:
  - the Vitest whole-session fetch allowlist (design D12);
  - in the browser, a CDP network log of a scripted session (every intent, unknown text, a capture with a follow-up, undo, errors) in which every request is same-origin `/api/` on the allowlist or a static asset, and no other host appears.

  Evidence: the allowlist test count, plus the request log summary (counts per path, 0 foreign hosts).
- [ ] 5.3 Run the browser matrix on `/tanya` (empty thread, and a thread with every card kind, a capture card and an error card), in light and dark at the five widths. Check:
  - axe 0 serious or critical;
  - horizontal overflow 0;
  - console errors 0;
  - lowest measured text contrast at least 4.5:1, and 3:1 for large text and UI;
  - targets at least 24 px;
  - focus visible and not obscured (2.4.11) at 390;
  - 200% and 400% zoom reflow;
  - reduced motion.

  Read every screenshot and fix what looks off. Evidence: the matrix table and screenshot paths.
- [ ] 5.4 Run the real-UI flows against the local stack:
  - **Figures:** each suggestion card's figures equal the API response and Beranda's figures.
  - **Recording:** "parkir 5rb jam 2 tadi" records one row (count before and after) at 14.00.
  - **Undo:** "Urungkan" removes it, and Beranda's Keluar returns to the same value.
  - **Follow-up:** a `needs_detail` capture is answered in the thread.
  - **Drawer:** "Ubah" opens the drawer on that row.
  - **No amount:** "parkir tadi" records nothing.
  - **Question with a number:** "berapa keluar 2 hari ini?" records nothing.
  - **Unknown text:** "halo" gives chips and records nothing.
  - **Waiting links:** each link lands on the exact Tinjau item.
  - **Lifetime:** reload empties the thread; navigating away and back keeps it.
  - **Users:** signing in as `pemilik_lain` shows none of the first user's messages.
  - **Failure:** a stopped API shows error cards without 0.

  Evidence: `web-p6/interact/interact.json` and screenshots.
- [ ] 5.5 Run a regression pass on every other route against a build of the `main` this branch starts from: axe, overflow and console errors equal or better. Confirm the sidebar, rail and Lainnya show Tanya in order at 1440, 1024 and 390. Evidence: the table and screenshots.
- [ ] 5.6 Check the bundle gate (design D11): `/tanya` route JS at most 30 kB gzip over the shared chunk, the shared chunk at most +1 kB gzip, and no new dependency (`package.json` diff empty). Evidence: the numbers against the 1.2 baseline.
- [ ] 5.7 `openspec validate add-tanya-conversation --strict` and `openspec validate --all --strict` exit 0. `git diff --check` is clean. Review the final diff for secrets, dead code and unrequested changes. Evidence: command output in verification.md.

Expected Result: every gate passes with recorded evidence, and nothing outside Tanya changed behaviour.

## 6. Acceptance

- [ ] 6.1 Present a short summary to the owner: what changed, the evidence, deviations, and new open decisions with defaults. Add what the owner should try at the end of the redesign (on-screen keyboard on a real phone, screen reader) to the end-of-redesign check list. Record the acceptance with the owner's words and date in proposal.md. Evidence: the owner's words quoted with the date.

## Workflow follow-up

- After acceptance: `openspec archive add-tanya-conversation -y` on the branch, then `openspec validate --all --strict` exits 0. Push the branch. The owner opens and merges the pull request, and the release runs only through `deploy.yml`.
- After the deploy: a read-only production check that `/tanya` is served and the nav shows Tanya. Delete the merged branch locally and on the remote.
