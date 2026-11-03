# Verification: add-tanya-conversation

To be filled during implementation. Every check runs locally against disposable resources, with synthetic data only:
- PostgreSQL 16 container;
- the worktree API with AI off and outbound HTTP blocked;
- `next start` behind a loopback proxy;
- headless Brave over CDP.

No production host, phone or real model is used.

- **Resources:** Postgres container `cft-tanya-pg` on 127.0.0.1:55522 (databases `ledger_test` for pytest, `ledger_browser` for the browser); API on 8222 (`run_api.sh`: AI off, jobs off, outbound HTTP through a dead proxy); `next start` on 3223 behind `proxy.mjs` on 3222; headless Brave CDP on 9522.
- **Scripts and seed:** `scratchpad/web-p6/` (copied from `web-p3/` with ports and paths retargeted). `seed.py` creates the synthetic owners `beranda_contoh` and `pemilik_lain`; the seeded database is dumped once and restored with `reset_db.sh` before each flow run.
- **Base:** phase 3 is not merged yet. The branch is rebased onto its pushed tip `17e15bd` (`origin/feat/transactions-workspace`), and the baseline is measured at `137eb6b` (plan approval on top of it). It is rebased onto `main` once phase 3 merges.

Revisions: baseline `137eb6b` (on phase 3's tip, before the rebase). Phase 3 then merged (`3189a91`) and its archive merged (`0ba4c41`), and the branch was rebased twice onto `origin/main`; neither rebase touched a file this change edits outside OpenSpec. The revision under test is `0e79103` (the browser runs used a build of the working tree that equals it). The bundle comparison uses a build of `origin/main` made from `git archive`. Document-only commits: the plan, approval and baseline commits.

## 1. Prerequisites (task 1.1)

| Phase 3 contract | Expected (design) | Found on `main` | Drift and action |
| --- | --- | --- | --- |
| `POST /api/capture/parse` | `{text, device_time}` → `preview` with `amount`, `problems`, `kind`, `description`, `occurred_at`, `time_phrase`, account and category hints; own bucket 120 per 60 s | `routers/capture.py` `parse_capture_text`: exactly those fields plus `time_options`; `PARSE_RATE_LIMIT = 120`, `PARSE_RATE_WINDOW = 60`, 429 `capture_rate_limited`. Problem codes `missing_amount` and `several_amounts` | None |
| `CatatDialog` inline client | Types and helpers inside `CatatDialog.tsx` | `Preview`, `CaptureQuestion`, `Capture`, `deviceTime`, `newId`, `timeText`, `undoStillOpen` and the inline `api.post` calls are there. The `/capture/text` response also carries `transaction` (account, category, pillar, `review_status`) and `movement` | Moved types are named `CapturePreview`, `CaptureQuestion`, `Capture` (D6). The capture card reads `capture.transaction` for account and category; no contract change |
| `TransactionDrawer` open API | Opened with a row id | `TransactionDrawer({ transactionId, onClose, record, onPickPair?, onDeleted? })`; `record` comes from `useTransactionUndo()` | Tanya passes `record` from its own `useTransactionUndo()` and no `onPickPair` (no list to pick from) |
| Copy-register guard | A test with an allowlist mechanism | `frontend/src/copyRegister.test.ts`: `SLANG` token regex, `CONVERSATION` exact-string allowlist, `ROOTS` scope | Tanya adds `components/tanya`, `app/tanya` and `lib/tanya` (minus `replies.ts`) to `ROOTS`; `lib/tanya/replies.ts` stays out of scope as the assistant-voice module (D10) |
| `LEDGER_NAV_HREF` | Exported from `navigation.ts` | `"/ledger?period=cycle"` in `components/workspace/navigation.ts` | None |

## 2. Baseline (task 1.2)

| Check | Result |
| --- | --- |
| `npm ci` / type-check / lint / `npm test` / build | All pass at `137eb6b`. Lint 0 errors, 0 warnings. Vitest 68 files, 612 tests |
| First-load JS per route and shared chunk (raw, gzip) | Shared chunk 13 scripts, 226.7 KB gzip. Route totals (raw / gzip KB): `/` 844.3 / 253.9, `/review` 811.8 / 244.6, `/ledger` 835.7 / 252.7, `/accounts` 871.4 / 255.3, `/goals` 790.2 / 234.9, `/insights` 1195.1 / 345.4, `/laporan` 790.3 / 237.1, `/settings` 765.5 / 232.1. CSS 18.6 KB gzip |
| Backend suite (Flyway, `REQUIRE_TEST_DATABASE=1`) | Flyway V1 to V44 on a fresh database; 1664 passed, 0 failed, 0 skipped (111 s) |
| Sidebar 1440 and Lainnya 390 screenshots | `web-p6/baseline/nav-{1440,1024,390}-{light,dark}.png`: sidebar groups Harian, Uang, Laporan; Lainnya lists Rekening & Kantong, Target & Tagihan, Analisis, Laporan, Pengaturan |

## 3. Unit and component tests (tasks 2.1–4.8)

| Area | Tests | Result |
| --- | --- | --- |
| `lib/capture.ts` move (CatatDialog tests before and after) | `CatatDialog.test.tsx` unchanged; new `lib/capture.test.ts` | CatatDialog 6 passed before and 6 after; helpers 6 passed |
| Navigation | `workspace.test.tsx`: group order Harian, AI, Uang, Laporan with `/tanya`; Tanya active on `/tanya`; rail has 11 named items; Lainnya lists Tanya first; tab bar unchanged | 108 passed (6 files) |
| Intent table (phrases per intent, near misses, benchmark) | `lib/tanya/intents.test.ts`: 6 to 11 Indonesian and English phrases per intent (53 in total), 7 near misses, specific intents before `cycle`, bills `range`, normalisation, the 200-character cap, question markers, and a 2000-run benchmark under 1 ms per 200-character text | 14 passed |
| Resolver | `lib/tanya/resolve.test.ts` with the API mocked: each intent card and no capture request; help; failed card fetch is an error card; one amount records once with the message's id; Tinjau wording; `needs_detail` kept; four question phrasings never recorded; `missing_amount` and `several_amounts` ask and never submit; unknown text after exactly one parse call; mixed text offers the chip and the forced capture records; parse 429 and network failure keep the text; network failure then resend reuses the id; record 429; empty text | 23 passed |
| Thread store (no persistence, user reset, cap) | `lib/tanya/thread.test.ts`: spies on `Storage.prototype.setItem`, `document.cookie` and `indexedDB.open` record no write; reset on a new user id; 100-message cap drops the oldest; clear; update in place | 5 passed |
| Cards (figures equal fixtures, no "belum dihitung", hidden balances) | `components/tanya/TanyaView.test.tsx` "Tanya page": greeting and lead, username and no-name fallback, unavailable safe-to-spend names the remaining amount, unloaded count left out of the label, cycle figures equal the fixture, Kakeibo lines in Beranda's order sum to `keluar.total` with the offset line and the `belanja` sentence, bills for the week (2 of 3 plans, Rp 665.000) and for the window (Rp 1.665.000), empty bills, waiting count = `attention` with 3 deep links, zero waiting, report ready and not ready via Perbarui, hidden balances leave no `Rp <digit>` anywhere, failed card shows the error, no figure, and rebuilds on retry; `lib/tanya/replies.test.ts` for the hour buckets | 14 + 2 passed |
| Capture in the composer | "Tanya recording": record on send with focus kept and one `/capture/text`; Urungkan announces and refocuses; 410 shows the server's reason; no Urungkan after the window; Ubah opens the drawer on the row; `needs_detail` answered with a fresh `reply_id`; an unanswered question stays open with "Belum dicatat…"; 429 keeps the text; network failure then Kirim ulang reuses the id; parse failure keeps the text; a question with a number and "halo" record nothing (one parse call); "parkir tadi" asks for the amount; "Catat sebagai transaksi" records on purpose | 13 passed |
| Accessibility (roles, focus, keys, axe) | "Tanya accessibility": a labelled `log` of `listitem`s with speaker and time labels and 0 `vitest-axe` violations; Enter sends, Shift+Enter adds a line, empty text never sends; the help lists "Di Tanya"; Hapus percakapan after confirming; another user sees none of the first user's messages | 5 passed |
| Failure states | Covered above: card fetch failure, empty bills, unavailable safe-to-spend, parse failure (offline), 429; a 401 follows the shell's existing redirect (unchanged `AppLayout`) | in the counts above |
| Copy-register guard (passes; planted token fails) | `copyRegister.test.ts` scope gains `components/tanya`, `app/tanya`, `lib/tanya/resolve.ts`, `thread.ts`, `types.ts` and `lib/capture.ts`. Planting "Nggak ada yang perlu ditinjau." in `components/tanya/cards.tsx` failed it with `components/tanya/cards.tsx:172` named; restoring the file passed it | 2 passed |

## 4. Full checks (task 5.1)

| Check | Result |
| --- | --- |
| Frontend `npm ci` / type-check / lint / `npm test` / build | All pass at `0e79103`. Lint 0 errors, 0 warnings. Vitest 76 files, 702 tests (701 on the rebased main plus this change's tests counted below; the network test adds 1). `/tanya` is a static route |
| Backend suite | Unchanged by this change. Fresh Flyway V1–V44 database, `REQUIRE_TEST_DATABASE=1`: 1664 passed, 0 failed, 0 skipped (103 s), at `93d7f5c` (no backend file changes after it) |

## 5. No model call (task 5.2)

| Check | Result |
| --- | --- |
| Vitest whole-session allowlist | `components/tanya/TanyaNetwork.test.tsx` drives the real API client with `fetch` stubbed through every intent, help, unknown text, a question with a number, a capture with a follow-up, undo and a failed card refresh: every one of the requests is a relative `/api/` path on the allowlist, none matches a model-provider host, and the distinct paths are exactly the eight expected. 1 passed |
| Browser request log: requests per path, foreign hosts | `web-p6/interact/request-log.json`, a scripted session of 11 texts, the forced capture, a capture with follow-ups, Urungkan and a blocked cycle summary: 28 requests, 0 foreign hosts. `POST /api/capture/parse` 5, `POST /api/capture/text` 2, `POST /api/capture/{id}/answer` 4, `POST /api/capture/{id}/undo` 1, `GET /api/dashboard/cycle-summary` 6, `GET /api/review/summary` 5, `GET /api/review/queue` 1, plus same-origin page prefetches of the card links (`GET /review` 3, `GET /laporan` 1). Questions never reached the parser |

## 6. Browser matrix on `/tanya` (task 5.3)

| Width | Theme | axe serious or critical | Overflow | Console errors | Lowest contrast | Small targets | Screenshot |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1440 | light | 0 (empty and full thread) | 0 | 0 | 5.31 (needs 4.5) | 0 | `matrix/tanya-1440-light.png`, `-full.png`, `-empty.png` |
| 1440 | dark | 0 (empty and full thread) | 0 | 0 | 6.35 (needs 4.5) | 0 | `matrix/tanya-1440-dark.png`, `-full.png`, `-empty.png` |
| 1280 | light | 0 (empty and full thread) | 0 | 0 | 5.31 | 0 | `matrix/tanya-1280-light.png`, `-full.png`, `-empty.png` |
| 1280 | dark | 0 (empty and full thread) | 0 | 0 | 6.35 | 0 | `matrix/tanya-1280-dark.png`, `-full.png`, `-empty.png` |
| 1024 | light | 0 (empty and full thread) | 0 | 0 | 5.31 | 0 | `matrix/tanya-1024-light.png`, `-full.png`, `-empty.png` |
| 1024 | dark | 0 (empty and full thread) | 0 | 0 | 6.35 | 0 | `matrix/tanya-1024-dark.png`, `-full.png`, `-empty.png` |
| 768 | light | 0 (empty and full thread) | 0 | 0 | 5.31 | 0 | `matrix/tanya-768-light.png`, `-full.png`, `-empty.png` |
| 768 | dark | 0 (empty and full thread) | 0 | 0 | 6.35 | 0 | `matrix/tanya-768-dark.png`, `-full.png`, `-empty.png` |
| 390 | light | 0 (empty and full thread) | 0 | 0 | 5.31 | 0 | `matrix/tanya-390-light.png`, `-full.png`, `-empty.png` |
| 390 | dark | 0 (empty and full thread) | 0 | 0 | 6.35 | 0 | `matrix/tanya-390-dark.png`, `-full.png`, `-empty.png` |

Every cell had the full thread: the cycle, Kakeibo, bills (week), waiting and report cards, a capture card recorded after two follow-up questions, the unknown reply with chips, and an error card (the cycle summary blocked, then "Perbarui"). The lowest text contrast in light is the error message, in dark the card caption. Script: `web-p6/matrix_tanya.mjs`; results `web-p6/matrix/matrix.json`.

Zoom 200% and 400%, reduced motion, and focus not obscured at 390:

- **Focus not obscured (2.4.11):** every control in the thread (18 per cell) was focused in turn; none ended under the sticky composer, the phone top bar or the tab bar at any width, and none at 200% (7 of 7) or 400% (7 of 7) zoom. The first run found 5 to 6 covered controls at 768 and 390 and 1 to 7 under zoom; fixed with `scroll-padding` while Tanya is open and a composer that stays in the flow below 520 px of height (design, implementation notes).
- **Zoom:** 200% and 400% of a 1280 × 900 window: overflow 0, the phone tab bar shown, screenshots `matrix/tanya-zoom200.png` and `tanya-zoom400.png`.
- **Reduced motion:** the typing dots' animation is `none`; the newest message is scrolled into view without smooth scrolling.
- **Keyboard:** after sending, focus stays in the composer ("Tulis transaksi atau pertanyaan"); Shift+Tab reaches the newest reply's chips and then the capture card's buttons in the thread.
- I read the screenshots of every width and theme. Fixed from them: the phone suggestion strip started flush at the screen edge (now 16 px scroll padding); everything else matched the preview's composition.

## 7. Real-UI flows (task 5.4)

| Flow | Expected | Observed |
| --- | --- | --- |
| Card figures equal the API and Beranda | Every figure from `/api/dashboard/cycle-summary` and the badge | Cycle card: Rp 240.000, Rp 3.521.500, Rp 9.800.000, Rp 46.330.000 and "4 · Rp 581.000" all present; Kakeibo lines and total equal; bills total Rp 995.000 equals the scheduled lines; "4 perlu ditinjau" equals `attention`; Beranda's four KPIs show the same four amounts |
| "parkir 5rb jam 2 tadi" records one row at 14.00 | One row | Run at 13.2x, before 14.00, so the server read "jam 2 tadi" as 02.00 today ("tadi" is earlier); it asked "Dari rekening mana?" and "Kategorinya apa?" in the thread, then recorded one row (123 → 124): `2026-11-03 02:00 5000 Parkir`. The same flow with "parkir 5rb jam 1 tadi" at 13.29 recorded 13.00. The time is the server's reading, not Tanya's |
| Urungkan restores Beranda's Keluar | Row gone, Keluar back | Keluar 3.521.500 → 3.526.500 recorded → 3.521.500 after Urungkan; rows 124 → 123; status "Catatan dibatalkan."; focus back in the composer; the card says "Dibatalkan" |
| `needs_detail` answered in the thread | Question and answer in the thread | Two questions (account, then category) answered by their buttons; the card updated in place to "Dicatat" |
| Ubah opens the drawer | Drawer on the row | "kopi 18rb" recorded at once; Ubah opened the shared drawer on it (−Rp 18.000, "Teks yang diketik: kopi 18rb"), screenshot `interact/ubah-drawer.png` |
| "parkir tadi" records nothing | No row | Asked "Jumlahnya berapa? …"; rows 125 → 125 |
| "berapa keluar 2 hari ini?" records nothing | No row | Unknown reply with chips, no parse request; rows 125 → 125 |
| "halo" gives chips, records nothing | No row | Unknown reply (with the line on recording) and the four chips; rows 125 → 125 |
| Waiting links land on the exact Tinjau item | Each link opens its item | QRIS 0812-xxxx → "Apa ini? Rp45.000 dari Dompet Digital" (folder apa), Toko Serba Ada → "Toko Serba Ada" (jawab), Keluar dari RDN Contoh → "Uang keluar dari RDN Contoh. Masuknya ke mana?" (pindah); focus on each item's heading |
| Reload empties; navigation keeps | Kept across in-app navigation, gone on reload | 2 messages before, 2 after Beranda → Tanya through the sidebar, 0 after a reload. `localStorage`, `sessionStorage` and readable cookies held no Tanya key |
| Second user sees nothing of the first | Empty thread | After signing out and in as `pemilik_lain`: 0 messages, greeting "Siang, Dewi." |
| API stopped: error cards, no 0 | Errors, never 0 | With the API stopped: "Perbarui" gave "Data ini tidak dapat dimuat." with "Coba lagi"; a new capture gave "Pesan ini tidak dapat diproses. Coba lagi." with the text kept in the composer; 0 figures of Rp 0; rows unchanged (125) |

## 8. Regression and bundle (tasks 5.5, 5.6)

| Check | Result |
| --- | --- |
| Other routes against `main` (axe, overflow, console) | `/`, `/review`, `/ledger`, `/insights`, `/laporan`, `/accounts`, `/goals` and `/settings` at 1280 and 390 on this build and on a build of `origin/main`: identical. Overflow 0 and console errors 0 everywhere; the only serious axe finding is the known `/accounts` colour contrast (2 nodes at 1280), on both builds. `web-p6/regression-new/`, `regression-main/` |
| Nav order at 1440, 1024 and 390 | Sidebar and rail list Beranda, Tinjau, Transaksi, Tanya, Rekening & Kantong, Target & Tagihan, Analisis, Laporan (11 named items with the foot); Lainnya at 390 lists Tanya first; the tab bar is unchanged. `web-p6/after/nav-*.png` against `web-p6/baseline/` |
| `/tanya` route JS over shared (gzip) | 19.7 KB (gate 30 KB); route total 246.6 KB gzip, 811.0 KB raw; route CSS 20.9 KB gzip with the module |
| Shared chunk change (gzip) | 226.7 → 226.9 KB (+0.2 KB, gate +1 KB) against the `origin/main` build. `/` and `/ledger` grew by 1.0 and 0.9 KB gzip because the chunk shared by Catat and the drawer split in two once Tanya imports the same pieces (one extra script, +0.7 KB); `/review` unchanged in size |
| `package.json` dependency diff | Empty: no new dependency |

## 9. OpenSpec and diff (task 5.7)

- `openspec validate add-tanya-conversation --strict`: valid.
- `openspec validate --all --strict`: 75 passed, 0 failed, exit 0 (on main after phase 3's archive).
- `git diff --check`: clean. The diff against `origin/main` touches 25 files outside this change's folder: the Tanya page, components and `lib/tanya/`, the moved capture client, and the shared files of design D15 plus `shortcuts.tsx` (implementation notes). No secret, log line or dependency was added.

## Not verified

- No production, phone or companion check (none expected: no endpoint or contract changed).

- The on-screen keyboard on a real phone (headless browsers have none). This goes on the end-of-redesign check list.
- Screen readers (semantics checked with role queries and axe). This also goes on the end-of-redesign check list.
- The time a capture reads ("jam 2 tadi" before 14.00 is 02.00) is the server parser's, unchanged by this phase; the scenario's 14.00 holds only when the phrase is sent after 14.00.

## Acceptance

- 2026-11-03: CI "Checks" run 41590557161 on `ed995a9` passed (backend, frontend, compose and backup). The owner said "accepted, defaults ok". The Beranda lede greeting by username instead of the account name is a phase 1 behaviour, handed to phase 5 (Akun).
