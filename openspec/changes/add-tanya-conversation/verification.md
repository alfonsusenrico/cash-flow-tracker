# Verification: add-tanya-conversation

To be filled during implementation. Every check runs locally against disposable resources, with synthetic data only:
- PostgreSQL 16 container;
- the worktree API with AI off and outbound HTTP blocked;
- `next start` behind a loopback proxy;
- headless Brave over CDP.

No production host, phone or real model is used. Name the containers, ports and seed scripts here.

Revisions: name the baseline revision, the revision under test, and which later commits touch only documents.

## 1. Prerequisites (task 1.1)

| Phase 3 contract | Expected (design) | Found on `main` | Drift and action |
| --- | --- | --- | --- |
| `POST /api/capture/parse` | | | |
| `CatatDialog` inline client | | | |
| `TransactionDrawer` open API | | | |
| Copy-register guard | | | |
| `LEDGER_NAV_HREF` | | | |

## 2. Baseline (task 1.2)

| Check | Result |
| --- | --- |
| `npm ci` / type-check / lint / `npm test` / build | |
| First-load JS per route and shared chunk (raw, gzip) | |
| Backend suite (Flyway, `REQUIRE_TEST_DATABASE=1`) | |
| Sidebar 1440 and Lainnya 390 screenshots | |

## 3. Unit and component tests (tasks 2.1–4.8)

| Area | Tests | Result |
| --- | --- | --- |
| `lib/capture.ts` move (CatatDialog tests before and after) | | |
| Navigation | | |
| Intent table (phrases per intent, near misses, benchmark) | | |
| Resolver | | |
| Thread store (no persistence, user reset, cap) | | |
| Cards (figures equal fixtures, no "belum dihitung", hidden balances) | | |
| Capture in the composer | | |
| Accessibility (roles, focus, keys, axe) | | |
| Failure states | | |
| Copy-register guard (passes; planted token fails) | | |

## 4. Full checks (task 5.1)

| Check | Result |
| --- | --- |
| Frontend `npm ci` / type-check / lint / `npm test` / build | |
| Backend suite | |

## 5. No model call (task 5.2)

| Check | Result |
| --- | --- |
| Vitest whole-session allowlist | |
| Browser request log: requests per path, foreign hosts | |

## 6. Browser matrix on `/tanya` (task 5.3)

| Width | Theme | axe serious or critical | Overflow | Console errors | Lowest contrast | Small targets | Screenshot |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1440 | light | | | | | | |
| 1440 | dark | | | | | | |
| 1280 | light | | | | | | |
| 1280 | dark | | | | | | |
| 1024 | light | | | | | | |
| 1024 | dark | | | | | | |
| 768 | light | | | | | | |
| 768 | dark | | | | | | |
| 390 | light | | | | | | |
| 390 | dark | | | | | | |

Zoom 200% and 400%, reduced motion, and focus not obscured at 390:

## 7. Real-UI flows (task 5.4)

| Flow | Expected | Observed |
| --- | --- | --- |
| Card figures equal the API and Beranda | | |
| "parkir 5rb jam 2 tadi" records one row at 14.00 | | |
| Urungkan restores Beranda's Keluar | | |
| `needs_detail` answered in the thread | | |
| Ubah opens the drawer | | |
| "parkir tadi" records nothing | | |
| "berapa keluar 2 hari ini?" records nothing | | |
| "halo" gives chips, records nothing | | |
| Waiting links land on the exact Tinjau item | | |
| Reload empties; navigation keeps | | |
| Second user sees nothing of the first | | |
| API stopped: error cards, no 0 | | |

## 8. Regression and bundle (tasks 5.5, 5.6)

| Check | Result |
| --- | --- |
| Other routes against `main` (axe, overflow, console) | |
| Nav order at 1440, 1024 and 390 | |
| `/tanya` route JS over shared (gzip) | |
| Shared chunk change (gzip) | |
| `package.json` dependency diff | |

## 9. OpenSpec and diff (task 5.7)

- `openspec validate add-tanya-conversation --strict`:
- `openspec validate --all --strict`:
- `git diff --check`:

## Not verified

- The on-screen keyboard on a real phone (headless browsers have none). This goes on the end-of-redesign check list.
- Screen readers (semantics checked with role queries and axe).
