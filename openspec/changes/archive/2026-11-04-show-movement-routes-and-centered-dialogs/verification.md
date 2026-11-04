# Verification: show-movement-routes-and-centered-dialogs

## Revisions

- **Baseline:** the approval commit `d15189f`, whose application code equals `main` at `0a342a9`. Every baseline measurement ran before any code change.
- **Under test:**
  - backend suite at `c315ad8`, with no backend change after `8a64df0`;
  - frontend checks at the final commit of this record, which differs from `c315ad8` only by the added Beranda test;
  - browser runs at `c315ad8`.
- **Comparison build for unchanged writes:** `main` at `0a342a9`, built in a scratch worktree.

## Environment and data

- **Database:** PostgreSQL 16 in `cft-routes-pg` on 55572, migrated by flyway/flyway:10 to V44.
  - `ledger_test` serves the backend suite.
  - `ledger_browser` serves the browser runs.
- **Servers:**
  - API (uvicorn) on 8272, with background jobs, AI and plan auto-linking off;
  - `next start` on 3273, behind a loopback proxy on 3272;
  - headless Brave with CDP on 9572.
- **Data:** synthetic only. The 2026-11-04 preview seed is shaped like the owner's account: BCA, Jago with 13 pockets, Gopay, ShopeePay, RDN, Stockbit and Bibit. This change extends it with:
  - the 1 November sequence: BCA · ATM → Gopay at 01.39, then Gopay → Jago · Tabungan at 01.41;
  - pockets with similar names in two banks: BCA · Tabungan Rumah → Jago · Tabungan;
  - an unpaired income filed as Internal Movement on Jago · Main Pocket (Rp 120.000, "Pindah uang dari BCA");
  - an unpaired expense filed as Internal Movement on ShopeePay;
  - a plan due on 3 November with no payment, so "Rencana jatuh tempo" opens on Transaksi.

  Account names are unique per owner, so a second "Darurat Kecil" under BCA was refused. The BCA Tabungan Rumah and Jago Tabungan pair stands in for it.

## 1. Baseline

| Check | Result |
| --- | --- |
| Backend pytest | 1735 passed, 0 failed |
| Vitest | 72 files, 637 passed |
| Type-check, lint, build | exit 0 each |
| First-load JS (gzip) | `/` 249.7 kB, `/ledger` 252.4 kB, `/insights` 250.5 kB, `/accounts` 254.0 kB |

**Screenshots:** 10 views × 1440 light, 1440 dark and 390 dark, 30 in all. They're in `scratchpad/web-routes/shots/baseline/` and show the defects:
- **Reversed inbound legs:** the Tabungan-filtered list titles the inbound legs "Tabungan → Gopay" and "Tabungan → Tabungan Rumah". The money moved the other way.
- **Unpaired income:** titled "Main Pocket", and its detail says "Dari: Main Pocket, Ke: Pasangan belum ditemukan". The money came into Main Pocket.
- **No banks:** no pocket names its bank.
- **Side panel:** every former drawer opens as a 391 px non-modal side panel at x=1049. At 1440 px it squeezes the list until "8 lagi di hari ini · Muat lagi" wraps one word per line.

## 2. Server: parent names

- **2.1** `backend/tests/test_movement_route_names.py` has 3 tests: linked legs on a pocket and on an account without a parent, the inbound leg alone in a filtered list, and an unpaired leg. The list and the detail agree on both fields. All 3 pass with the change and fail without it.
- **2.2** Snapshots were taken of the seeded ledger before and after the change, 262 responses each: every list page, the movement view, Beranda's window, each account filter, and every row's detail. With `account_parent_name` and `transfer_target_account_parent_name` removed, 0 responses differ. All 988 rows carry both fields: 749 with a parent name and 261 with a partner parent.

## 3. Client: one route rule

- **3.1 `movementRoute`** is tested in `components/beranda/activity.test.ts` (10 tests):
  - merged, inferred, outbound, inbound and unpaired rows, with and without parents;
  - a row from an older API without parent fields;
  - `movementDirection`.

  Consolidation carries the target's bank (`lib/ledgerMovements.test.ts`). On the old `activity.tsx`, the inbound case received "Tabungan → Gopay" against the expected "Gopay → Tabungan". It fails before the change and passes after.
- **3.2 Each surface:**
  - **Transaksi** (`app/ledger/page.test.tsx`, "Movement routes", 3 tests):
    - "BCA · ATM → Gopay";
    - "Gopay → Jago · Tabungan";
    - "? → Jago · Main Pocket" with "Pasangan belum ditemukan · Pindah uang dari BCA";
    - "Masuk dari Gopay" in a list filtered to the account;
    - "Keluar ke Gopay" in the phone feed;
    - the detail's Dari "Pasangan belum ditemukan" and Ke "Jago · Main Pocket".
  - **Beranda** (`app/page.test.tsx`): a linked pair across banks, an inbound leg without its partner, and an unpaired leg.
  - **Rekening detail** (`app/accounts/page.test.tsx`): "Masuk dari Gopay" and "Keluar ke BCA · Tabungan Rumah".

## 4. Client: centered dialogs

- **4.1 `Drawer`:** dialog mode above 760 px and sheet mode at 760 px and below (`components/workspace/overlays.test.tsx`, 13 tests in the file):
  - focus moves in, Tab is trapped, and Esc, the scrim or the close button returns focus to the opener;
  - the page scroll is locked and restored;
  - it is portalled to the body.

  **Stacked dialogs:** a `Modal` opened from the dialog owns Tab and Esc, then the dialog takes the keyboard back. The test fails without the topmost check in `useFocusTrap` and passes with it.
- **4.2 Callers:** the existing caller tests pass, and a new Beranda test opens "Rencana jatuh tempo" as `data-mode="dialog"`. In the browser, every caller was opened in the matrix (section 5): the transaction detail from Transaksi, Beranda and Kalender, "Rencana jatuh tempo" on Transaksi, and the Peta Aliran node and link. Each is centered with the form fields at full width. Beranda opens "Rencana jatuh tempo" only for automatically linked plans, which the seed can't produce with auto-linking off, so Beranda's opener is covered by Vitest and the shared component by the Transaksi run.
- **4.3 Dead code:** `rg` finds no `DrawerSlotContext`, `ws-drawer-slot`, `data-drawer`, `RAIL_QUERY` or `[data-mode="overlay"]` in `frontend/src`. The one exception is the new Beranda test that asserts the slot is gone. Lint passes.

**Defects found and fixed while verifying:**
- **Scroll lock.** `useScrollLock` and `Modal` set `overflow: hidden` on the body. With `overflow-x: clip` on the root, the body became its own clipping box: the page still scrolled behind the dialog, and the sticky sidebar jumped by the scroll offset. In a probe scrolled to 600 px, before the fix the sidebar moved to −600 and a wheel scrolled the page to 1400. After the fix the sidebar stays at 0 and the page stays at 600. The same holds for the Rekening `Modal`. The lock now sets the root element (`lockPageScroll` in `layers.ts`), and both dialogs use it.
- **Truncated routes.** Phone feed and Beranda titles cut long routes at one line ("? → Jago · Main Po…"). Titles now wrap to three lines in the phone feed and two on Beranda, and the feed's second line wraps. The 390 px screenshots show every route in full.

## 5. Verification

### 5.1 Full suites

| Check | Result |
| --- | --- |
| Backend pytest (`c315ad8`) | 1738 passed, 0 failed (1735 + 3 new) |
| `npm ci` | exit 0 |
| Type-check | exit 0 |
| Lint | exit 0, 0 warnings |
| Vitest | 73 files, 654 passed, 0 unhandled errors |
| Build | exit 0 |

### 5.2 Browser matrix

Results are in `scratchpad/web-routes/matrix/matrix.json`.

**Coverage:** 14 states at 1440, 1280, 1024, 768 and 390 px, in light and dark, plus a reduced-motion cell at 1440 dark. That's 154 cells.

**States:**
- Transaksi, all and filtered to a pocket;
- Beranda, Peta Aliran and Kalender;
- the Rekening detail;
- the transaction detail opened four ways: from a paired, an unpaired and an inbound row, and from Beranda;
- the transaction detail from Kalender;
- the Peta Aliran node and link;
- "Rencana jatuh tempo".

| Check | Result |
| --- | --- |
| Overflow | 0 cells |
| Console errors | 0 |
| Targets under 24 px | 0 |
| Running animations, reduced motion | 0 |
| Dialog cells | 97: 72 centered dialogs, 14 sheets (all at 390 px), 11 Rekening `Modal` |
| Side column present | 0 |
| Dialog off-centre (left ≠ right) | 0 |
| Tab leaving the dialog in 14 presses | 0 |
| Not closed by Esc | 0 |
| Focus not returned to the opener | 0 |
| axe serious or critical | 8 cells, all Kalender in dark, see below |

- **The 8 axe cells:**
  - **Finding:** `color-contrast` on the green "uang masuk" mark inside a heavily shaded Kalender day cell (3.87:1, #10b981 on #61383b). This change does not touch Kalender.
  - **On `main`:** the same check at `0a342a9` finds the same 3 nodes, so it is pre-existing and recorded as a follow-up.
- **Peta Aliran node at 390 px:** no target, by design. The phone shows the table instead of the map. The node's detail is reached through the link rows, which the `flow-link` state covers at 390 px.

### 5.3 Compare page

`scratchpad/web-routes/compare.html` has 30 pairs, baseline beside after, for 10 views at 1440 light, 1440 dark and 390 dark. The agent read the pairs:
- **Transaksi, unfiltered:** "BCA · ATM → Gopay", "Gopay → Jago · Tabungan", "? → Jago · Main Pocket · Pasangan belum ditemukan" and "ShopeePay → ?". "8 lagi di hari ini · Muat lagi" fits on one line again.
- **Filtered views:** filtered to Jago · Tabungan, "Masuk dari Gopay" and "Masuk dari BCA · Tabungan Rumah". On Rekening detail, "Masuk dari Gopay".
- **Beranda:** routes with banks, and "Pindah saldo · Pasangan belum ditemukan".
- **Dialogs:**
  - the details open centered at 720 px;
  - on the phone they open as sheets;
  - "Rencana jatuh tempo" and the Peta Aliran link detail also open centered.

### 5.4 Unchanged writes

Five flows ran in the dialog at 1440 px, on this branch and on `main`, from the same restored seed:
- edit notes;
- switch Pengeluaran to Masuk;
- delete;
- edit a movement in its dialog;
- "Lewati kali ini" on a due plan.

The requests:
- `PATCH /api/transactions/{id}` twice;
- `DELETE /api/transactions/{id}`;
- `PATCH /api/movements/{id}`;
- `POST /api/recurring/{id}/occurrences/skip`.

Method, path and body are byte-for-byte equal on both builds, 5 of 5. The captures are in `writes-main.json` and `writes-after.json`.

### 5.5 Size, specs and clean-up

- **First-load JS, gzip, against the baseline:**
  - `/` 249.8 kB (+0.1);
  - `/ledger` 252.6 kB (+0.2);
  - `/insights` 250.4 kB (−0.1);
  - `/accounts` 254.1 kB (+0.1).

  The limit is +5 kB.
- **Specs:** `openspec validate --all --strict` passes 80 of 80. `git diff --check` is clean.
- **Clean-up:**
  - every process was stopped by port (8272, 3272, 3273, 9572);
  - `cft-routes-pg` was removed, along with the browser profiles and the scratch `main` worktree;
  - `ps`, `lsof` and `docker ps` show nothing left.

## Not verified

- A screen reader, and a real touch device: the phone was emulated at 390 px with touch emulation.
- Beranda's "Rencana jatuh tempo" opener in the browser. It needs an automatically linked plan, so it's covered by Vitest.
- Production. The deploy check follows the merge.

## Follow-ups

- Kalender's income mark is under 4.5:1 on the darkest spending shade in dark mode. This is pre-existing and separate from this change.
