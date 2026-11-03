# Verification: redesign-money-and-settings

Fill this in during implementation. Each entry names the commit or working-tree state it was measured on. Data is synthetic only.

## 1. Rebase and baseline (tasks 1.1–1.3, at `<commit>`)

- **Rebase:** onto `main` at `<commit>`. Phases 3, 4 and 6 merged as `<PRs>`. MODIFIED blocks refreshed: `<list>`.
- **Alias measurement after the rebase:**

  | Kind | Count | Largest consumers |
  | --- | --- | --- |
  | `var()` alias uses | | |
  | Legacy Tailwind keys and palette remaps | | |
  | Legacy utility classes | | |

- **Baseline:**

  | Check | Result |
  | --- | --- |
  | `npm test` / type-check / lint / build | |
  | First-load JS `/accounts`, `/goals`, `/settings/*` | |
  | Built CSS | |
  | Backend suite (passed / failed / skipped) | |
  | `GET /api/dashboard/net-worth` median (20 calls) | |
  | `GET /api/accounts` median (20 calls) | |
  | axe, overflow, console per route at 1280 / 390, light / dark | |

- **Unchanged-writes harness on `main`:** a table with one row per flow, giving the requests recorded, the rows snapshotted and the artefact path.

## 2. Backend (tasks 2.1–2.4, at `<commit>`)

| Area | Tests | Result |
| --- | --- | --- |
| Net worth helper (2.1) | | |
| `GET /api/habits` (2.2) | | |
| Capture undo reverts its label (2.3) | | |
| Full suite (2.4) | | |

Timing of `GET /api/habits` with 1000 labels over 60 keys and 200 top-up answers: median `<ms>`, with `<n>` statements for 10 and for 1000 labels.

## 3. Frontend (tasks 3.1–6.9, at `<commit>`)

| Area | Tests | Result |
| --- | --- | --- |
| Rekening & Kantong (3.1–3.4) | | |
| Target & Tagihan (4.1–4.3) | | |
| Pengaturan (5.1–5.4) | | |
| Tokens and aliases (6.1–6.7) | | |
| Copy register (6.8) | | |

- **Alias-to-token mapping:** the table in design.md D11, with the decision on each raw-valued alias.
- **Removed components:** each one with the `rg` evidence that it has no importer.
- **Label renames (old → new):** the full list.

## 4. Browser (tasks 7.2, 7.4, 7.6, production build of `<commit>`)

### Matrix on `/accounts`, `/goals` and `/settings/*`

| Route / state | Width | Theme | axe serious+critical | Min contrast | Overflow | Console | Screenshot |
| --- | --- | --- | --- | --- | --- | --- | --- |

### Keyboard, zoom and reduced motion

### Pixel comparison of every route against the baseline

| Route | 1280 light | 1280 dark | 390 light | 390 dark | Explanation |
| --- | --- | --- | --- | --- | --- |

### Cross-page consistency

| Figure | Beranda | This page | Equal |
| --- | --- | --- | --- |

## 5. Unchanged writes (task 7.3)

| Flow | Requests equal | Rows equal | Notes |
| --- | --- | --- | --- |

## 6. Gates (task 7.5)

| Gate | Baseline | After | Limit | Pass |
| --- | --- | --- | --- | --- |

## 7. OpenSpec and diff (task 7.1)

- `openspec validate --all --strict`:
- `git diff --check`:

## Not verified

- Production. This is recorded in PROJECT_STATE.md after the deploy.
- Screen readers, unless run. Semantics are checked with axe and role queries.
