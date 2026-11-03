# Verification

The verification record of `redesign-transactions-workspace`. It is filled during implementation and
uses synthetic data only. Each result names the commit it was measured at.

## 1. Baseline (task 1.1, at `<commit>`)

| Check | Result |
| --- | --- |
| `npm test` | |
| `npm run type-check` / `lint` / `build` | |
| `/ledger` first-load JS (raw / gzip) | |
| Shared first-load JS (raw / gzip) | |
| Backend suite (pass / fail / skip) | |
| `GET /api/transactions` median of 20, no filter / `period=cycle` (5 000 rows) | |
| axe on `/ledger`, edit modal, `QuickCaptureModal` (1280, 390; light, dark) | |

## 2. URL defect (tasks 1.2, 3.1)

| Step | Before (URL, list request) | After |
| --- | --- | --- |
| `/ledger?type=income` → "Semua" | | |
| `/laporan?…` → plain `/laporan` | | |
| Sidebar "Transaksi" from `/ledger?type=income` | | |
| Stepper and type chips from plain `/ledger` | | |
| Newest Next.js 16.2 patch (if the registry is reachable) | | |

## 3. Backend (tasks 2.1–2.6, at `<commit>`)

- Day totals, tab counts and `local_date`:
- New filters:
- Capture parse (no writes, no model, rate limit, logs):
- Delete paths:
- Timings and statement count:

| Measurement | Result | Gate |
| --- | --- | --- |
| `GET /api/transactions` without `include` | | ≤ baseline + 10% |
| With `include=daily_totals,type_counts` | | < baseline + 25 ms, 4 statements |
| `POST /capture/parse` | | < 50 ms |

- Full suite (pass / fail / skip, with skip reasons):

## 4. Frontend (tasks 3.1–5.5, at `<commit>`)

- Vitest (files / tests):
- type-check, lint, build:
- Bundle: `/ledger` (≤ +15 kB gzip), shared (≤ +3 kB gzip):
- Removed code (`rg` output):

## 5. Browser (tasks 6.2–6.5, production build of `<commit>`)

### Matrix

| Width | Theme | Page or state | axe serious/critical | Lowest contrast | Overflow | Console errors | Screenshot |
| --- | --- | --- | --- | --- | --- | --- | --- |

- Zoom 200% and 400%:
- Reduced motion:
- Keyboard walk:

### Real-interface flows (task 6.3)

| Flow | Before | After | After undo |
| --- | --- | --- | --- |

### Regression (task 6.5)

## 6. OpenSpec and diff

- Plan commit: `openspec validate redesign-transactions-workspace --strict` is valid. `openspec validate --all --strict` gives 73 passed and 1 failed, the existing main-spec warning of `notification-purpose-questions`. On a scratch copy, `openspec archive redesign-transactions-workspace -y` followed by `--all --strict` gives 73 passed, exit 0.
- `openspec validate --all --strict` at the final commit:
- `git diff --check`:

## Not verified

## For the owner's final check (after all phases)
