# Verification: fix-movement-completion-duplicates

Date: 2026-10-29. Base is origin/main 2716468, plus the working tree of `fix/movement-completion-duplicates`, which was then committed. All data is synthetic.

| Check | Result |
|---|---|
| Fresh `postgres:16`, Flyway V1→V32 | 32 migrations applied |
| Full backend suite | 907 passed, 0 skipped (901 before; 6 new) |
| New completion and queue tests against origin/main code | 4 of the 7 selected tests failed: the 3 completion tests and the queue test. The window/amount boundary cases passed, as expected. |
| Frontend `npm test` | 212 passed (210 before; 2 new, with axe 0 violations on the candidate list) |
| lint, type-check, build | exit 0 |
| `openspec validate --all --strict` | exit 0 |
| `git diff --check` | clean |

**Not verified:**
- A browser pass of the candidate list. The change is a fieldset of radio rows with two buttons, covered by Testing Library and axe.
- Production data correction. It is recorded in `PROJECT_STATE.md`, and the owner corrects any duplicate in the ledger.
