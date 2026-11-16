# Verification: separate-transaction-name-and-note

Tested on 2026-11-16, branch `feat/transaction-name-and-note` on base `9e5be43`. The backend rename was done by a delegated agent and reviewed here; the web and companion changes were done here.

## Checks

| Check | Result |
| --- | --- |
| Backend, full suite (Postgres 16, Flyway V1→V57) | 2363 passed |
| `git grep -nE "\bt\.notes\b\|transactions\.notes\|INSERT INTO transactions[^;]*\bnotes\b" -- backend/` | no match (two multi-line `INSERT … notes` remain, in tests that run against a pre-V57 scratch schema) |
| Frontend type check, lint, tests (vitest), build | 0 errors, lint passed, 1110 passed, build passed |
| Companion `testDebugUnitTest`, `compileDebugAndroidTestKotlin` | 649 tests (648 plus 1 new), 0 failures; compiles |
| `openspec validate --all --strict` | 86 passed, 0 failed (86 items) |

## Migration on a realistic dataset

The prototype's synthetic dataset was loaded at V51 (`seed_parity.py`) and migrated to V57. Result: 188 transactions; the 85 with text keep it as `name`; `note` is empty on all 188.

## Scenario evidence

| Scenario | Evidence |
| --- | --- |
| A remark on an event's spending | `tests/test_transaction_name_and_note.py` (create, update, detail); web: `rows.test.tsx` "keeps the owner's note out of the title", `ManualView.test.tsx` (name and note sent separately); captures `nn-transaksi-*`, `nn-rincian-*` |
| The AI never writes the note | `test_transaction_name_and_note.py`: the notification and text-capture model inputs hold no note, and the new rows' note is null |
| Search finds a note | API test; on the running app, `?q=fellowship` lists both noted rows (capture `nn-cari-catatan-*`) |
| Migrated rows | V57 test in a scratch schema; the dataset run above |
| An older companion resolves an event | API tests for `notes` only, and for `name` + `notes` (name wins), on create, update, resolve and bulk rename; companion: `CaptureModelTest` reads `name`, `note` and the old `notes` |

## Browser check

**Setup:** a production build (`next start`), the backend and a loopback proxy, with synthetic data. Three rows were given a name and/or a note through the API.

**Coverage:** the ledger, search by note, the transaction detail, the edit dialog, and Catat with its details open; at 1440 × 900 and 390 × 844, light and dark (20 captures).

**Automated findings:** 0 serious or critical axe violations, 0 console errors.

**Found and fixed during the check:**
- the note clipped without an ellipsis, and took width from the title on desktop. The note now shrinks first and ends with an ellipsis;
- the search placeholder now names names and notes.

**Remaining:** on desktop a long title still shares the narrow title column with the note. The full title shows in the detail and on the phone.

**Evidence:** `cash-flow-tracker-prototypes/research/evidence/separate-transaction-name-and-note/`.

## Not verified

- **Notification rows in the companion:** the detail does not show the owner's note, because the event result carries the recorded name only. Notes are added on the web.
- Production behaviour after deploy.

## Acceptance

The owner accepted the result on 2026-11-16 ("accept, push it and open the PR"). The production check and the companion release follow the merge and deploy.
