## Verification: catch-second-report-after-pairing

**Revision tested:** the working tree on `fix/seen-twice-after-pairing`, based on `81e0f4d` (main). The tested tree is the one committed.

**Data:** synthetic only. Production facts in the proposal and design come from read-only checks the owner authorised on 2026-11-05. No production data is stored in the repository or the tests.

### Reproduction (task 1.1)

- I wrote `test_gopay_then_mybca_then_jago_books_one_movement` in the 5 Nov order: GoPay at 0 s, myBCA at 1.1 s, Jago at 1.3 s.
- On the unfixed `movement_pairing.py` it **failed**: the Jago credit was booked as an extra income on Kantong Utama.
- With the fix it passes. The Jago event waits in `/api/review/queue` `seen_twice`.

### Red before, green after

I restored `main`'s version of each source file and ran the new tests against it. Each new test failed:
- **Backend:** `test_gopay_then_mybca_then_jago_books_one_movement`, `test_a_paired_leg_is_never_replaced`, `test_possible_transfer_without_a_link_is_not_held` and `test_deferred_pass_holds_a_report_whose_original_paired_later` (4 failed).
- **Frontend:** `Question rows follow the money's direction` (1 failed).

All of them pass with the fix.

### Suites

| Check | Result |
| --- | --- |
| `pytest tests/ -ra`, with CI's pinned tools from `requirements-dev.txt`, on PostgreSQL 16 migrated V1 → V49 by Flyway 10 | 1843 passed, 0 failed, 0 skipped |
| `backend/tests/test_duplicate_arrivals.py` | 44 passed (40 existing, 4 new) |
| `npm run type-check` | exit 0 |
| `npm run lint` | exit 0 |
| `npm test` (vitest) | 76 files, 685 tests passed |
| `npm run build` | exit 0 |
| `openspec validate --all --strict` | 81 passed, 0 failed |
| `git diff --check` | clean |

### Browser (task 3.3)

**Setup:**
- a local stack on loopback: the API, `next start` and a proxy;
- the synthetic seed database migrated to V49, with the AI off and outbound network blocked;
- the real ingest pipeline fed by synthetic notifications.

**Notifications sent:**
- the 5 Nov order (GoPay Rp10.000, myBCA, then Jago);
- a lone Jago "You've received Rp75.000 from Dewi Lestari.".

**Pipeline results:**
- **Second report:** the Jago report in the 5 Nov order became a `duplicate_observation`, with nothing booked.
- **Lone income:** the lone income was booked with a purpose question.

**Tinjau at 1440 px and 390 px, light and dark:**
- the income question row reads "Pemasukan · Belum dikenali · Uang masuk" with "+Rp 75.000" in the income colour;
- the expense row still reads "−Rp 21.701";
- "Terlihat dua kali 1" lists the held report;
- axe found no serious or critical violations, the console had no errors, and nothing overflows horizontally.

The screenshots stay in the session scratchpad and are not committed.

### Not verified

- **Real phone:** no run on the real phone. The owner's next real BCA → GoPay top-up after the release is the field check. Since 5 Nov, the owner's account also has a Gopay ↔ Jago app link, so that case is now decided by the link path. The unlinked path is proven by the tests and the browser scenario.
- **Production:** not checked from the branch. The release goes through the merge and the deploy workflow.
