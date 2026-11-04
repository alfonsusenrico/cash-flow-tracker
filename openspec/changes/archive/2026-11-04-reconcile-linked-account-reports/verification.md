# Verification: reconcile-linked-account-reports

**Revisions:**
- `4d35bb9`: regression tests and V47 on top of the plan, before any rule code.
- `9790d9a`: one more test, added before the rule: a late report after its kept leg paired.
- `6a305c1`: backend rule, API, seen-twice and the retired patterns.
- `0f03153`: web and user guide.
- This commit: the record.

**Environment:**
- Local only: Postgres 16 in `cft-linked-pg` (55612), Flyway 10 to V47.
- API on 8312; `next start` on 3313 behind the loopback proxy on 3312; headless Brave on 9612.
- Synthetic owners only: the test fixtures, and the `raka_contoh` seed of the earlier routes check.
- Production was read once, read-only: 0 skip patterns, 0 skipped events (2026-11-04).

## 1. Regression tests first (task 1.1)

On `4d35bb9`, with V47 applied and no rule code, 14 of the 45 new or changed tests failed for the expected reasons:

| Failing test | Why it failed |
| --- | --- |
| linked account skips the Jago report (both orders) | no link rule |
| late report within five minutes is skipped | no link rule |
| possible transfer under a link is not skipped | the plain guard held it |
| two top-ups each absorb one report | no link rule |
| acknowledging learns the account link once, links the named account, learns nothing from a typed row | the response had `pattern`, not `link` |
| skipped reports are listed and recorded separately | the item had no `link` |
| deferred pass applies a link once | no link rule |
| link API: list and stop, other owner 404, companion 403, pattern endpoints gone | the routes did not exist |

The other changed tests already held on the old code, as they should: without a link the guard asks, a real Jago income records, other amounts and senders don't match, stopped and other owners' links don't decide, and a doubt beyond the window. The test "late report after the kept leg paired" was added before the rule and was not run separately on the old code.

## 2. Automated checks (task 4.1)

| Check | Revision | Result |
| --- | --- | --- |
| Backend full suite | `0f03153` (backend unchanged since `6a305c1`) | 1799 passed, 0 failed, 0 skipped |
| Targeted: duplicate arrivals and account links | `6a305c1` | 46 passed |
| V47 applied, then the file run again | `4d35bb9` | applied; the second run changes nothing |
| V47 conversion (`test_account_links.py`) | `6a305c1` | resolvable pattern becomes one `converted` link and is stopped; unresolvable pattern is stopped without a link; a second run adds nothing |
| Offline benchmark (`test_notification_cases.py`, `test_notification_benchmark.py`) | `6a305c1` | 105 passed |
| Frontend `npm ci`, `tsc --noEmit`, `eslint --max-warnings=0`, `npm run build` | `0f03153` | all exit 0 |
| Vitest | `0f03153` | 74 files, 669 passed, 0 unhandled errors |
| `openspec validate --all --strict` | this commit | see section 6 |
| `git diff --check` | this commit | clean |

## 3. Browser check (task 4.2)

**Scenario:** built through the real ingest pipeline (deterministic mode) on the seed, with GoPay and Jago reports:
- a 120k mirror waiting in "Terlihat dua kali";
- a 500k mirror confirmed with "Benar, satu transaksi", which created the link "Gopay juga dilaporkan Bank Jago";
- a 250k Jago report posted 3 minutes after GoPay's, skipped by the link.

| Cell | View | axe serious/critical | Overflow | Console errors | Targets under 24 px |
| --- | --- | --- | --- | --- | --- |
| 1440 light, 1440 dark, 390 dark | Tinjau · Terlihat dua kali | 0 | 0 | 0 | 0 |
| same | Tinjau · Dilewati otomatis | 0 | 0 | 0 | 0 |
| same | Rekening detail of Gopay | 0 | 0 | 0 | 0 |

**Read on the screenshots:**
- "Rekening yang sama · Gopay juga terlihat di Bank Jago", and "Benar, satu transaksi · Rekening ditautkan; laporan serupa dilewati otomatis".
- "Rekening terhubung · Gopay juga dilaporkan Bank Jago" with only "Catat terpisah".
- "Juga dilaporkan · Bank Jago · Lepaskan".

Screenshots and `report.json` are in the session scratchpad, `linked/shots/`.

## 4. Unchanged behaviour without links (task 4.3)

**Method:**
- The same seed was restored twice: at V46 for `main` (`6b505b6`, its own code) and at V47 for this branch.
- One identical ingest sequence ran through each: the 1 Nov sequence, a reversed mirror, two unrelated same-amount credits, a payment, and a GoPay → Jago transfer.
- The output was normalised, without ids or times: per-event results, event states, legs, every account balance and the "Terlihat dua kali" count.

**Result:** the two outputs are byte-identical (10 results, 10 events, 13 legs, 2 waiting items).

## 5. Owner phone test cases (task 4.4)

Use small real amounts. Start with no link, because production has none. Check each result in Tinjau, Transaksi and Rekening & Kantong.

1. **First time, GoPay Tabungan top-up.** Move Rp 1.000 from BCA to GoPay Tabungan (by Jago).
   - **Expect:** one movement BCA → Gopay, and Jago · Main Pocket unchanged. Jago's "received from GoPay" waits in Tinjau → "Terlihat dua kali", showing "Rekening yang sama · Gopay juga terlihat di Bank Jago".
   - **Then:** answer "Benar, satu transaksi". The toast says "Gopay juga dilaporkan Bank Jago. Laporan serupa dilewati otomatis.", and Gopay's detail lists "Juga dilaporkan · Bank Jago".
2. **Linked, normal order.** Repeat case 1.
   - **Expect:** one movement and no Tinjau question. Jago's report appears under "Dilewati otomatis" with "Rekening terhubung · Gopay juga dilaporkan Bank Jago".
3. **Linked, Jago's report first.** If Jago happens to notify before GoPay, the result is the same as case 2. The Jago row disappears once GoPay's report arrives, and Main Pocket is unchanged. Don't edit the Jago row in between, because an edited row is kept and GoPay's report waits in Tinjau instead.
4. **Late notification.** If one app notifies up to 5 minutes late, the result is still the same as case 2. Beyond 5 minutes, both are recorded and flagged for review in Tinjau, and nothing is skipped silently.
5. **A real Jago income.** Receive Rp 1.000 into Jago from someone else, or from GoPay with no GoPay credit before it.
   - **Expect:** it is recorded as a normal income on Jago and is not skipped.
6. **BCA → Jago transfer.** Move Rp 1.000 from BCA to a Jago pocket.
   - **Expect:** one movement BCA → Jago (pocket), unaffected by the link.
7. **Pocket move inside Jago.** Move Rp 1.000 between two Jago pockets.
   - **Expect:** one movement between the pockets, with nothing in "Terlihat dua kali".
8. **Same amount, unrelated.** Make two payments of the same small amount a few minutes apart, from different apps or shops.
   - **Expect:** both are recorded as two expenses, with nothing skipped.
9. **Undo and unlink.**
   - On a skipped report, choose "Catat terpisah". It becomes its own transaction, and "Urungkan" skips it again.
   - In Gopay's detail, choose "Lepaskan" and confirm. The next case-1 top-up asks in Tinjau again.

## 6. OpenSpec

- `openspec validate --all --strict`: 80 passed, with 0 warnings.
- An archive rehearsal on a scratch copy: 79 passed, with 0 warnings and no placeholder Purpose. The first rehearsal flagged "One Arrival Seen by Two Apps" at 531 characters, so its clause was shortened to 484 without changing its meaning.

## 7. Not verified

- A real phone, which the owner tests with section 5.
- A screen reader.
- The live model. The AI prompt is unchanged, so there is no evaluation cost.
