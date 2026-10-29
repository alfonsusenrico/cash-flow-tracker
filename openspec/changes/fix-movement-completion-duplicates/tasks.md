## 1. Correct the queue and completion

- [x] 1.1 Require the stored reason `awaiting_counterpart` for the review queue's awaiting section (D1). Evidence: `test_review_queue_waits_only_for_flagged_legs` fails on origin/main and passes now.
- [x] 1.2 Look for an existing leg before creating one in `complete_movement`, with `create_new` and idempotent linking (D2, D3). Evidence: the `test_movement_pairing.py` completion tests (link existing, candidates then create_new, two matches refused, window/amount boundaries) — 3 of them fail on origin/main and pass now.
- [x] 1.3 Tinjau lists candidates with "Hubungkan" and "Tetap buat baru" (D4). Evidence: two new `app/review/page.test.tsx` tests including axe.

Expected Result: older one-sided rows leave the awaiting section, and completing can no longer record a transfer twice.

## 2. Verify and deliver

- [x] 2.1 Full backend suite on a fresh V1→V32 database, frontend test/lint/type-check/build, `openspec validate --all --strict`. Evidence: verification.md.
- [ ] 2.2 Read-only production check of earlier completions for duplicate legs, reported to the owner (no production edit).
- [ ] 2.3 Archive, push `fix/movement-completion-duplicates`, report the compare link.
