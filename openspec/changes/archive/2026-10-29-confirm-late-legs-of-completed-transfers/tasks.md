## 1. Correct late legs

- [x] 1.1 Confirm a completed leg before recording an observed leg (D1). Evidence: `test_late_leg_confirms_a_completed_transfer_instead_of_recording_again[2|600]` and `test_two_completed_transfers_hold_the_late_leg`. All three fail on origin/main 30e52db (held as ambiguous / 3 rows instead of 2 / recorded) and pass now.

Expected Result: a late leg never records a completed transfer twice.

## 2. Verify and deliver

- [x] 2.1 Full backend suite on a fresh V1→V32 database, and `openspec validate --all --strict`. Evidence: verification.md.
- [x] 2.2 Archive, push `fix/late-leg-after-completion`, report the compare link.
