# Verification: confirm-late-legs-of-completed-transfers

Date: 2026-10-29. Base is origin/main 30e52db, plus this branch's working tree. All data is synthetic.

| Check | Result |
|---|---|
| Reproduction on origin/main code | A +10-minute Jago leg created a third row (3 instead of 2). A +2-second leg was held as `ambiguous_existing_movement`. With two completed transfers, the late leg was recorded. |
| New tests on the fix | 4 selected tests pass (both delay cases, the two-transfer hold, plus one more matching test) |
| Full backend suite on a fresh V1→V32 database | 910 passed, 0 skipped (907 before; 3 new) |
| Frontend | not run: no frontend file changed |
| Production | not touched. Read-only evidence found one completed transfer whose Jago leg is pending a pocket question; it resolves after deploy when the owner answers it. |
