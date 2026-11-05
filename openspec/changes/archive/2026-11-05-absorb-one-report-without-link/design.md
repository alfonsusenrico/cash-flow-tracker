## Context

`linked_arrival` lets a kept leg absorb at most one report per linked institution (OD133). It reads the leg's earlier duplicate observations (`absorbed_packages` in `LINKED_REPORT_SQL`). When a link declines for that reason, `same_arrival` falls through to the unlinked guard.

Until `catch-second-report-after-pairing` D1, the unlinked guard saw only unpaired legs. A leg that had absorbed a report was usually paired by then, so the missing rule never showed. Since D1, the guard also sees paired legs, and it re-matches them.

**Replay trace, synthetic restatement of Sesi uji step 10:**
1. Jago 08:50:24.297: `linked_arrival` returns None (step 7's GoPay leg has already absorbed a Jago report). `same_arrival` returns step 7's GoPay leg as the original.
2. GoPay 08:50:24.913: no original.
3. The report is held as a duplicate of the wrong top-up.

**With a throwaway patch that adds absorb-once to the unlinked guard:**
- step 10 becomes a link skip;
- no other step changes;
- the replay scores 18 of 18.

## Decisions

### D1. The unlinked guard applies absorb-once

`SAME_ARRIVAL_SQL` selects `absorbed_packages` with the same subquery as `LINKED_REPORT_SQL`. `same_arrival` drops a candidate whose absorbed packages include the new leg's institution, before the mirrored, close and possible-transfer checks.

The dropped leg is not a doubt either: it has already been decided against its own report.

Rejected alternatives:
- **Skip the unlinked guard whenever a link exists for the account.** An owner without links keeps the bug, and a linked owner loses the guard for other institutions.
- **Narrow the window for paired legs.** Real top-ups can be seconds apart. Absorb-once is the actual invariant: one arrival has one report per app.

## Risks / Trade-offs

- **[An app that repeats its own report twice, as a re-post, for one arrival.]** The companion and the ingest idempotency already deduplicate re-posts by key and payload. A true second report from the same app is a new arrival, which is the point of this rule.
- **Rollback:** revert the release. No data changes.
