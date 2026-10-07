# Framework adoption follow-up — 2026-10-07

## Outcome and authority

Owner approved recovering the unmerged adoption branch in a persistent sibling worktree, archiving the historical design, replacing Bob hooks and compacting state in the owner checkout. This supersedes the earlier temporary-worktree and pending-hook/archive decisions. No application code, CI, production, settings/secrets, notes or other worktree files changed. No merge or deployment occurred.

Restored branch from b5851a94b66277c7441be23e47d6fbdc9dfedcc9 at ../cash-flow-tracker-adopt. Commit only there until .githooks reaches other branches: core.hooksPath is shared but each worktree needs its own hook files. Durable PROJECT_STATE.md and JOURNAL.md reside in the owner's main checkout, not the worktree.

## Pruned metadata

`git worktree prune --dry-run --verbose`, then `git worktree prune --verbose` removed metadata for five verified absent folders: cash-flow-tracker-adopt, cft-cookie-baseline, cft-jago-source-pocket, cft-secure-cookies and cft-sender-clarification (formerly under /private/tmp). No existing folder was removed. cash-flow-tracker-topup-label remains registered and intact. `git worktree add ../cash-flow-tracker-adopt chore/adopt-agent-framework` restored the adoption branch.

## Historical design

`git mv design.md research/archive/2026-05-design-locked-system.md`. The first line identifies it as superseded and links to ../../frontend/tokens.css. The original document body is preserved apart from making its existing token reference a working relative link. AGENTS.md now links to the archive and retains live tokens as authority. Older evidence describes its original tested revision rather than today's archive status.

## Hooks and notes

Full .git/hooks backup: ~/.local/state/cash-flow-tracker/hooks-backup-2026-10-07/, directory mode 0700. Verified byte-identical backup before removing exactly pre-commit, post-commit, post-checkout and post-merge. Their Bob helpers were inline; no extra helper file needed removal. All *.sample hooks remain.

Read-only `git ls-remote origin 'refs/notes/*'` exited 0 with no advertised references. `git notes --ref=bob list | wc -l` returned **5**. Notes were not deleted or modified; this proves current remote state, not whether notes were ever published historically. No note contents were read.

Installed using `/Users/enrico/enrico/agent-framework/tools/af hooks install . --apply`. Generated executable .githooks/commit-msg and .githooks/pre-push; core.hooksPath=.githooks. No hook bypass flags were used.

Hook proof, directly against temporary message files, without throwaway commits:

- `Update hooks`: commit-msg returned **1**, rejected.
- `chore(hooks): install framework hooks`: commit-msg returned **0**, accepted.

## Durable private state

Backup: ~/.local/state/cash-flow-tracker/state-backup-2026-10-07/PROJECT_STATE.original.md, mode 0600 under a 0700 directory. Original state: **100,939 bytes**; compact state: **4,040 bytes** at compaction, delivery receipts kept below 8 KB. Backup was byte-compared before overwrite. Dated history is copied newest first into local JOURNAL.md, mode 0600; both state files are ignored through .git/info/exclude. Personal historical context stays private and is not copied into tracked evidence.

Owner checkout remains fix/investment-topup-label at 08b0bbd3cb3ae6774618522045c956151593a752. Its 11-entry dirty status and 27 file hashes were captured before operations and must match at delivery. Only the two authorized untracked state files were rewritten there; authorized shared Git metadata changed separately.

## Verification

Application source tested is unchanged from b5851a9. Tests ran with the design/hook working-tree changes present; final commits add only documentation and generated hooks.

- `af project check .`: **0 errors / 0 warnings**.
- `openspec validate --all`: **44 passed / 0 failed**, exit 0 (errors gate).
- `openspec validate --all --strict`: **44 passed / 0 failed**, exit 0; **no warnings**. No specs were reshaped during this follow-up.
- Backend: **716 passed, zero skipped**, 18.45 s, exit 0. New loopback-only PostgreSQL 16 container cft-adoption-followup-test-db; verified empty before applying all **26 migrations in numeric order**.
- Frontend: **148 tests / 34 files passed**, exit 0; type-check, lint and production build passed.
- `git diff --check`: passed. Tracked follow-up scope is archived design, instruction/documentation updates, research evidence and .githooks only.

Backend command from the adoption root:

```sh
TEST_DATABASE_URL=postgresql://postgres@127.0.0.1:5548/ledger_test SKIP_TEST_MIGRATIONS=1 PYTHONPATH=backend /Users/enrico/enrico/project/enrico/cash-flow-tracker/backend/.venv/bin/python -m pytest backend/tests -q --tb=short
```

SKIP_TEST_MIGRATIONS bypasses only the host fixture after independent numeric-order migration application; no database tests were skipped. Existing local stacks were not used for test data. Frontend commands from frontend/: npm ci --no-audit --no-fund, npm test, npm run lint, npm run type-check, npm run build. npm's existing blocked install-script policy remained intact. Local test outputs are cft-adoption-followup-*.log under /private/tmp; durable commands/results are recorded here and in the owner's state.

Production login Secure-cookie confirmation and earlier mobile acceptance remain owner confirmations, outside this adoption. Existing CI/deployment, migration, token/preset, tooling and repeatable browser evidence follow-ups remain unchanged. Owner decides whether to retain historical notes. Commit and remote-tip receipts are recorded in the owner's PROJECT_STATE.md and delivery reply.
