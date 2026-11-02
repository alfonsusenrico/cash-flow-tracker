# Verification

All runs were local, on 2026-11-02, on the working tree of `fix/transfer-colour-and-disk-guard` (from origin/main 6d0df31) before its commit. Data was synthetic. Nothing touched the production server.

## Backend

- Database: a fresh PostgreSQL 16 container (`cft-sb-db`), migrated V1→V42 by `flyway/flyway:10` with the settings of `checks.yml` ("Successfully applied 42 migrations", schema version 42).
- Python 3.12 venv from `backend/requirements-dev.txt`, with `SKIP_TEST_MIGRATIONS=1` and `REQUIRE_TEST_DATABASE=1`.
- `python -m pytest tests/ -ra`: **1513 passed**, 0 failed, 0 skipped, 1 warning.
- `tests/test_deploy_release_script.py` alone:
  - previous `deploy_remote_release.sh`: 11 failed, 12 passed;
  - new script: 23 passed.

## Frontend

- `npm ci`: exit 0.
- `npm run type-check`: exit 0.
- `npm run lint`: exit 0, 0 errors, 1 warning. The warning is the existing unused `_done` in `src/app/review/page.tsx`, which this change does not touch.
- `TZ=Asia/Jakarta npm test`: **61 files, 383 tests passed**.
- `src/app/tokens.test.ts` alone: on the previous tokens, config and components, 8 of 45 fail. With this change, 45 of 45 pass.
- `npm run build`: exit 0. The built CSS has:
  - `.text-sky-500,.text-sky-700{color:var(--transfer-text)}`;
  - `.bg-transfer{background-color:var(--color-transfer)}`;
  - `.text-transfer-contrast{color:var(--transfer-contrast)}`;
  - `.bg-sky-500\/10{background-color:rgb(14 165 233/.1)}`, the fill unchanged;
  - `.text-amber-500,.text-amber-600,.text-amber-700{color:var(--warning-text)}`, the warning ink unchanged.

## Browser

Setup:
- headless Brave over CDP on debug port 55744;
- `next dev` on 127.0.0.1:55743 and the API on 127.0.0.1:55742, against a separate fresh V42 database;
- one synthetic owner with income, expenses and one "Pindah Saldo" of Rp 200.000 (`Tarik tunai contoh`);
- `/`, `/ledger` and `/accounts` at 390 and 1280 px, light and dark: 12 views before and 12 after.

The "before" run used the previous web files and the same data.

| View | axe `color-contrast` before → after | Lowest sky, blue or transfer text before → after | Lowest such icon before → after |
| --- | --- | --- | --- |
| `/` 390 light | 2 → 0 | 2.77 → 7.56 | 2.38 → 6.50 |
| `/` 390 dark | 0 → 0 | 6.81 → 8.82 | 5.54 → 7.17 |
| `/` 1280 light | 2 → 0 | 3.68 (blue) → 6.84 | 2.38 → 6.50 |
| `/` 1280 dark | 0 → 0 | 5.13 → 7.77 | 4.62 → 7.17 |
| `/ledger` 390 light | 2 → 0 | 2.51 → 6.84 | 2.51 → 6.84 |
| `/ledger` 390 dark | 0 → 0 | 6.01 → 7.77 | 6.01 → 7.77 |
| `/ledger` 1280 light | 2 → 0 | 2.51 → 6.84 | none |
| `/ledger` 1280 dark | 0 → 0 | 6.01 → 7.77 | none |
| `/accounts` 390, both themes | 0 → 0 | none | none |
| `/accounts` 1280 light | 2 → 2 | none | none |
| `/accounts` 1280 dark | 2 → 2 | none | none |

Findings:
- **Transfer contrast.** Before, all 8 `color-contrast` nodes on `/` and `/ledger` were transfer elements: the sky and blue movement amounts, the "Pindah Saldo" badge, "Tergabung" and the target account. After, there are none.
- **Accounts.** The 4 nodes on `/accounts` are the pre-existing muted text at 75% opacity (design.md, Follow-ups). They are unchanged.
- **Colour on `/` at 1280 px.**
  - Before: the "Pindah Saldo" capsule and text button were amber (5.92:1 and 6.39:1 in light), the movement rows were blue, and the capsule's amber tile measured 2.15:1.
  - After: all of them are sky. The capsule measures 6.84:1 light and 7.77:1 dark, the text button 7.56:1 and 8.82:1, and the tile's icon 7.01:1 and 9.07:1.
- **Amber.** No amber or blue element remains in a transfer context. The amber left on these pages is the "Rencana" icon and the decorative card chips at 390 px, and the "Tunai" share on `/accounts`.
- **Layout and console.** Horizontal overflow was 0 px and console errors 0 in all 24 runs.
- **Screenshots.** `before/*.png` and `after/*.png` (12 each) were kept in the local scratch folder. They are not committed.

## Operations

- `scripts/verify/backup_restore_roundtrip.sh` with `VERIFY_PREFIX=cft-sb`: **33 PASS, 0 FAIL**, "Round trip verified: 32 tables, Flyway version 42". The checks include:
  - `free_mib=` on the success line (`… pruned=1 free_mib=54296`) and on the failed-dump line;
  - no disk-space warning at the default threshold;
  - `DEPLOY_MIN_FREE_GB=9999`: the dump is written, then `status=warning reason=low-space step=disk-space min_free_gb=9999 free_mib=54296`;
  - `DEPLOY_MIN_FREE_GB=0`: `status=failed reason=bad-threshold step=config-min-free` and no dump;
  - no password in any log.
- `docker compose -f docker-compose.yml config --quiet`, without and with `--profile backup`: exit 0. With `DEPLOY_MIN_FREE_GB=7` set, the backup service receives `DEPLOY_MIN_FREE_GB: "7"`.
- `materialize_runtime_env.py --allow-missing` with an empty environment writes `DEPLOY_MIN_FREE_GB=5`, and `release_storage.py min-free-gb` reads 5 from it.
- shellcheck (`koalaman/shellcheck:stable`, Docker) on `deploy_remote_release.sh`, its remote heredoc extracted as a script, `db/backup/backup.sh` and the round-trip script:
  - origin/main: 7 findings;
  - this change: the same 7. All are `SC2086` and `SC2029` notes on the existing local `ssh` and `scp` lines, and there is no new finding.
- actionlint (`rhysd/actionlint:latest`, Docker) on `deploy.yml` and `checks.yml`: the same 2 existing `SC2086` info findings in the deploy workflow's "Setup SSH Key" step as on origin/main, and no new finding.

## Repository

- `git diff --check`: exit 0.
- `openspec validate --all --strict`: 70 passed, 0 failed.

## Not verified

- The guard on the real server. There was no production access. Its first real run is the next deploy, whose log should show the two `💽` lines.
- `docker info` and `df` on `msi` as the deploy user. Both were replaced by shims in the tests.
