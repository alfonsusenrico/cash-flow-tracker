# Tasks

## 1. Sky blue for every transfer

- [x] 1.1 D1: inventory every transfer and movement element on the web (`Pindah`, `transfer`, `movement`, `isMovement`, amber, blue and sky utilities). Evidence: the table in design.md D1, and the source check in `tokens.test.ts` over Beranda, Transaksi, `MobileLedgerFeed` and `MobileHomeView`.
- [x] 1.2 D2: add `--color-transfer`, `--transfer-text` and `--transfer-contrast`, map `text-sky-400` to `text-sky-700` to `--transfer-text`, and add the `transfer` Tailwind colour. Evidence: the built CSS has `.text-sky-500,.text-sky-700{color:var(--transfer-text)}`, `.bg-transfer{background-color:var(--color-transfer)}` and `.text-transfer-contrast{color:var(--transfer-contrast)}`, and `.bg-sky-500\/10` keeps `rgb(14 165 233/.1)`.
- [x] 1.3 D1 and D3: move the Beranda "Pindah Saldo" button, movement rows and detail amount, and the `HeroWalletCard` capsule and tile, to sky, and the Transaksi movement dot to `--color-transfer`. Amber stays on warnings. Evidence: the browser run finds no amber or blue element in a transfer context after the change, and the remaining amber is the "Rencana" icon, the decorative card chips and the "Tunai" share.
- [x] 1.4 Extend `frontend/src/app/tokens.test.ts`:
  - the `--transfer-text` and `--transfer-contrast` pairs in both themes;
  - `--transfer-text` on the sky tints in both themes;
  - the sky mapping and the pinned fills;
  - the source check of every movement branch and "Pindah Saldo" element.

  Evidence: 8 of 45 fail on the previous tokens, config and components, and 45 of 45 pass now.
- [x] 1.5 Headless Brave on `/`, `/ledger` and `/accounts` at 390 and 1280 px, in light and dark, before and after. Evidence in verification.md:
  - axe `color-contrast` on transfer elements goes from 8 nodes to 0;
  - every measured transfer text is at least 4.5:1 and every icon at least 3:1;
  - no overflow and no console errors;
  - 12 screenshots before and 12 after.

Expected Result: every transfer element reads as sky blue in both themes, its text passes 4.5:1, amber means warnings only, and every fill keeps its colour.

## 2. Free disk space guard

- [x] 2.1 D4 and D5: check the free space of Docker's data directory and `BACKUP_HOST_DIR` before `release_storage.py prepare`, with `release_storage.py min-free-gb` reading and validating `DEPLOY_MIN_FREE_GB` (default 5). Evidence: the new cases in `backend/tests/test_deploy_release_script.py`. 11 of 23 fail on the previous script, and 23 of 23 pass now:
  - low disk: refused before the release directory, build, backup, preflight, migration, start and prune, with `current` unchanged;
  - configured threshold on the backup directory: refused the same way;
  - exactly at the threshold: proceeds;
  - six invalid values and unreadable free space: refused before the build.
- [x] 2.2 D5: wire `DEPLOY_MIN_FREE_GB` through `materialize_runtime_env.py`, `deploy.yml`, the backup service in `docker-compose.yml` and `.env.example`. Evidence: `docker compose config --quiet`, with and without the backup profile, exits 0, and actionlint reports no new finding.
- [x] 2.3 D6: `backup.sh` adds `free_mib` to every log line, warns below the threshold, and refuses an invalid threshold as `config-min-free`. Evidence: `scripts/verify/backup_restore_roundtrip.sh` passes 33 checks, including:
  - `free_mib` on the success and failure lines;
  - no warning at 5 GiB;
  - a warning with the dump still written at 9999 GiB;
  - `config-min-free` with no dump at 0.
- [x] 2.4 D7: document the guard, its threshold and "What to do when the disk is full", including not touching other projects' images without the owner's say, in `docs/production-deployment.md` section 6 and `docs/database-backups.md`. Evidence: the two documents in the diff.
- [x] 2.5 shellcheck on the deploy script, its remote part, `backup.sh` and the round-trip script, and actionlint on the workflows, through Docker, against origin/main. Evidence: no new finding (verification.md).

Expected Result: a deploy with too little free space stops before it builds or writes anything, the running release stays online, and the backup log shows the free space and warns when it is low.

## 3. Verification

- [x] 3.1 Backend suite on a fresh PostgreSQL 16 database migrated V1→V42 by `flyway/flyway:10`, with `SKIP_TEST_MIGRATIONS=1` and `REQUIRE_TEST_DATABASE=1`, in a Python 3.12 venv from `backend/requirements-dev.txt`. Evidence: counts in verification.md.
- [x] 3.2 Frontend `npm ci`, test (`TZ=Asia/Jakarta`), lint, type-check and build. Evidence: counts in verification.md.
- [x] 3.3 `git diff --check` and `openspec validate --all --strict`. Evidence: exit 0.

Expected Result: local checks pass on the exact working tree that is committed.

## Workflow follow-up

- Archive the change with `openspec archive unify-transfer-colour-and-guard-disk`.
- Push `fix/transfer-colour-and-disk-guard` and report the Checks workflow result and the compare link. The owner opens and merges the pull request.
