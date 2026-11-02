# Proposal

## Why

Two small fixes, one visual and one operational:

- **Transfer colour.** `tactile-pulse-ui` says sky blue means an internal transfer ("Pindah Saldo"). The web paints transfers in three colours:
  - amber: the desktop Beranda "Pindah Saldo" text button and the `HeroWalletCard` "Pindah Saldo" capsule;
  - blue: the movement rows and the detail amount on desktop Beranda;
  - sky: the phone-width home view and Transaksi.

  Sky text (`#0EA5E9`) also measures 2.77:1 on white, and axe reports it on `/` and `/ledger` in the light theme.
- **Full disk.** A production deploy stopped safely because the server disk was full: 285 MB were free, and the pre-migration backup could not be written. Nothing checked the free space before the build, and the backup log did not show how much was left.

## Approval

Recorded 2026-11-02:

1. **Transfers.** The owner said "sky blue for transfers". This answers the open decision "Transfer colour in the spec" of `clear-pillar-and-remaining-contrast` (archived 2026-11-02).
2. **Disk guard.** The deploy above aborted safely, and the owner approved the cleanup on the server. This guard is the recommended prevention. It falls under the owner's earlier "ok I approve all you recommend" for operations safety (Phase 0, 2026-11-02).

Both follow the Phase 0 approval of the same day: the product is public and multi-user, and amounts are never guessed.

## Expected Outcome

- On Beranda, Transaksi and the phone-width home view, every "Pindah Saldo" action, movement row, movement amount and movement badge is sky blue, in the light and the dark theme. Amber appears only for warnings, such as "Waspada".
- Sky text measures at least 4.5:1 on every surface and sky tint it sits on in both themes. axe reports no contrast violation for a transfer element on `/`, `/ledger` or `/accounts` at 390 and 1280 px.
- A deploy checks the free space of Docker's data directory and of the backup directory before it builds or writes anything. Below `DEPLOY_MIN_FREE_GB` (default 5 GiB), it stops with a message that names the free space and the threshold. The running release stays online and `current` is unchanged.
- Every backup log line records the free space left. A run below the same threshold also logs a disk-space warning.
- The runbooks describe the guard and what to do when the disk is full, including not touching other projects' images without the owner's say.

## What Changes

- Web tokens: `--color-transfer` (sky fill), `--transfer-text` (`#075985` light, the fill in dark) and `--transfer-contrast` (ink on the solid fill). Tailwind maps `text-sky-400` to `text-sky-700` to `--transfer-text`, and adds `bg-transfer`, `text-transfer` and `text-transfer-contrast`. The sky fills keep their colour.
- Beranda: the "Pindah Saldo" text button, the movement rows' icon and amount, and the detail amount move to sky.
- `HeroWalletCard`: the "Pindah Saldo" capsule moves to sky, and its tile uses `bg-transfer` with `text-transfer-contrast`.
- Transaksi: the movement category dot uses `--color-transfer` instead of a hard-coded hex.
- `scripts/deploy_remote_release.sh`: a free-space check before the release tree, the build, the backup and the migrations.
- `scripts/release_storage.py`: a `min-free-gb` command that reads and validates `DEPLOY_MIN_FREE_GB`.
- `db/backup/backup.sh`: `free_mib` on every log line, a `disk-space` warning below the threshold, and `config-min-free` for an invalid threshold.
- Configuration: `DEPLOY_MIN_FREE_GB` in `materialize_runtime_env.py`, `deploy.yml`, `docker-compose.yml` (backup service) and `.env.example`.
- Docs: `docs/production-deployment.md` section 6 and `docs/database-backups.md`.

## Non-goals

- Other colour families, and amber where it means a warning. Amber on non-transfer elements (the investment pill on `HeroWalletCard`, the "Tunai" share on Rekening, the "Rencana" icon) stays as it is (design.md, Open decisions).
- Non-transfer sky uses (the "Pilar Kakeibo" card, the investment amount on the phone-width home view, two information notes). They keep their sky fill. Their text now reads with the transfer ink.
- The pre-existing `color-contrast` findings on `/accounts` at 1280 px (muted text at 75% opacity). They are unrelated to transfers (design.md, Follow-ups).
- Cleaning the server. Nothing in this change touches the server or deletes anything. The guard only reads free space.
- Android code. `mobile/` is untouched.
- Production access or release. All verification is local, on disposable PostgreSQL and shimmed deploy runs with synthetic data. Pushing the branch is authorised. The pull request, merge and release stay with the owner and the approved pipeline.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `tactile-pulse-ui`: sky blue for every transfer element, a sky text ink, and amber only for warnings.
- `release-pipeline`: a free-space check before the build (added).
- `database-backups`: free space on every log line (modified), and a low-space warning (added).

Standards:
- deployed: the guard fails closed and runs before any write. The threshold is validated, and the runbook gives the recovery steps.
- money: no amount, balance or formula changes. Only colours change.
- personal-data: backup logs gain only a number of MiB, never paths of data, settings or secrets.
- multi-user: no change to owner scoping.

No new project capability is added.

## Impact

- Web: `tokens.css`, `tailwind.config.js`, `app/page.tsx`, `app/ledger/page.tsx`, `components/dashboard/HeroWalletCard.tsx`. Tests: `app/tokens.test.ts`.
- Operations: `scripts/deploy_remote_release.sh`, `scripts/release_storage.py`, `scripts/materialize_runtime_env.py`, `db/backup/backup.sh`, `docker-compose.yml`, `.github/workflows/deploy.yml`, `.env.example`. Tests: `backend/tests/test_deploy_release_script.py` and `scripts/verify/backup_restore_roundtrip.sh`.
- Docs: `docs/production-deployment.md`, `docs/database-backups.md`.
- Database: no migration.
- Owner action: optional. Set the GitHub secret `DEPLOY_MIN_FREE_GB` only to change the 5 GiB default.
