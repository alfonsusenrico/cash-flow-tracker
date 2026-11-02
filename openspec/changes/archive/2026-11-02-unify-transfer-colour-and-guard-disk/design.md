# Design

## Context

Two approvals of 2026-11-02 meet in one small change on `fix/transfer-colour-and-disk-guard`, branched from origin/main 6d0df31:

- The owner answered the open decision of `clear-pillar-and-remaining-contrast` with "sky blue for transfers".
- A production deploy aborted safely on a full disk (285 MB free, the pre-migration backup could not write). The owner approved the cleanup. This change adds the recommended prevention under "ok I approve all you recommend".

The colour work follows the token approach of `fix-light-theme-text-contrast` and `clear-pillar-and-remaining-contrast`:
- a text ink of the same hue, mapped in Tailwind's `textColor` so fills stay as they are;
- a contrast ink for text and icons on the solid fill;
- pairs in `tokens.test.ts`.

## Decisions

### D1. Inventory of transfer elements

Found by searching for `Pindah`, `transfer`, `movement`, `isMovement` and the amber, blue and sky utilities in `frontend/src`:

| Place | Element | Before | After |
| --- | --- | --- | --- |
| `app/page.tsx` (desktop Beranda) | "Pindah Saldo" text button in "Komposisi Dompet & Rekening" | `text-amber-600 dark:text-amber-400` | `text-sky-500` |
| `app/page.tsx` | movement row icon tile in "Aktivitas Terkini" | `bg-blue-500/10 text-blue-500` | `bg-sky-500/10 text-sky-500` |
| `app/page.tsx` | movement row amount | `text-blue-500` | `text-sky-500` |
| `app/page.tsx` | movement amount in "Detail Transaksi" | `text-blue-500` | `text-sky-500` |
| `HeroWalletCard` | "Pindah Saldo" capsule | amber tint, `text-amber-700 dark:text-amber-400` | `bg-sky-500/10`, `text-sky-500`, `border-sky-500/30` |
| `HeroWalletCard` | its icon tile | `bg-amber-500 text-white` (2.15:1) | `bg-transfer text-transfer-contrast` |
| `app/ledger/page.tsx` | movement category dot | inline `#0ea5e9` | `var(--color-transfer)` |
| Transaksi, `MobileLedgerFeed`, `MobileHomeView` | movement badge, icon, amount, "Tergabung", target account, "Transfer" action | `text-sky-500` (2.77:1 on white) | unchanged classes; the ink comes from D2 |

Checked and left alone, because they are neutral or not transfers:
- the Quick Capture "Perpindahan" tab, `InternalMovementModal` and the "Jadikan Pindah Saldo" bar, which use neutral or lime colours;
- the recurring rules, which do not colour transfer rules;
- the decorative amber "card chip" on the phone-width account cards;
- the blue "Rutin" icon, "Akun Induk" badge, "Invest" badge and the "Bank" share on Rekening.

### D2. Sky text ink

Sky-500 (`#0EA5E9`) measures 2.77:1 on white. As with emerald and amber, sky text gets a text ink of the same hue:

| Token | Light | Dark |
| --- | --- | --- |
| `--color-transfer` | `#0EA5E9` (sky-500) | `#38BDF8` (sky-400) |
| `--transfer-text` | `#075985` (sky-800, hue 201°) | `var(--color-transfer)` |
| `--transfer-contrast` | `#0A0D14` | `#0A0D14` |

`#075985` in light measures 7.56:1 on `--surface`, 6.37:1 on `--bg`, 5.62:1 on `--surface-raised`, and at least 4.80:1 on 10, 15 and 20% sky-500 tints over the card or the raised surface. `#38BDF8` in dark measures 8.82:1, 9.46:1 and 7.79:1 on the three surfaces, and at least 5.65:1 on the same tints. `--transfer-contrast` measures 7.01:1 on `#0EA5E9` and 9.07:1 on `#38BDF8`.

`tailwind.config.js` maps `text-sky-400` to `text-sky-700` to `--transfer-text` in `theme.extend.textColor`, so every sky fill, tint and border keeps Tailwind's sky. It also adds `transfer` (`bg-transfer`, `text-transfer`, `text-transfer-contrast`). `text-transfer` reads the text ink, so a later `text-transfer` cannot paint the low-contrast fill as text.

Rejected:
- `#0369A1` (sky-700): 4.41:1 on the raised surface.
- `#066196`: passes every surface but measures 4.39:1 on a 15% tint over the raised surface.
- Mapping `bg-sky-*` to the token. Tailwind 3 cannot apply an opacity modifier (`bg-sky-500/10`) to a `var()` colour, and the fills are not the contrast problem.
- Changing each class to a new `text-transfer` utility. The global mapping also lifts sky text the inventory did not name, so a later `text-sky-500` cannot regress.

### D3. Amber means warnings

Amber is removed from the two "Pindah Saldo" elements only. Every warning stays amber: "Waspada" and other badges, the reconciliation flag, the merge-mismatch alert, the pending-plan banner and the review notes. `--warning-text` and the amber mapping are unchanged.

### D4. Where the free-space check runs

The check is the first step on the server, in the remote part of `deploy_remote_release.sh`, before `release_storage.py prepare`. At that point only the bootstrap files exist in `/tmp`. Before it, nothing is written into the release tree, no image is built, no dump is taken and nothing is migrated. A refused deploy therefore leaves the running containers, the release tree and `current` as they were.

The check reads two filesystems:
- **Docker data**: `docker info --format '{{.DockerRootDir}}'`. Images and build cache land there.
- **Backups**: `BACKUP_HOST_DIR`, resolved by the existing `release_storage.py backup-dir` from the uploaded `runtime.env`. The resolution moved up from after the extraction, with the same validation. The dumps land there.

Free space comes from `df -Pk`, the POSIX format whose fourth field is the space available to the deploy user, in KiB. Each check fails closed:
- if `docker info` fails or prints nothing, the deploy stops;
- if `df` output is not a number, the deploy stops;
- below `DEPLOY_MIN_FREE_GB × 1024 × 1024` KiB, the deploy stops with:

  `❌ Not enough free disk space for <label> at <path>: <n> MiB free, below DEPLOY_MIN_FREE_GB=<g> GiB. Aborting before any build, backup or migration; the running release is untouched.`

At or above the threshold, it logs `💽 <label>: <n> MiB free at <path> (minimum <g> GiB).`

Rejected:
- Checking after the build. The build itself can fill the disk.
- Checking in the GitHub workflow over SSH. The deploy script is the one place that runs on the server.
- Pruning automatically when space is low. The host is shared, and deleting needs the owner's say (D7).

### D5. The threshold

`DEPLOY_MIN_FREE_GB` is a whole number of GiB from 1 to 9999, default 5:
- Zero would disable the guard.
- A fraction or a suffix such as `5GB` is ambiguous.

`release_storage.py min-free-gb --env-file` reads the last value in `runtime.env`, like `BACKUP_HOST_DIR`, and validates it. An empty value means the default. An invalid value stops the deploy before anything else, without echoing the value. "GB" in the name means GiB (1024³ bytes), the unit `df -k` divides into.

It reaches `runtime.env` the same way as the backup settings:
- the GitHub secret of the same name in `deploy.yml`, defaulting to `'5'`;
- `ENV_DEFAULTS` in `materialize_runtime_env.py`;
- `.env.example` for local stacks.

### D6. Free space in the backup log

`backup.sh` appends `free_mib=<MiB>` to every log line, read with `df -Pk` on `BACKUP_DIR`, or `unknown` when `df` cannot read it. After the outcome line of a run, whether it succeeded or failed, a run below the threshold adds:

`status=warning reason=<reason> step=disk-space min_free_gb=<g> free_mib=<n>`

The warning never skips, fails or prunes a dump. A night with little space still takes its dump if it fits. The backup service receives the same `DEPLOY_MIN_FREE_GB` through `docker-compose.yml`. An invalid value fails the run as `config-min-free`, like the other settings. The deploy has already refused such a value before the service could start with it.

The line holds only a number. There are no paths, settings or tool output, so the log limits of `database-backups` still hold.

### D7. Runbook

`docs/production-deployment.md` section 6 covers:
- what the guard checks, and the log lines;
- the abort message;
- "What to do when the disk is full": look first (`df`, `docker system df`, `du`), then remove only what this project owns:
  - its dangling images, with the deploy's two label filters;
  - releases past the retained three, through `release_storage.py cleanup`;
  - dumps past retention, never the newest verified one.

  It says plainly not to touch other projects' images, containers, volumes or build cache without the owner's say. It also forbids an unfiltered prune and lowering the threshold to get a deploy through.

`docs/database-backups.md` gains the new log fields, the warning line, the threshold row and a pointer to section 6. Running any of these commands on the server still needs the owner's explicit instruction.

### D8. Tests

Web (`app/tokens.test.ts`), 8 new cases, all failing on the previous code:
- the `--transfer-text` and `--transfer-contrast` pairs in both themes;
- `--transfer-text` on the sky tints in both themes;
- the sky mapping, with the sky fill and the transfer fills pinned;
- a source check that every movement branch on Beranda, Transaksi, `MobileLedgerFeed` and `MobileHomeView` uses `text-sky-500` and no amber or blue, and that the "Pindah Saldo" button, capsule, tile and ledger dot use sky.

Deploy (`backend/tests/test_deploy_release_script.py`): the shim harness gains a `df` shim and a `docker info` answer. New cases:
- low disk (285 MiB): exits before the release directory, build, backup, preflight, migration, start and prune, with `current` still on the seeded previous release;
- a configured threshold of 8 GiB, with 50 GiB on Docker data and 6 GiB on the backup directory: Docker data passes, and the backup directory refuses the deploy the same way;
- exactly 5 GiB free: proceeds, logs both free-space lines, and checks before the build;
- six invalid thresholds and unreadable free space: refused before the build;
- `resolve_min_free_gb` defaults and last-value rule.

The existing full-order test now also asserts that both `df` calls come before the build.

Backup (`scripts/verify/backup_restore_roundtrip.sh`, run in CI):
- the success and failure lines end with `free_mib=`;
- no warning at the default threshold;
- `DEPLOY_MIN_FREE_GB=9999` writes its dump and logs the warning;
- `DEPLOY_MIN_FREE_GB=0` fails as `config-min-free` without a dump.

## Risks

- `text-sky-500` and `bg-sky-500` now paint different colours in light, as with emerald and amber. The Tailwind comment and the mapping test state it.
- Light sky text is visibly deeper (sky-800). It keeps the sky hue, and every sky fill is unchanged.
- Non-transfer sky text also takes the transfer ink (D2 non-goals). Its contrast only improves.
- A full disk now stops a deploy that might have fitted, for example with 4 GiB free. Lowering the threshold through the secret is possible but the runbook advises freeing this project's space first.
- If Docker's data directory sits on a filesystem the deploy user cannot stat, `df` fails and the deploy stops. On `msi` the deploy user runs Docker already, so `docker info` works. This was not tried on the server (no production access).

## Migration

None. The web change ships with the normal release. The guard is active on the next deploy, with the 5 GiB default and no secret needed. Rollback is a revert.

## Open decisions for owner

1. **Investment colour.** The spec says lavender for investment top-ups. The web shows the investment balance in amber on the `HeroWalletCard` pill and in sky on the phone-width home view, and the Beranda "Invest" badge in blue. This change keeps them, because the approval covered transfers only. Should investment use lavender everywhere, in a later change?
2. **Other sky uses.** The "Pilar Kakeibo" card on Beranda and two information notes (Target & Tagihan progress note, "Ditautkan otomatis") are sky but not transfers. Keep them, or move them to a neutral colour so sky means transfers only?

## Follow-ups

- `/accounts` at 1280 px: two `color-contrast` findings in each theme on muted text at 75% opacity ("0 cicilan aktif" and the runway basis note). They existed before this change and are unrelated to transfers.
- The "Rencana" icon on the phone-width home view is amber, which reads as a warning. A neutral or lime icon would keep amber for warnings only.
- The guard was exercised with shims and the backup image locally. Its first real run will be the next production deploy, and the deploy log should show the two `💽` lines.
