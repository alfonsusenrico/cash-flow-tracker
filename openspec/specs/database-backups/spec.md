# database-backups Specification

## Purpose
Keep restorable, verified copies of the production database on the host, outside the Docker volumes, so losing the database volume or a bad migration never loses users' books.

## Requirements

### Requirement: Nightly Verified Database Dump
The production stack SHALL take one database dump every day at the configured local time (default 02:30). It SHALL also take one at start when no dump has succeeded in the last 24 hours. Each dump SHALL be in PostgreSQL custom format, named with the database, UTC timestamp and reason, and verified by listing its archive contents before it counts as a success.

#### Scenario: Scheduled night
- **GIVEN** the backup service is running and the last success is less than 24 hours old
- **WHEN** the configured local time arrives
- **THEN** one dump named `cashflow-<database>-<UTC timestamp>-nightly.dump` SHALL be written and verified
- **AND** the next run SHALL be scheduled for the same local time on the following day

#### Scenario: Missed night caught up
- **GIVEN** no dump has succeeded in the last 24 hours
- **WHEN** the backup service starts
- **THEN** it SHALL take a nightly dump at once, before waiting for the next scheduled time

#### Scenario: Stopping the service
- **WHEN** the backup service is asked to stop while it waits
- **THEN** it SHALL exit within 5 seconds without waiting for the scheduled time

### Requirement: Atomic Dump Files
A dump SHALL appear under its final name only when it is complete, verified and, if configured, encrypted. It SHALL be written to a temporary file in the same directory, flushed and then renamed. A failed or interrupted run SHALL leave no dump under a final name and SHALL remove its temporary files.

#### Scenario: Dump fails
- **WHEN** the database refuses the connection during a run
- **THEN** no new dump file and no temporary file SHALL remain
- **AND** the run SHALL be reported as failed with the failing step

### Requirement: Dump Retention
After each verified dump, dumps older than the retention period (default 14 days) SHALL be deleted. The dump just written SHALL never be deleted. Retention SHALL NOT run after a failed dump.

#### Scenario: Old and recent dumps
- **GIVEN** a dump 15 days old and a dump 13 days old with the default retention
- **WHEN** a new dump succeeds
- **THEN** the 15-day-old dump SHALL be deleted and the 13-day-old dump SHALL remain

#### Scenario: Backups failing for weeks
- **GIVEN** every run has failed for more than the retention period
- **WHEN** another run fails
- **THEN** no existing dump SHALL be deleted

### Requirement: Host Backup Location
Dumps SHALL be stored in a host directory read from the runtime configuration (`BACKUP_HOST_DIR`), defaulting to `<deploy root>/production/backups`. The directory SHALL be absolute and outside the release tree and Docker's storage, owned by and private to the deploy user. Dump files SHALL be readable only by that user.

#### Scenario: Default location
- **WHEN** no backup directory is configured
- **THEN** dumps SHALL be written to `<deploy root>/production/backups`, created with mode 0700, as files with mode 0600 owned by the deploy user

#### Scenario: Unsafe location
- **WHEN** the configured directory is relative, contains `:`, is `/`, is the environment root, or lies inside the release tree or Docker's storage
- **THEN** the deploy SHALL stop before building or backing up, naming the variable

### Requirement: Backup Log Limits
Backup and restore logs SHALL contain only the outcome, the reason, the file name, the size in bytes, the duration, the number of pruned dumps, and on failure the failing step and exit code. Every backup log line SHALL also record the free space, in MiB, of the filesystem that holds the dumps, or `unknown` when it cannot be read. They SHALL NOT contain connection settings, passwords, tool output or data.

#### Scenario: Failed run with a wrong password
- **WHEN** a run fails because the database rejects the password
- **THEN** the log SHALL hold one failure line naming the `dump` step
- **AND** no log line SHALL contain the password

#### Scenario: Free space on every line
- **WHEN** a backup run succeeds or fails
- **THEN** its outcome line SHALL end with `free_mib=` and the free space of the dump directory in MiB

### Requirement: Optional Encryption at Rest
When an age public key is configured (`BACKUP_AGE_RECIPIENT`), each verified dump SHALL be stored only in encrypted form (`.dump.age`), and its plaintext temporary file SHALL be deleted. Without a key, dumps SHALL be stored unencrypted with private file permissions. An invalid key SHALL fail the run before any dump is taken.

#### Scenario: Key configured
- **WHEN** a dump succeeds with an age public key configured
- **THEN** only a `.dump.age` file SHALL be written for it
- **AND** it SHALL restore with the matching private key

### Requirement: Restore Into a New Database
The project SHALL provide a restore command that loads a dump (plain, or encrypted with a supplied identity file) into a database that does not exist yet. It SHALL restore in one transaction, and drop the new database if the restore fails. It SHALL refuse an existing target and report the restored Flyway version. A row-count command SHALL print exact counts per table and no row contents.

#### Scenario: Round trip
- **GIVEN** a database migrated V1 to the latest version with synthetic rows, and a dump of it
- **WHEN** the dump is restored into a new database on a fresh PostgreSQL 16 server
- **THEN** every table SHALL have the same row count as the source
- **AND** Flyway SHALL report the same schema version and validate successfully

#### Scenario: Existing target
- **WHEN** a restore names a database that already exists
- **THEN** it SHALL fail with `target-exists` and leave that database unchanged

### Requirement: Low Free Space Warning
A backup run that ends with less free space on the dump directory than `DEPLOY_MIN_FREE_GB` GiB (default 5, the deploy's own threshold) SHALL log a `status=warning` line with the step `disk-space` and the threshold. The warning SHALL NOT skip, fail or delete a dump. A threshold that is not a whole number from 1 to 9999 SHALL fail the run before the dump with the step `config-min-free`.

#### Scenario: Run below the threshold
- **WHEN** a backup run completes while the dump directory has less free space than the threshold
- **THEN** the dump SHALL be written as usual
- **AND** the log SHALL add `status=warning`, `step=disk-space`, the threshold and the free space

#### Scenario: Run above the threshold
- **WHEN** a backup run completes with more free space than the threshold
- **THEN** the log SHALL hold no disk-space warning

#### Scenario: Invalid threshold
- **WHEN** `DEPLOY_MIN_FREE_GB` is 0
- **THEN** the run SHALL fail with `step=config-min-free` and write no dump
