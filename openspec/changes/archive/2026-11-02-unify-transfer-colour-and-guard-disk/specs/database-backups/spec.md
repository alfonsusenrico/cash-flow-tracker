## MODIFIED Requirements

### Requirement: Backup Log Limits
Backup and restore logs SHALL contain only the outcome, the reason, the file name, the size in bytes, the duration, the number of pruned dumps, and on failure the failing step and exit code. Every backup log line SHALL also record the free space, in MiB, of the filesystem that holds the dumps, or `unknown` when it cannot be read. They SHALL NOT contain connection settings, passwords, tool output or data.

#### Scenario: Failed run with a wrong password
- **WHEN** a run fails because the database rejects the password
- **THEN** the log SHALL hold one failure line naming the `dump` step
- **AND** no log line SHALL contain the password

#### Scenario: Free space on every line
- **WHEN** a backup run succeeds or fails
- **THEN** its outcome line SHALL end with `free_mib=` and the free space of the dump directory in MiB

## ADDED Requirements

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
