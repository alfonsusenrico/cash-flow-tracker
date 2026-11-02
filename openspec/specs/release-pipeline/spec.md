# release-pipeline Specification

## Purpose
Gate every production release on the checks that prove it, protect the database before it is migrated, and keep the release from touching anything on the shared host that it does not own.

## Requirements

### Requirement: Database Tests Gate the Release
The checks that run on branch pushes and pull requests, and as the production preflight, SHALL run the full backend suite against a PostgreSQL 16 database. That database SHALL first be migrated V1 to the latest version with the same Flyway image and settings as production. An unreachable test database SHALL fail the run instead of skipping database tests. Checks SHALL run on free GitHub-hosted runners.

#### Scenario: Preflight on main
- **WHEN** a commit is pushed to main
- **THEN** the deploy SHALL start only after the backend suite passed on the migrated database, and after the frontend and operations checks passed

#### Scenario: Database missing in CI
- **WHEN** the backend suite runs with the database requirement set and the database is unreachable
- **THEN** the run SHALL fail rather than skip the database tests

### Requirement: Mobile-Only Changes Skip the Server Deploy
A push to main whose changed files are all under `mobile/` SHALL NOT start the server deploy or its checks. A push that also changes any other path SHALL deploy as usual.

#### Scenario: Mobile-only push
- **WHEN** a push to main changes only files under `mobile/`
- **THEN** the production deploy workflow SHALL NOT run

### Requirement: Backup Before Migration
A production deploy SHALL take and verify a database dump after building the new images and before any Flyway preflight, repair or migration. If the dump fails, the deploy SHALL stop before migrating and SHALL leave the running release and the `current` pointer unchanged.

#### Scenario: Backup succeeds
- **WHEN** a deploy runs
- **THEN** the order SHALL be build, pre-migration dump named with the commit, Flyway preflight, migration, then service start

#### Scenario: Backup fails
- **WHEN** the pre-migration dump fails
- **THEN** the deploy SHALL exit with an error saying it aborted before any migration
- **AND** no Flyway preflight, migration, service start, promotion or image cleanup SHALL run

### Requirement: Scoped Image Cleanup
Release cleanup SHALL remove only dangling images this project built. Those are the images labelled `cash-flow-tracker.managed=true`, or labelled by Compose with this deployment's project name. Cleanup SHALL NOT remove unlabelled images, other projects' images or tagged images. A failed cleanup SHALL be reported as a warning without failing the promoted release.

#### Scenario: Shared host
- **GIVEN** dangling images with this project's label, with this Compose project's label, with no label, and with another project's label, plus a tagged image with this project's label
- **WHEN** release cleanup runs
- **THEN** only the two dangling images of this project SHALL be removed

### Requirement: Free Disk Space Before Deploy
Before a production deploy writes the release tree, builds images, takes the pre-migration dump or migrates, it SHALL check that Docker's data directory and the backup directory each have at least `DEPLOY_MIN_FREE_GB` GiB free. The threshold SHALL default to 5 and SHALL accept only whole numbers from 1 to 9999. Below it, the deploy SHALL stop with a message naming the free space and the threshold.

#### Scenario: Disk below the threshold
- **GIVEN** a running release that `current` points to
- **WHEN** a deploy finds 285 MiB free on Docker's data directory with the default threshold
- **THEN** the deploy SHALL exit with an error naming "285 MiB free" and "DEPLOY_MIN_FREE_GB=5 GiB"
- **AND** no release directory, image build, pre-migration dump, Flyway preflight, migration, service start or image cleanup SHALL happen
- **AND** `current` SHALL still point to the running release

#### Scenario: Enough free space
- **WHEN** both directories have at least the threshold free
- **THEN** the deploy SHALL log the free space of each and continue with the build, the pre-migration dump, the Flyway preflight and the migrations in that order

#### Scenario: Configured threshold
- **WHEN** `DEPLOY_MIN_FREE_GB` is 8 and the backup directory has 6 GiB free
- **THEN** the deploy SHALL stop before the build, naming "6144 MiB free" and "DEPLOY_MIN_FREE_GB=8 GiB"

#### Scenario: Invalid or unreadable values
- **WHEN** `DEPLOY_MIN_FREE_GB` is not a whole number from 1 to 9999, or the free space or Docker's data directory cannot be read
- **THEN** the deploy SHALL stop before the build, the dump and the migrations, and SHALL NOT treat the check as passed
