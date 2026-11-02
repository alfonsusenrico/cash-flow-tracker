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
