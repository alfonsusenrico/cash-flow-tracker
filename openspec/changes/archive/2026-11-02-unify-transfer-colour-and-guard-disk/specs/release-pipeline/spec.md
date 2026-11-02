## ADDED Requirements

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
