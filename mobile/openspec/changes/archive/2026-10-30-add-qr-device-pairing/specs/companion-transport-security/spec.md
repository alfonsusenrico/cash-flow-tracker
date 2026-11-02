## MODIFIED Requirements

### Requirement: Release builds use only encrypted connections
A release build of the companion SHALL refuse plain HTTP connections to any host, so every request to the tracker, including the API key and notification content, goes over HTTPS. A debug build MAY use plain HTTP so that a local backend on the LAN or the emulator host can be tested. The server SHALL be fixed by the build without a runtime URL field. Only debug builds SHALL accept the companionServerUrl Gradle property for local testing; release builds SHALL use the fixed HTTPS tracker.

#### Scenario: Release build with an HTTP server address
- **WHEN** a release build is configured with an `http://` server address and tries to sync
- **THEN** Android blocks the connection, nothing is sent, and the rows stay pending

#### Scenario: Release build with the HTTPS tracker
- **WHEN** a release build syncs with the default `https://` tracker address
- **THEN** the request is sent as before

#### Scenario: Debug build against a local backend
- **WHEN** a debug build is compiled with `-PcompanionServerUrl=http://10.0.2.2:8000` and syncs
- **THEN** the request reaches the local backend

#### Scenario: Merged manifests
- **WHEN** the release and debug manifests are merged
- **THEN** neither declares `usesCleartextTraffic`, both reference the network security configuration, the release configuration denies cleartext and the debug configuration allows it
