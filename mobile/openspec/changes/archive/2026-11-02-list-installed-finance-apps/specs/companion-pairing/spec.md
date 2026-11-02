## ADDED Requirements

### Requirement: Setup names only installed finance apps
The notification-access step of setup SHALL name only the registered finance apps installed on this phone, each label once, read again whenever setup returns to the foreground. When none is installed it SHALL show the same short line as Settings instead of the names.

#### Scenario: Two apps installed
- **WHEN** only myBCA and the Gojek app are installed and setup shows the notification-access step
- **THEN** the step names "myBCA" and "Gojek" and no other app

#### Scenario: No finance app installed
- **WHEN** no registered package is installed
- **THEN** the step says "Belum ada aplikasi bank atau e-wallet yang aku kenal di HP ini. Begitu kamu pasang, nanti muncul di sini."
