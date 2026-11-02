## MODIFIED Requirements

### Requirement: Every registered app can be toggled
Settings SHALL list every registered package installed on this phone once, with its registry label and a toggle, read again whenever Settings opens or the app returns to the foreground. A package with case variants SHALL count as installed when any spelling is. With none installed, Settings SHALL say so in one short line. The default for each package SHALL be unchanged: BCA mobile and Shopee are off, and the others are on.

#### Scenario: Gojek super-app can be disabled
- **WHEN** the owner turns off `com.gojek.app` in Settings
- **THEN** the listener SHALL ignore notifications from `com.gojek.app` and keep capturing from `com.gojek.gopay`.

#### Scenario: Only installed apps are listed
- **WHEN** only `com.bca.mybca.omni.android` and `com.gojek.app` are installed
- **THEN** Settings SHALL list "myBCA" and "Gojek" once each and no other registered app

#### Scenario: No finance app installed
- **WHEN** no registered package is installed
- **THEN** Settings SHALL show "Belum ada aplikasi bank atau e-wallet yang aku kenal di HP ini. Begitu kamu pasang, nanti muncul di sini." and no toggle

#### Scenario: App installed later
- **WHEN** the owner installs `com.stockbit.android` after opening the companion and then opens Settings again
- **THEN** Settings SHALL list "Stockbit"

## ADDED Requirements

### Requirement: Capture does not depend on what is installed
The listener SHALL decide whether a package is a registered financial app, and whether it is captured, from the registry and the saved toggle or its default only. What Settings or setup lists SHALL NOT change capture.

#### Scenario: Notification from an app that was not listed
- **WHEN** `com.gojek.gopay` was not installed when Settings last opened, is installed later, and posts a notification while its toggle was never changed
- **THEN** the listener SHALL capture it

#### Scenario: Saved toggle survives installation
- **WHEN** the owner turned off `com.gojek.app`, uninstalled it, and installs it again
- **THEN** the listener SHALL keep ignoring its notifications

### Requirement: Package visibility is limited to registered apps
The manifest SHALL declare every registered package spelling in `<queries>` so Android 11+ reports whether it is installed, and SHALL NOT request `QUERY_ALL_PACKAGES`.

#### Scenario: Registry gains a package
- **WHEN** a package is added to the registry but not to the manifest's `<queries>`
- **THEN** the unit tests SHALL fail

### Requirement: Registered apps are named as the owner knows them
Each registered package SHALL carry the name of the app it is. `com.gojek.app` SHALL be labelled "Gojek", described as "GoPay di aplikasi Gojek", and SHALL stay in the `gopay` institution, so parsing, the cross-app duplicate rule and the tracker's package mapping are unchanged.

#### Scenario: Gojek app label
- **WHEN** a notification from `com.gojek.app` is captured
- **THEN** its stored and sent `app_label` SHALL be "Gojek" and its institution SHALL be `gopay`
