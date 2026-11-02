# Proposal

## Approval
On 2026-11-02 the owner approved this fix ("yes do the fix"): Pengaturan → "Aplikasi yang dipantau" lists every package variant in `FinancialSourceRegistry.SELECTABLE_APPS`, including apps that are not installed, so the owner sees myBCA twice and GoPay three times and reads them as duplicates. `com.gojek.app` is also labelled "GoPay" although it is the Gojek app. The same day the owner approved the Phase 0 plan ("ok I approve all you recommend"); this job belongs to it.

## Why
A list of every registered package variant, installed or not, looks like a list of duplicates and makes the owner doubt what is monitored. Calling the Gojek app "GoPay" adds a third "GoPay" row for an app the owner knows by another name.

## What Changes
- Pengaturan's "Aplikasi yang dipantau" lists only the registered finance apps installed on this phone, read again whenever the screen opens or the app returns to the foreground. With none installed it says so in one short casual line.
- Setup's notification-access step names only the installed finance apps, with the same empty line, read again whenever setup returns from a settings page.
- The manifest declares the registered packages in a `<queries>` block for Android 11+ package visibility. The app does not ask for `QUERY_ALL_PACKAGES`.
- Capture matching does not change: `isTargetFinancialApp` and the notification gate never consult what is installed, so an app installed later is captured by its saved toggle or its default.
- `com.gojek.app` is labelled "Gojek" with the description "GoPay di aplikasi Gojek". Its institution stays `gopay`, so parsing, the cross-app duplicate rule and the server's package mapping are unchanged.
- The app version becomes 2.4.1. The source `versionCode` follows the release scheme `X * 10000 + Y * 100 + Z` (20401).

## Capabilities

### New Capabilities
None.

### Modified Capabilities
- `notification-capture`: the toggle list shows installed registered apps only, capture stays independent of what is installed, and `com.gojek.app` is named "Gojek".
- `companion-pairing`: the notification-access step names only the installed finance apps.

## Impact
- `mobile/app/src/main/AndroidManifest.xml`: a `<queries>` block of the 11 registered package spellings. No new permission.
- `registry/FinancialSourceRegistry.kt`: the Gojek label, `REGISTERED_PACKAGES` and `installedSelectableApps`. New `registry/InstalledPackages.kt` wraps `PackageManager`.
- `ui/settings/SettingsScreen.kt`, `ui/pairing/PairingScreen.kt`, `PairingActivity.kt`: installed-only lists and the empty line.
- `app/build.gradle.kts`: version 2.4.1, code 20401.
- Server: `app_label` is sent with each notification and stored in `notification_events.app_label`. New captures from `com.gojek.app` carry "Gojek" instead of "GoPay"; stored rows keep their old label. The backend maps institutions by `package_name`, and the payload hash and parsing do not use the label, so no server change is needed.
- No Room migration, API or release workflow change.
