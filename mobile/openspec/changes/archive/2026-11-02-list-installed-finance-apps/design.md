# Design

## Context
`FinancialSourceRegistry` holds 11 package spellings (Bank Jago twice by case) in 10 selectable entries. Pengaturan maps `SELECTABLE_APPS` straight to toggles, and setup's notification-access step shows `SELECTABLE_APPS` labels as badges. The listener gate (`NotificationGate.route`) checks `isTargetFinancialApp` and `AppPreferences.isPackageEnabled`, whose defaults are BCA mobile and Shopee off, the rest on. The app targets SDK 35, so from Android 11 `PackageManager` sees only packages the manifest declares. See proposal.md for the motivation.

## Goals / Non-Goals
**Goals:** show what is on this phone; keep capture byte-for-byte the same; make the installed lookup injectable so the JVM tests cover it.

**Non-Goals:** discovering finance apps that are not registered; changing the Riwayat per-app filters; any server change; renaming stored `app_label` values on existing rows.

## Decisions
- **Visibility through `<queries>`, not `QUERY_ALL_PACKAGES`.** The 11 registered spellings are listed as `<package>` entries. `QUERY_ALL_PACKAGES` is a restricted Play permission and would show the app far more than it needs. A unit test parses the manifest and fails when its packages differ from `REGISTERED_PACKAGES`, so a new registry entry cannot silently stay hidden.
- **Filter in the registry, lookup at the edge.** `FinancialSourceRegistry.installedSelectableApps(isInstalled)` is a pure function over the registry, and `InstalledPackages` wraps `PackageManager.getPackageInfo` (the `PackageInfoFlags` overload on API 33+). Tests pass a lambda or use Robolectric's `ShadowPackageManager`. An entry with case variants counts as installed when any spelling is, so either Bank Jago build shows one row.
- **Capture never asks what is installed.** Only the two lists use `InstalledPackages`. The gate and `isTargetFinancialApp` are untouched, so a newly installed app is captured by its stored toggle or default from its first notification, before Pengaturan is ever opened. A regression test routes every registered package through the gate with nothing installed.
- **When to read.** Pengaturan reads on entry and again on every `ON_RESUME` (`LifecycleEventEffect`), so an app installed while the sheet is open appears on return. Setup reads the list in `PairingActivity.readPermissions()`, which already runs on every `onResume`, by adding `financeApps` to `SetupPermissions`. Eleven `getPackageInfo` calls are cheap enough for the main thread.
- **One empty line for both screens.** `NO_FINANCE_APPS` is shared so the two screens never drift. In Pengaturan it replaces the "Cuma dari aplikasi ini…" note, which would read oddly over an empty list.
- **Gojek label only.** `com.gojek.app` becomes "Gojek" and keeps `institution = "gopay"`. The label is stored per row (`RawNotificationEntity.appLabel`) and sent as `app_label`; the backend stores and lists it but derives the institution from `package_name`, and neither the payload hash nor parsing reads it. The Riwayat "GoPay" filter selects by package and still includes `com.gojek.app`, which matches the money it carries.
- **Version 2.4.1, code from the scheme.** `sourceVersionCode` is now computed with `releaseVersionCode(sourceVersionName)` (20401) instead of the hand-kept 27, so debug builds sort with the published `android-v*` codes and the two numbers cannot disagree.

## Risks / Trade-offs
- [A finance app ships under a new package name] → It is neither captured nor listed until the registry and manifest both gain it; the manifest test forces the second step.
- [Some OEM hides a package from `getPackageInfo` despite `<queries>`] → The app is only missing from the list; capture still works because the gate never asks.
- [Existing rows from `com.gojek.app` keep "GoPay"] → Accepted. Only new captures carry "Gojek"; old rows are history and the institution is unchanged.

## Migration Plan
No data migration. Ships with the next companion release (`android-v2.4.1`), which the owner tags. Rolling back is installing the previous APK; preferences are untouched.

## Open decisions for owner
- Riwayat keeps one "GoPay" per-app filter that includes the Gojek app. A separate "Gojek" chip was left out as the conservative choice; say if you want it.
