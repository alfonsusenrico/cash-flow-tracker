# Design

## Context
The mobile worktree starts at main 4bafaa2, version 2.0.0/code 19. The authoritative prototype is option-final.html and the approved phased plan is in the main mobile checkout's private research notes. Backend origin/main b255833 supplies the pairing contract.

## Goals / Non-Goals
Keep the E visual system, every existing completion path, stable device identity and current Room schema. Scope is mobile pairing only. No owner phone or production access.

## Decisions
1. Use CameraX 1.4.2 and bundled ML Kit 17.3.0, released more than two weeks ago. The approved custom scanner precludes Google Code Scanner's own screen. Restrict recognition to QR; bind preview and analysis to lifecycle; close each frame and unbind on leaving. No image persistence.
2. Parse a bounded hierarchical cashflow://pair URI. Require exactly v, server and code once each, v=1, an exact server match, and ten uppercase Crockford characters. Reject paths, fragments, userinfo, ports and extra or duplicate parameters before exchange. Both camera and external links enter the same confirmation screen. Codes and keys are never logged or placed in saved UI state.
3. BuildConfig supplies the server, with a debug-only companionServerUrl Gradle property. Production getters ignore legacy URL preferences. Preserve the existing context when normalized legacy URL equals the build URL. A different legacy server must invalidate the context and credentials to prevent sending another server's data to the fixed server. Unit tests inject transport dependencies, rather than expose a runtime URL field.
4. Detect Settings.Global.DEVICE_NAME, otherwise manufacturer plus model, deduplicating manufacturer prefixes. Normalize whitespace, exclude control characters and bound to backend's 80-character limit. Keep device_id stable.
5. Use an unauthenticated, non-redirecting exchange client. Save the returned key synchronously through pairing context handling before the authenticated identity check. Health alone is public and cannot validate a key. Retry the identity check without consuming the QR twice. Never echo exception messages or response bodies into UI.
6. Owner decision 2026-10-30: backend same-device exchange replaces its old key immediately. No additional delete request is needed for that key. A legacy manual key can be identified by its prefix and revoked only after the new key authenticates and ownership is established; failures leave it available in web settings. Do not revoke a key on another owner's account.
7. Setup steps are listener permission, battery/background, then pairing, with skip actions. Existing configured users enter the app directly. Settings offers connection test and re-pairing. External links open confirmation even on an existing install, and never exchange automatically.

## Risks / Trade-offs
- Exchange response lost after the server replaces a key: show an actionable failure and request a fresh web QR. Never claim the old key remains usable.
- Manual fallback is retained for camera denial/unavailability; real optical scanning still requires a physical QR on a separate screen.
- Room remains v7. Actual TalkBack speech and owner-phone camera behavior need owner acceptance and are reported separately.

## Migration Plan
Upgrade preserves rows and stable device id. Equal-server context is byte-identical. Different-server credentials are cleared with a fresh context. Do not install on TECNO in this phase. Rollback app package preserves Room compatibility, but any server-revoked key must be paired again.

## Sources
- https://developer.android.com/jetpack/androidx/releases/camera (1.4.2, 2025-03-26)
- https://developers.google.com/ml-kit/vision/barcode-scanning/android (bundled 17.3.0)
- https://developer.android.com/media/camera/camerax/analyze (frame lifecycle)
