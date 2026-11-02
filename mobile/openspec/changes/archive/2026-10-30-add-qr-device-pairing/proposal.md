# Proposal

## Why
The approved companion setup should pair through a web-generated QR instead of requiring a server URL and a copied key.

## Expected Outcome
Version 2.1.0 guides notification and battery access, scans the web QR after camera permission, confirms the detected device, and tests its new connection. Manual keys remain available.

## Approval
Owner approved the three-phase redesign on 2026-10-29 and explicitly resumed phase 2 on 2026-10-30. On 2026-10-30 the owner approved using the existing backend behavior: same-device keys are replaced during exchange, rather than after Tes Koneksi. No additional backend change is authorized or required.

## What Changes
- Add QR-first setup, a bundled scanner, explicit exchange confirmation and manual fallback.
- Detect the read-only device name and use a fixed build server.
- Preserve pairing context for existing installations whose saved server matches the fixed server.
- Save an exchanged key immediately and retry authentication without exchanging a used code again.
- Release 2.1.0, code 20, without a Room change.

## Non-goals
Capture, notification parsing, backend behavior, production access and installation on the owner phone.

## Capabilities
### New Capabilities
- `companion-pairing`: Safe setup, QR exchange, automatic device naming and key replacement.
### Modified Capabilities
- `companion-transport-security`: Fixed server and debug build property instead of a runtime URL field.

## Impact
Android setup, preferences, API client, manifest and tests. CameraX 1.4.2 and bundled ML Kit barcode-scanning 17.3.0, with camera permission requested at scanner entry. Existing API, personal-data and UI capabilities continue. Camera access adds permission-denial and lifecycle verification requirements. No database migration or backend change.
