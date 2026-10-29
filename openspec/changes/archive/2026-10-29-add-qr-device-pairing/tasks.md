## 1. Backend

- [x] 1.1 V34: `api_keys` device columns and `pairing_codes`. Evidence: fresh V1→V34 migration; existing keys become `manual`.
- [x] 1.2 Code creation, exchange, list and revoke, the revoked-key auth check, and manual-only reset (D1–D4). Evidence: tests for every spec scenario, including expired, reused and burned codes, the rate limit, owner isolation, a 401 after revoke, and a paired key that survives a manual rotate.

Expected Result: per-device keys with safe single-use pairing.

## 2. Web Settings

- [x] 2.1 The "Perangkat terhubung" card, the QR dialog (`qrcode` package), the device list and revoke (D5). Evidence: vitest + axe tests for the dialog (countdown, expiry, regenerate, focus, Escape) and the list; a browser check at 390/1280 in light and dark.

Expected Result: the owner can pair and revoke from the web.

## 3. Verify and deliver

- [x] 3.1 Full backend and frontend suites, lint, type-check, build, `openspec validate --all --strict`, and verification.md.
- [x] 3.2 Archive, push `feat/qr-device-pairing`, and report the compare link. Merge after the capture branch.
