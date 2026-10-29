## Approval

The owner approved the font fix on 2026-10-29 ("yes do the font fix"). The same branch also carries a test-only fix for the failed pairing deploy (run 40812595445). That fix unblocks the merged pairing change and alters no behaviour.

## Why

Production deploys fail intermittently while `next build` fetches Plus Jakarta Sans and Geist Mono from Google through `next/font/google`. The error is `TypeError: Cannot read properties of null (reading '1')`. A failed preflight blocks a release that is otherwise correct.

Separately, `globals.css` still imports six families from Google at runtime. Every visitor's browser contacts Google for fonts that are already bundled or never used.

## Expected Outcome

- A release build no longer needs network access to Google Fonts, so the preflight can no longer fail on that fetch.
- Pages render in the same two families. The browser loads them from the app's own origin and makes no request to a third-party font host.
- The pairing deploy's preflight passes. Without a database, the backend suite skips the database tests instead of failing on them.

## What Changes

- Bundle the Latin variable files of Plus Jakarta Sans (weights 200–800) and Geist Mono (100–900), each with its OFL-1.1 licence. Load them with `next/font/local`, keeping the `--font-jakarta` and `--font-geist-mono` variables.
- Remove the runtime Google Fonts `@import` from `globals.css`.
- Narrow the nginx Content-Security-Policy to `style-src 'self' 'unsafe-inline'; font-src 'self'`, dropping both Google hosts.
- `test_pair_is_rate_limited_per_client_ip` takes the `client` fixture, so it skips when Postgres is unreachable, as CI's preflight requires.

## Non-goals

- No change to typography, weights, sizes or colours, and no new font families.
- No change to how CI provisions services. The preflight still runs without Postgres.
- Local verification only. The release goes through the GitHub deploy workflow after the owner merges. Production checks after the deploy are recorded in `PROJECT_STATE.md`.

## Capabilities

### New Capabilities

- `web-font-hosting`: the web app serves its fonts from its own origin, and builds do not depend on a font provider.

### Modified Capabilities

None. Project capabilities (`deployed`, `personal-data`) do not change. Removing a third-party request reduces personal-data exposure.

## Impact

- `frontend/src/app/layout.tsx`, `frontend/src/app/globals.css`, and the new `frontend/src/app/fonts/` (two woff2 files of 27 KB and 23 KB, plus two licence texts).
- `nginx/default.conf` (CSP header).
- `backend/tests/test_device_pairing.py`.
- No dependency, API, schema or data change.
