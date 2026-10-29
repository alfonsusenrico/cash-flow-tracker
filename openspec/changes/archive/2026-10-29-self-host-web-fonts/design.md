## Context

`next/font/google` downloads font files during `next build`. In the release preflight this fetch sometimes fails with `TypeError: Cannot read properties of null (reading '1')`, which aborts the deploy. The capture deploy hit this on 2026-10-29 and passed on a rerun.

`globals.css` also carries an old runtime `@import` of Geist, Geist Mono, Inter, JetBrains Mono, Outfit and Plus Jakarta Sans. The design-preset switcher that used Inter and Outfit was removed earlier. Today the tokens reference only `--font-jakarta` and `--font-geist-mono`, and the other names appear only as fallbacks behind them.

## Decisions

### D1. Bundle the woff2 files and load them with `next/font/local`

`next/font/local` keeps everything `next/font` already gives the app:

- the CSS variables;
- preloading;
- `size-adjust` fallback metrics, so layout does not shift;
- hashed, immutable file names.

The files come from the Fontsource packages `@fontsource-variable/plus-jakarta-sans@5.3.0` and `@fontsource-variable/geist-mono@5.3.0`. Each is the Latin variable `wght` file, the same subset the Google config requested. Their licence texts are copied beside them as `*-OFL.txt`.

Rejected alternatives:

- **Add the Fontsource packages as dependencies and import their CSS.** This adds dependencies, ships every subset, and loses `next/font`'s fallback metrics.
- **Keep Google and retry the build.** The release would stay flaky, and visitors' IP addresses would still go to Google.

### D2. Variable weight ranges instead of a static weight list

The Google config requested weights 300–800 for Jakarta and 400–700 for Geist Mono. The variable files cover 200–800 and 100–900, so every weight used today renders from one file per family.

### D3. Drop the runtime Google import and narrow the CSP

Nothing references the imported families except as fallbacks behind the bundled ones. Removing the import ends the runtime third-party request. Narrowing the CSP to same-origin then prevents that request from returning by accident.

Rejected alternative: keep the CSP allowances "just in case". That would allow a request the spec forbids.

### D4. Make the rate-limit pairing test depend on the `client` fixture

CI's preflight runs the backend suite without Postgres. Database tests are expected to skip through the `client` fixture. `test_pair_is_rate_limited_per_client_ip` still reached the database because it built its own TestClient without that fixture, so it failed with `PoolClosed`. Requesting `client` restores the suite's contract and changes no assertion.

## Risks / Trade-offs

- **Font updates are now manual.** This is acceptable: both families change rarely, and the version is recorded in this change.
- **Characters outside the Latin subset fall back to system fonts.** This matches the previous `subsets: ["latin"]` behaviour.

## Migration Plan

Deploy normally. Rollback is reverting the commit. No data or schema is involved.
