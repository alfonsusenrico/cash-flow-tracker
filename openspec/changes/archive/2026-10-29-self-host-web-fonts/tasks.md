## 1. Fonts

- [x] 1.1 Bundle the Latin variable woff2 files and OFL licences under `frontend/src/app/fonts/`. Load them with `next/font/local` in `layout.tsx`, keeping `--font-jakarta` and `--font-geist-mono`. Evidence: `npm run build` exits 0, and `.next/static/media` holds two woff2 files.
- [x] 1.2 Remove the runtime Google Fonts `@import` from `globals.css`, and narrow the nginx CSP to same-origin fonts and styles. Evidence: `src/app/fonts.test.ts` fails 3 of 4 tests on the old files and passes 4 of 4 after the change.
- [x] 1.3 Check the production build in headless Brave. Evidence: `/login` renders in `jakarta`, both font requests go to `/_next/static/media/`, and there are 0 third-party requests.

Expected Result: the build needs no font provider, and pages load fonts only from the app's own origin.

## 2. CI preflight

- [x] 2.1 `test_pair_is_rate_limited_per_client_ip` takes the `client` fixture. Evidence: the backend suite without Postgres reports 609 passed, 453 skipped, 0 failed.

Expected Result: the preflight's backend step passes without a database.

## 3. Verify and deliver

- [x] 3.1 Full checks, recorded in `verification.md`: the backend suite on a fresh Flyway V1→V34 database, frontend test, lint, type-check and build, `openspec validate --all --strict`, and `git diff --check`.
- [ ] 3.2 Archive the change, push the branch, and report the compare link. The owner opens and merges the PR. After the deploy, confirm the run succeeded and production reports Flyway V34.

Expected Result: a pushed branch whose deploy is not exposed to the Google Fonts fetch.
