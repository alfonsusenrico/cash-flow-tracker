# Verification: self-host-web-fonts

Date: 2026-10-29. Base is origin/main 8b8642a, and every result comes from the working tree on top of it. No production system was contacted.

| Check | Result |
|---|---|
| Backend suite without Postgres (CI preflight conditions) | 609 passed, 453 skipped, 0 failed. Before the fix: 1 failed (`test_pair_is_rate_limited_per_client_ip`, `PoolClosed`), as in deploy run 40812595445 |
| Fresh `postgres:16`, Flyway V1→V34, full backend suite | 34 migrations applied. 1062 passed, 0 skipped |
| Frontend `npm ci`, `npm test` | 227 passed in 46 files (223 on the base; 4 new in `src/app/fonts.test.ts`) |
| `fonts.test.ts` on the old layout, globals and nginx files | 3 of 4 fail, as expected. The licence test passes because the font files were present |
| Frontend `npm run lint`, `npm run type-check`, `npm run build` | exit 0. Lint reports one existing warning in `src/app/review/page.tsx`, outside this change |
| Build output | `.next/static/media` holds two woff2 files. The built CSS has no Google host |
| `next start` + headless Brave on `/login` | body font `jakarta`, loaded face `jakarta 200 800`. Font requests: 2, both `/_next/static/media/*.woff2`. Third-party requests: 0. The only console error is a 404 from `/api/auth/me`, because no backend ran in this check |
| `openspec validate --all --strict` | exit 0, 61 items |
| `git diff --check` | clean |

## Not verified

- **A build with the network cut off.** Instead, the build output and the source test show that no Google fetch remains.
- **Geist Mono rendering.** `/login` preloads the face but uses no monospace text, so the browser check confirmed only that its file is served from the app's own origin.
- **Production.** After the owner merges, the deploy run and Flyway V34 are checked and recorded in `PROJECT_STATE.md`.
