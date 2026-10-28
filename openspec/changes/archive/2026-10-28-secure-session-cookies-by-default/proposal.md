# Proposal

## Why

The production workflow and app configuration default session cookies to insecure. The owner approved this correction in the 2026-10-07 handoff after a framework review identified the conflicting defaults.

## Expected Outcome

HTTPS sessions use Secure cookies without requiring a GitHub secret. Production cannot start with insecure cookies. Local HTTP remains available through an explicit development configuration.

## What Changes

- Default app and base Compose cookies to secure; only explicit `COOKIE_SECURE=false` disables them outside production.
- Refuse insecure cookies in production, including the default production environment.
- Pass an unset repository secret as empty and let the production materializer supply `true`; do not inherit the cookie flag from historical runtime seeds.
- Document local HTTP opt-in and provide an explicitly selected local Compose override.
- Add before/after regression evidence, a production materializer dry run, and full backend verification.

## Non-goals

No deployment, production access, secret/settings changes, merging, database/schema changes, or changes to the owner's existing checkout. PR CI coverage, workflow secret interpolation/SSH host verification, immutable image rollback, and ingress/health/runtime-file handling remain separate follow-ups.

## Capabilities

### New Capabilities

- `secure-session-cookies`: Secure defaults, production startup enforcement, and explicit local HTTP opt-in. This specifies existing authentication/deployment behavior; it adds no runtime capability or dependency.

### Modified Capabilities

None. The existing spec inventory has no session-cookie requirement.

## Impact

App settings and SessionMiddleware configuration, test environment setup, production workflow/materializer, base/local Compose configuration, examples, and release notes. Cookie names, sessions, API contracts, and persisted data remain unchanged. Existing production configurations explicitly requesting insecure cookies will fail startup.

## Approval

The owner explicitly approved implementation, isolated branch creation from main, commit, and push in the handoff. Production login-header confirmation remains owner work after merge and deployment.
