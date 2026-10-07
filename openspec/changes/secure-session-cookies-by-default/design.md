# Design

## Context

See proposal.md for motivation and authority. Main `7fcd302` supplies an insecure app default, an insecure base Compose default, and a workflow fallback that overrides the materializer's secure default. Empty workflow variables also remain empty in materialization; historical runtime seeds can contain the former `false` flag. SessionMiddleware already consumes the typed settings flag correctly.

## Goals / Non-Goals

**Goals:** Correct configuration at its owning boundaries, fail closed before production serves requests, and preserve an explicit local HTTP path.

**Non-Goals:** Replace session storage/authentication, change unrelated materializer defaults, or execute production operations. No migration is needed.

## Decisions

1. Normalize the cookie flag with whitespace trimming and case folding; only literal `false` disables security. Missing, blank, and other values remain secure. Retain the existing environment default of `production`, recognizing `prod` as a production alias. Reject insecure production settings with a RuntimeError naming configuration, never a value. Rejecting all non-boolean strings instead would exceed the requested fail-safe behavior.
2. Pass `${{ secrets.COOKIE_SECURE }}` without a false fallback. The materializer uses its `true` default for an absent/blank cookie variable and excludes that flag from seed inheritance. Other seed/default behavior stays unchanged. Preserving historical `false` would prevent an unset-secret deployment from recovering; hard-coding true only in the workflow would leave standalone materialization vulnerable.
3. Base Compose defaults to secure. `docker-compose.local.yml` explicitly sets development and insecure cookies only when included with `-f`. Do not name it `docker-compose.override.yml`, because the approved release script uses implicit Compose discovery and could then load a local bypass in production. The local example pairs `APP_ENV=development` with a documented `COOKIE_SECURE=false` opt-in.
4. Test settings independently of the global test environment and assert an actual session Set-Cookie flag on a synthetic ASGI session route. Production import/startup rejection is checked in a subprocess. Give the existing HTTP-oriented test fixtures an explicit test environment; do not weaken the production assertion for tests.

## Risks / Trade-offs

- Explicit production `COOKIE_SECURE=false` causes startup failure → retain the intentional assertion and document removing the override or setting true before owner deployment.
- Local HTTP without opt-in cannot retain session cookies → document the local override and direct-run variables.
- Secret existence does not prove its value or actual deployed headers → name-only audit and owner confirmation after merge/deploy.
- Existing deployment rollback/image and CI database gaps remain → document separate follow-ups; do not alter them in this branch.

## Migration Plan

Push the topic branch for owner review and merge. The owner uses the existing deployment pipeline and confirms the login response's Secure flag. Existing cookie names and session data remain compatible. No agent deployment or secret change is authorized. Reverting source also restores unsafe defaults; prefer a forward correction and keep secure runtime configuration if recovery is needed.

## Open Questions

None for implementation. Production header confirmation belongs to the owner after deployment.
