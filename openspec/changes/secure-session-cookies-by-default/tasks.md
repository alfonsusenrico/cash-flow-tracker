# Tasks

## 1. Reproduction

- [x] 1.1 Add focused regressions for settings, production startup, session headers, workflow/Compose configuration and runtime materialization; record expected failures on unmodified main in verification.md.

Expected Result: The insecure default and seed behavior are reproduced with synthetic inputs.

## 2. Correction

- [x] 2.1 Secure settings defaults and reject production false; make the existing test setup explicitly non-production; verify settings/startup/header regressions pass.
- [x] 2.2 Correct workflow and materializer unset/blank/seed handling, secure base Compose and add the explicit local override; verify materializer/configuration regressions and a no-secret production dry run.
- [x] 2.3 Update local examples and cookie-specific deployment documentation; verify local configuration renders development/false and production renders production/true.

Expected Result: Production fails closed; explicit local HTTP remains usable without affecting release discovery.

## 3. Verification and Delivery

- [x] 3.1 Run the full backend suite against disposable migrated PostgreSQL with no database skips; record command/counts and frontend CI checks, Compose validation, native strict validation and diff review in verification.md.
- [x] 3.2 Check only the COOKIE_SECURE secret name with gh secret list, report likely impact and pending owner header confirmation, and refresh local PROJECT_STATE.md with evidence and separate follow-ups.
- [ ] 3.3 Commit with the authorized author and subject-only Conventional message, push fix/secure-cookies-by-default, verify the remote revision and report the compare link.

Expected Result: A reviewed branch is available to the owner; no deployment, production access, secret modification, or merge occurs.
