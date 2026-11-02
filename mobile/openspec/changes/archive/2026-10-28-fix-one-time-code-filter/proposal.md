## Approval

Approved by the owner on 2026-10-28.

## Why

The listener drops every notification that contains a one-time-code keyword before storing it. The keyword list includes a bare `PIN`. Banks commonly end transaction notifications with a safety footer such as "Jangan berikan PIN/OTP kepada siapa pun", so a real transaction carrying that footer is discarded silently: it is never stored, never sent, and leaves no trace. This is phase 1 of the cash-flow-tracker plan `research/notes/2026-10-28-automation-quality-plan.md`.

## Expected Outcome

- A notification is dropped as a one-time code only when it really carries a code:
  - the title is a one-time-code phrase; or
  - the text shows a 4–8 digit code next to such a phrase.
- A transaction notification with a PIN/OTP safety footer is captured and sent like any other.

## What Changes

- Replace the one-time-code keyword match with a title rule plus a code-proximity rule in `NotificationProcessor`.
- Remove bare `PIN` as a sufficient signal.
- Add unit tests for footers, codes before and after the phrase, and amounts that must not count as codes.

## Non-goals

- Any other capture-policy change: on-device noise classification, keying by notification, and toggles. These are cash-flow-tracker phase 4.
- Backend changes. The backend's own one-time-code rule is unaffected.
- Release authority. Verification is unit tests plus a debug build checked with dexdump. Installing on the owner's phone happens only on the owner's instruction, after the 1.6.0 pull request is merged.

## Capabilities

### New Capabilities

- `notification-capture`: which notifications the listener stores and forwards, starting with the one-time-code rule.

### Modified Capabilities

None. The repository has no main specs yet.

## Impact

- **Code:** `app/src/main/java/.../service/NotificationProcessor.kt`.
- **Tests:** `app/src/test/java/.../NotificationProcessorTest.kt`.
- **Release:** version 1.6.1 (code 13). There is no backend dependency, so it can ship independently.
