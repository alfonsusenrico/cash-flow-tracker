## Context

`NotificationProcessor.isOneTimeCode` matches the regex `\b(?:OTP|one[- ]time password|verification code|security code|kode (?:verifikasi|otp|akses|keamanan|rahasia)|PIN)\b` anywhere in the title, text or big text, and returns before the row is inserted. Dropped rows leave no local trace. That is intended for real codes, because codes must never leave the phone, but it is invisible when the match is wrong.

## Goals / Non-Goals

**Goals:** drop only notifications that carry a code; keep transactions that carry a safety footer.

**Non-Goals:** logging dropped notifications or keeping a local trace of them. That belongs to the phase 4 capture redesign, which decides what is kept locally.

## Decisions

### D1. Title rule plus a code-proximity rule

- **Title rule.** If the title contains a one-time-code phrase (without bare `PIN`), the notification is a code. Transaction titles never use these phrases.
- **Proximity rule.** In title, text or big text, a phrase from the set *including* `PIN` that sits within 30 characters of a standalone 4–8 digit token, in either order, marks a code. The token must not be part of a Rupiah amount: no `Rp`/`IDR` prefix, and not adjacent to `.`/`,` digit groups.

*Rejected:*
- Removing `PIN` from the keyword list only. A footer can also say "OTP", so the word alone is never enough.
- Requiring a code anywhere in the text. Reference numbers in transaction notifications would then trigger it.

### D2. Version 1.6.1 (code 13)

The fix is shipped as a patch build. Its testable behaviour is fully covered by JVM unit tests. Per the project memory, the debug APK is built with `--no-build-cache`, and the version constant is checked with dexdump before any install.

## Risks / Trade-offs

- [A code formatted with separators, such as "123 456", slips through and is stored] → Banks in the sample send contiguous codes. The backend still ignores one-time-code text, so it never reaches the ledger. A unit test documents the behaviour.
- [A 4–8 digit reference number sits next to "OTP" in a transaction footer] → This requires both a footer phrase and a bare reference number within 30 characters. Such a case is unlikely and is covered by a negative test with a typical footer layout.

## Migration Plan

There is no data migration. Roll back by reinstalling 1.6.0, which is kept from `adb shell pm path` before any install.
