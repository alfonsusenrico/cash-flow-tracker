## Context

`POST /accounts/{id}/reconcile` clears `reconciliation_required`. `POST /accounts/{id}/balance-matches` (`record_match`) only stores a matched point after checking that the typed balance equals the recorded one. Production 2026-11-16 shows two matches at 82.515 for Tabungan, with the flag cleared only by the later reconcile.

## Decisions

1. **Clear on a successful match.** It is the same database transaction as the matched point. A refused match (`balance_mismatch`) changes nothing.
2. **Only the checked account.** A parent's matched total does not prove each pocket's own balance, so its pockets keep their flags.

Rejected: clearing the flag in the web app by calling `/reconcile` after a match. That makes two requests where one confirmation already exists, and leaves the API inconsistent for other clients.
