# Spec Delta

## ADDED Requirements

### Requirement: Capture Parse Preview
`POST /api/capture/parse` SHALL return the deterministic reading of a typed capture: amount, problems, kind, description, time with its phrase and options, an account hint resolved to one of the owner's accounts, and a category hint from the owner's merchant memory only. It SHALL write nothing, SHALL call no model, SHALL be owner-scoped, and SHALL apply the capture text bounds.

#### Scenario: Preview of a short expense
- **GIVEN** device time 2026-11-02 10:00 in Asia/Jakarta and an owner whose memory maps "parkir" to Transportasi (need)
- **WHEN** the owner previews "parkir 5rb jam 2"
- **THEN** the preview SHALL have amount 5.000, description "Parkir", time 14:00 with phrase "jam 2", and category hint Transportasi with pillar need

#### Scenario: Missing amount
- **WHEN** the owner previews "bayar parkir"
- **THEN** the amount SHALL be `null` and `problems` SHALL contain `missing_amount`

#### Scenario: Preview writes nothing and asks no model
- **WHEN** the owner previews any text
- **THEN** no capture, transaction, merchant memory, feedback or notification row SHALL be created or changed, and the model provider SHALL NOT be called

#### Scenario: Another owner's memory and accounts
- **GIVEN** only another owner has the account "Dompet Digital" and a memory for "parkir"
- **WHEN** this owner previews "parkir 5rb pakai dompet digital"
- **THEN** the account hint and the category hint SHALL be `null`

#### Scenario: Bounds and rate limit
- **WHEN** the text is empty or longer than 200 characters, or the owner sends more than 120 previews in 60 seconds
- **THEN** the response SHALL be 422, or 429 `capture_rate_limited`, and the `/capture/text` budget SHALL be unaffected

#### Scenario: Preview logs
- **WHEN** a preview is answered
- **THEN** the logs SHALL hold the user id, the outcome and problem codes only, never the text or the amount

### Requirement: Web Catat Dialog
The web "+ Catat" action and the N key SHALL open a text-first dialog that previews the typed text through the parse endpoint, refuses to record text without exactly one amount, records through `POST /api/capture/text` declaring the question types it answers, asks follow-up questions inline, and offers "Urungkan" within the capture's undo window. "Isi manual" SHALL open the existing manual form.

#### Scenario: Typing shows the reading
- **WHEN** the owner types "parkir 5rb jam 2"
- **THEN** within a second the dialog SHALL show chips for Rp 5.000, 14.00 and the account or "rekening: nanti aku tanya", without recording anything

#### Scenario: Text without an amount is refused
- **WHEN** the owner types "bayar parkir"
- **THEN** "Catat" SHALL be disabled and the dialog SHALL say "Jumlahnya belum ada. Aku nggak nebak, tulis jumlahnya ya (misal 5rb)."

#### Scenario: Recording with a follow-up question
- **GIVEN** the owner has no memory for "pulsa" and no account was named
- **WHEN** the owner records "beli pulsa 50rb"
- **THEN** the dialog SHALL ask "Dari rekening mana?" with account choices, and answering SHALL record the expense and close the dialog with a toast offering "Urungkan"

#### Scenario: Undo right after recording
- **WHEN** the owner presses "Urungkan" in the toast within 10 seconds
- **THEN** the capture SHALL be undone through `/capture/{id}/undo`, the row SHALL be gone, and balances SHALL be as before

#### Scenario: Retry after a network failure
- **WHEN** recording fails with a network error and the owner presses "Catat" again
- **THEN** the dialog SHALL resend the same `client_capture_id`, and at most one transaction SHALL exist

#### Scenario: Manual form fallback
- **WHEN** the owner chooses "Isi manual" after typing "parkir 5rb"
- **THEN** the manual form SHALL open with the amount 5.000 filled in

#### Scenario: Preview unavailable
- **WHEN** the parse endpoint fails or answers 429
- **THEN** the dialog SHALL say "Belum bisa baca sekarang" and "Catat" SHALL stay available
