# mobile-mapping-confirmation Specification

## Purpose
Let the owner confirm or correct an AI-proposed account mapping from an Android notification or the app, without opening the web dashboard.

## ADDED Requirements

### Requirement: Actionable Confirmation Notification
When an event result is `needs_confirmation`, the companion app SHALL post one notification per event naming the amount, the bank's account name, the proposed account, and the confidence, with a confirm action for the proposed account and an action that opens a picker. Confirming from the notification SHALL call the backend without opening the app and SHALL replace the notification with the outcome.

#### Scenario: Confirming from the notification shade
- **WHEN** the owner taps "Ya, GoPay" on the confirmation notification
- **THEN** the app confirms the mapping in the background, the event is later shown as recorded, and the usual recorded alert follows

#### Scenario: Confirmation fails offline
- **WHEN** the confirm call cannot reach the server
- **THEN** the notification says the confirmation failed and offers to open the app, and the event stays awaiting confirmation

### Requirement: In-App Mapping Picker
The detail sheet SHALL show the proposal for events awaiting confirmation and SHALL let the owner choose the proposed account, an alternative, or any liquid account.

#### Scenario: Choosing a different account in the app
- **WHEN** the owner opens "Pilih lain" and selects "Dana Darurat"
- **THEN** the app confirms that account and closes the confirmation notification
