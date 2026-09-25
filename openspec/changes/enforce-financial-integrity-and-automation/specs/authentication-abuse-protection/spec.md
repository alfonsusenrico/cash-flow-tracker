# Spec Delta

## Purpose

Defines bounded authentication attempt behavior that reduces credential guessing and registration abuse while preserving legitimate access.

## ADDED Requirements

### Requirement: Authentication Attempt Rate Limits
The system SHALL apply configured rate limits to login attempts by client and normalized username and to registration attempts by client. Exceeded limits SHALL return HTTP 429 with retry guidance and SHALL NOT reveal whether a username exists.

#### Scenario: Throttling repeated failed login attempts
- **WHEN** a client exceeds the configured failed-login limit for one username within the active window
- **THEN** subsequent attempts receive HTTP 429 until the window permits another attempt
- **AND** the response does not disclose account existence

#### Scenario: Successful login clears user-scoped failures
- **WHEN** a legitimate user authenticates successfully before the client-wide limit is reached
- **THEN** the user-scoped failed-attempt state is cleared
