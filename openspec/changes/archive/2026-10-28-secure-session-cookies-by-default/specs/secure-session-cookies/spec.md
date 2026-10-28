# Spec Delta

## Purpose

Protect browser sessions with secure cookie defaults while permitting an explicit local HTTP development configuration.

## ADDED Requirements

### Requirement: Session cookies are secure by default
The service SHALL issue Secure session cookies when COOKIE_SECURE is absent, blank, or any value other than an explicit case-insensitive, whitespace-trimmed false.

#### Scenario: No cookie override
- **WHEN** the service starts without COOKIE_SECURE
- **THEN** a session response includes the Secure cookie attribute

#### Scenario: Blank or non-false override
- **WHEN** COOKIE_SECURE is blank or does not explicitly equal false
- **THEN** session cookies remain secure

### Requirement: Production refuses insecure sessions
The service SHALL fail startup before serving requests if COOKIE_SECURE is explicitly false and APP_ENV identifies production, including prod and the missing APP_ENV default.

#### Scenario: Insecure production settings
- **WHEN** production starts with COOKIE_SECURE=false
- **THEN** startup fails with a configuration error naming COOKIE_SECURE and without disclosing secrets

#### Scenario: Explicit local HTTP opt-in
- **WHEN** a non-production local environment explicitly sets COOKIE_SECURE=false
- **THEN** session cookies omit Secure so local HTTP sessions can operate

### Requirement: Release configuration preserves secure defaults
The production configuration SHALL default COOKIE_SECURE to true when the repository secret is unset or empty, including when a historical runtime seed contains false. Base Compose SHALL default to secure; an explicitly selected local override SHALL pair insecure cookies with a non-production environment.

#### Scenario: Unset repository secret
- **WHEN** production runtime configuration is materialized without COOKIE_SECURE
- **THEN** the resulting cookie flag is true

#### Scenario: Empty secret and historical insecure seed
- **WHEN** materialization receives an empty COOKIE_SECURE and an old seed with COOKIE_SECURE=false
- **THEN** the cookie flag is true and other seed values retain existing precedence

#### Scenario: Local override is explicit
- **WHEN** base Compose is used without the local override or environment opt-in
- **THEN** cookie security defaults to true
- **AND** explicitly including the local override selects development with COOKIE_SECURE=false
