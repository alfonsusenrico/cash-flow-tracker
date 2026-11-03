# Spec Delta

## MODIFIED Requirements

### Requirement: Default Bahasa Indonesia Localization
The user interface SHALL render all text in Bahasa Indonesia by default, without requiring users to configure language settings, and every page SHALL declare Indonesian as its document language.

#### Scenario: First-time user opens application
- **WHEN** any user visits the web app or login page
- **THEN** all navigational labels, form placeholders, metrics, and buttons display in natural Bahasa Indonesia

#### Scenario: Document language
- **WHEN** a browser or screen reader loads any page, including the login page
- **THEN** the root element SHALL carry `lang="id"`, and proper names, merchant names and quoted notification text SHALL be kept as written
