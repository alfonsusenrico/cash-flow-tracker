## ADDED Requirements

### Requirement: Fonts are served from the app's own origin

The web app SHALL bundle every font file it uses and serve it from its own origin.

- A production build SHALL NOT fetch fonts from a third-party provider.
- A page SHALL NOT request fonts or font stylesheets from a third-party host.
- Each bundled font SHALL ship with its licence text.

#### Scenario: Build without access to a font provider

- **WHEN** the frontend is built on a host that cannot reach Google Fonts
- **THEN** the build completes and the output contains the Plus Jakarta Sans and Geist Mono files

#### Scenario: Page renders with bundled fonts

- **WHEN** a browser loads any page of the web app
- **THEN** body text renders in Plus Jakarta Sans and numeric code text in Geist Mono
- **AND** every font request goes to the app's own origin, with none to a third-party host

#### Scenario: Third-party font is blocked by policy

- **WHEN** a stylesheet or script tries to load a font or font stylesheet from another origin
- **THEN** the Content-Security-Policy (`font-src 'self'`, `style-src` without external hosts) blocks the request
