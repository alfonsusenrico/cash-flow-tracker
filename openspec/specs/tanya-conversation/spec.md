# tanya-conversation Specification

## Purpose
Keep the retired Tanya page out of the web app: old links land on Beranda, and typed recording stays in Catat until a page that can actually answer questions returns.

## Requirements

### Requirement: Tanya Page
The web app SHALL NOT offer a Tanya page. A request for `/tanya` SHALL redirect permanently to Beranda, and no navigation, shortcut or help entry SHALL name Tanya. Recording a typed transaction SHALL stay available through Catat (`text-capture`).

#### Scenario: Opening Tanya
- **WHEN** a signed-in owner opens a bookmarked `/tanya`
- **THEN** the response SHALL be a permanent redirect to `/`, and Beranda SHALL open without an error

#### Scenario: Safe-to-spend unavailable
- **WHEN** the owner looks for the safe amount that Tanya's lead line used to show
- **THEN** it SHALL be on Beranda as "Aman dipakai sampai gajian", and no page SHALL show Rp 0 in its place
