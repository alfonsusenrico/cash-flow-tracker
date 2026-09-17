# Spec: lakukan-segera-rail

## Overview
The Beranda right action rail SHALL include a "Lakukan Segera" priority task section showing
pending scheduled bills due within 3 days and unconfirmed transactions.

## Requirements

- The right rail on Beranda SHALL show a "Lakukan Segera" (Do These First) section.
- The section SHALL display pending scheduled bills with due date, amount, and a 1-click action button.
- If there are no pending tasks, the section SHALL be hidden entirely.
- Pending tasks SHALL be fetched from the existing scheduled/pending endpoint.
- Each task card SHALL show: name, due date, amount, and a primary action button.
- The right rail SHALL also contain: Mini Vault (accounts liquidity) and Mini Goals.

## Scenarios

#### Scenario: Lakukan Segera shows pending bills
- **GIVEN** there are scheduled bills due within 3 days
- **WHEN** the Beranda page loads
- **THEN** the "Lakukan Segera" section SHALL appear in the right rail
- **AND** each pending bill SHALL show name, due date badge, and amount

#### Scenario: Lakukan Segera hidden when empty
- **GIVEN** there are no pending scheduled bills
- **WHEN** the Beranda page loads
- **THEN** the "Lakukan Segera" section SHALL NOT appear
- **AND** the Mini Vault section SHALL fill the top of the right rail
