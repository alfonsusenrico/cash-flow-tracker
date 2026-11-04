## ADDED Requirements

### Requirement: Pre-Redesign Palette Values
The workspace tokens SHALL carry the colour values of the pre-redesign app (commit `6cc6704`, "Coral Terminal") in both themes: window, surface, ink, muted, primary coral, the semantic hues with their text inks and soft fills, the six Kakeibo hues and the sidebar tones. Token names SHALL stay as they are. A token without an old counterpart SHALL take the nearest old value and be listed in the change's verification record.

#### Scenario: Dark window and coral primary
- **WHEN** a page renders in the dark theme
- **THEN** the window SHALL be `#050608`, cards `#0E1118`, body text `#F8FAFC` and the primary fill `#FF4757` with its `#0A0D14` contrast ink

#### Scenario: Light theme values
- **WHEN** a page renders in the light theme
- **THEN** the window SHALL be `#E8ECEF`, cards `#FFFFFF`, body text `#0A0D14`, the primary fill `#FF334B`, need `#2448C4` and saving `#296B38`

#### Scenario: Contrast still enforced
- **WHEN** `tokens.test.ts` runs after the values change
- **THEN** every text pair SHALL measure at least 4.5:1 and every meaningful mark at least 3:1 in both themes, and the test SHALL fail on the first pair below its threshold
