# Spec Delta

## MODIFIED Requirements

### Requirement: Global Keyboard Shortcuts
The workspace SHALL support ⌘K/Ctrl+K to focus search, N to open capture, ? to open the shortcut help and Esc to close the top-most popover, sheet, drawer or dialog. Single-key shortcuts SHALL be ignored while typing in a field and SHALL be switchable off in the help dialog; the choice SHALL persist in the browser. The help SHALL also list the keys of the open page, and the switch SHALL cover them.

#### Scenario: Typing in a field
- **WHEN** the owner types "n" into the search field
- **THEN** capture SHALL NOT open

#### Scenario: Turning single-key shortcuts off
- **WHEN** the owner turns off "Pintasan satu tombol" and later presses N or ? outside a field
- **THEN** nothing SHALL happen, while ⌘K/Ctrl+K and Esc keep working, and the setting SHALL survive a reload

#### Scenario: Page keys in the help
- **GIVEN** a page that registers its own keys, such as Tinjau
- **WHEN** the owner opens the help on that page and on Beranda
- **THEN** that page's keys SHALL be listed under the page's name only while it is open, and the switch caption SHALL say that it also turns them off
