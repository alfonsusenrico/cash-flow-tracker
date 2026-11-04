## MODIFIED Requirements

### Requirement: Fluid Full-Width Workbench Layout
Above 760 px the signed-in pages SHALL render inside one floating workspace window on the canvas: rounded corners, the window shadow token, up to 1680 px wide and centred, holding the sidebar and the main column. Details SHALL open as centered dialogs over the window, never as a right-hand column, so the main column keeps its width while a detail is open. The main column SHALL adapt its grids to its own width. At 760 px and below the window SHALL fill the screen without corners or shadow.

#### Scenario: Viewing dashboard on wide monitor
- **WHEN** the dashboard is viewed at 1440 px or wider
- **THEN** the window sits on the canvas with a visible margin, the cards and tables fill the main column, the window is at most 1680 px wide, and no horizontal scrolling occurs at 1440, 1280, 1024 or 768 px

#### Scenario: Opening a detail keeps the main column
- **WHEN** the owner opens a transaction detail on Transaksi at 1440 px
- **THEN** the main column SHALL keep its width and layout behind a dimmed scrim, and the detail SHALL be a centered dialog
