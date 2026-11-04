## MODIFIED Requirements

### Requirement: Peta Aliran Map and Keyboard
Peta Aliran SHALL draw the flow as a Sankey whose links are distinguished by pattern as well as colour. Its nodes SHALL be focusable buttons with accessible names that give the amounts in and out. The arrow keys SHALL move between nodes within and across columns, and Enter SHALL open the node's drawer, shown as a centered dialog above 760 px and a bottom sheet at 760 px and below. Esc SHALL close the drawer with focus restored. The drawer SHALL list a link's rows, and each row SHALL open the shared transaction drawer. The map SHALL keep its full width while a drawer is open.

#### Scenario: Keyboard node walk
- **WHEN** the owner tabs into the map, presses the Right arrow and then Enter
- **THEN** focus SHALL move to a node in the next column, and its drawer SHALL open with its links

#### Scenario: Link rows in the drawer
- **WHEN** the owner opens the link from "Bank Utama" to Kebutuhan
- **THEN** the drawer SHALL list its rows 50 at a time, with their total equal to the link's amount
