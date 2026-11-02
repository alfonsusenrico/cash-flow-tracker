# Spec Delta

## REMOVED Requirements

### Requirement: Floating Capsule Navigation
**Reason**: The approved workspace design replaces the floating capsule dock below 1024 px with a phone tab bar at 760 px and below and an icon rail from 761 to 1180 px.
**Migration**: Phone navigation is defined by "Phone Tab Bar" in `executive-sidebar-layout` and the information architecture in `tactile-pulse-ui`. The tab-label contrast rule (4.5:1 in both themes) moves to "Phone Tab Bar". The dock tokens are removed.
