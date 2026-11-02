## MODIFIED Requirements

### Requirement: The auto-sync switch governs every automatic send
While "Auto-Sync Notifikasi" is off, the companion SHALL NOT send captures on its own: no periodic sync, no send after a capture, no send on listener reconnect, no automatic resend of a queued typed capture. Turning it off SHALL cancel that work; turning it on SHALL schedule the periodic sync and send what is waiting, typed captures included. App start SHALL align work with the switch. Owner actions, including the first send of a typed capture, SHALL still send while it is off.

#### Scenario: Switch turned off
- **WHEN** the owner turns auto-sync off while the periodic sync is scheduled, a send is queued and a typed capture waits for its resend
- **THEN** all three are cancelled and no capture or typed capture is sent until the owner asks

#### Scenario: Listener reconnects while off
- **WHEN** the listener reconnects, a new capture is stored, or a typed capture's first send fails offline while auto-sync is off
- **THEN** no send or resend starts and the rows and the typed capture stay pending

#### Scenario: Switch turned on
- **WHEN** the owner turns auto-sync on
- **THEN** the periodic sync is scheduled, a send of pending rows is queued, and queued typed captures are resent once the network allows

#### Scenario: Upgrade with leftover periodic work
- **WHEN** the app starts with auto-sync off and a periodic sync left scheduled by an older build
- **THEN** that periodic sync is cancelled

#### Scenario: Owner syncs by hand while off
- **WHEN** the owner taps "Sinkronkan sekarang", sends a kept row, or taps "Kirim" on a queued typed capture while auto-sync is off
- **THEN** the send is queued, and "Sinkronkan sekarang" also queues the resend of typed captures

#### Scenario: No account connected
- **WHEN** auto-sync is on but no server address and API key are set
- **THEN** a capture, a typed capture or a reconnect starts no send
