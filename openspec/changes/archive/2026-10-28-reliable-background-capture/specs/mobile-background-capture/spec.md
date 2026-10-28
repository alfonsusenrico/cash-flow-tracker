# mobile-background-capture Specification

## Purpose
Submit captured notifications and deliver their results promptly without depending on deferrable background jobs, and leave on-device evidence of listener disconnects.

## ADDED Requirements

### Requirement: Direct Submission After Capture
After storing a captured notification, the companion app SHALL submit pending events and check their results from the capturing process without waiting for a scheduled background job, SHALL deliver recorded alerts as results arrive, and SHALL keep the scheduled job as a fallback when the direct attempt fails.

#### Scenario: Capture while the app is in the background
- **WHEN** a bank notification is captured while the app's screen is not open and the network is available
- **THEN** it is submitted and, once the server records it, the recorded alert appears without opening the app

#### Scenario: Network unavailable at capture
- **WHEN** the direct submission fails because there is no connection
- **THEN** the event stays pending and the scheduled job submits it when connectivity returns

### Requirement: Single Submission at a Time
The companion app SHALL NOT run two submissions of pending events concurrently in one process.

#### Scenario: Direct and scheduled submission overlap
- **WHEN** a scheduled submission starts while a direct submission is running
- **THEN** it waits for the direct submission to finish and submits only what is still pending

### Requirement: Listener Health Record
The companion app SHALL record listener connect and disconnect times, the number of disconnects, the last capture time, and the last direct submission time and outcome, SHALL show them in Settings, and SHALL request that Android rebind the listener after a disconnect or when opened while disconnected.

#### Scenario: Listener disconnected overnight
- **WHEN** the listener disconnects and later reconnects
- **THEN** Settings shows the disconnect and reconnect times and the disconnect count increases
