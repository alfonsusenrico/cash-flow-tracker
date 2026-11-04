## MODIFIED Requirements

### Requirement: Tanya Page
The web app SHALL NOT offer a Tanya page. A request for `/tanya` SHALL redirect permanently to Beranda, and no navigation, shortcut or help entry SHALL name Tanya. Recording a typed transaction SHALL stay available through Catat (`text-capture`).

#### Scenario: Opening Tanya
- **WHEN** a signed-in owner opens a bookmarked `/tanya`
- **THEN** the response SHALL be a permanent redirect to `/`, and Beranda SHALL open without an error

#### Scenario: Safe-to-spend unavailable
- **WHEN** the owner looks for the safe amount that Tanya's lead line used to show
- **THEN** it SHALL be on Beranda as "Aman dipakai sampai gajian", and no page SHALL show Rp 0 in its place

## REMOVED Requirements

### Requirement: Deterministic Question Answers
**Reason**: The owner found that Tanya could only record and not answer questions, so the name misled (2026-11-04). The page and its intent code are retired (`refine-workspace-layout` D10).
**Migration**: Typed transactions are recorded with Catat. Free-form questions wait for a separately approved `add-tanya-llm-answers`.

### Requirement: Waiting Money Stays Counted
**Reason**: The owner found that Tanya could only record and not answer questions, so the name misled (2026-11-04). The page and its intent code are retired (`refine-workspace-layout` D10).
**Migration**: Typed transactions are recorded with Catat. Free-form questions wait for a separately approved `add-tanya-llm-answers`.

### Requirement: Unknown Text Gets Choices, Not Guesses
**Reason**: The owner found that Tanya could only record and not answer questions, so the name misled (2026-11-04). The page and its intent code are retired (`refine-workspace-layout` D10).
**Migration**: Typed transactions are recorded with Catat. Free-form questions wait for a separately approved `add-tanya-llm-answers`.

### Requirement: Recording From the Composer
**Reason**: The owner found that Tanya could only record and not answer questions, so the name misled (2026-11-04). The page and its intent code are retired (`refine-workspace-layout` D10).
**Migration**: Typed transactions are recorded with Catat. Free-form questions wait for a separately approved `add-tanya-llm-answers`.

### Requirement: Amounts Are Never Guessed
**Reason**: The owner found that Tanya could only record and not answer questions, so the name misled (2026-11-04). The page and its intent code are retired (`refine-workspace-layout` D10).
**Migration**: Typed transactions are recorded with Catat. Free-form questions wait for a separately approved `add-tanya-llm-answers`.

### Requirement: Capture Follow-up and Undo in the Thread
**Reason**: The owner found that Tanya could only record and not answer questions, so the name misled (2026-11-04). The page and its intent code are retired (`refine-workspace-layout` D10).
**Migration**: Typed transactions are recorded with Catat. Free-form questions wait for a separately approved `add-tanya-llm-answers`.

### Requirement: Conversation Kept Only in Memory
**Reason**: The owner found that Tanya could only record and not answer questions, so the name misled (2026-11-04). The page and its intent code are retired (`refine-workspace-layout` D10).
**Migration**: Typed transactions are recorded with Catat. Free-form questions wait for a separately approved `add-tanya-llm-answers`.

### Requirement: No Model Calls From Tanya
**Reason**: The owner found that Tanya could only record and not answer questions, so the name misled (2026-11-04). The page and its intent code are retired (`refine-workspace-layout` D10).
**Migration**: Typed transactions are recorded with Catat. Free-form questions wait for a separately approved `add-tanya-llm-answers`.

### Requirement: Owner-Scoped Answers
**Reason**: The owner found that Tanya could only record and not answer questions, so the name misled (2026-11-04). The page and its intent code are retired (`refine-workspace-layout` D10).
**Migration**: Typed transactions are recorded with Catat. Free-form questions wait for a separately approved `add-tanya-llm-answers`.

### Requirement: Failures Without False Figures
**Reason**: The owner found that Tanya could only record and not answer questions, so the name misled (2026-11-04). The page and its intent code are retired (`refine-workspace-layout` D10).
**Migration**: Typed transactions are recorded with Catat. Free-form questions wait for a separately approved `add-tanya-llm-answers`.

### Requirement: Accessible Conversation
**Reason**: The owner found that Tanya could only record and not answer questions, so the name misled (2026-11-04). The page and its intent code are retired (`refine-workspace-layout` D10).
**Migration**: Typed transactions are recorded with Catat. Free-form questions wait for a separately approved `add-tanya-llm-answers`.

### Requirement: Copy Register in Tanya
**Reason**: The owner found that Tanya could only record and not answer questions, so the name misled (2026-11-04). The page and its intent code are retired (`refine-workspace-layout` D10).
**Migration**: Typed transactions are recorded with Catat. Free-form questions wait for a separately approved `add-tanya-llm-answers`.

### Requirement: Phone Composition
**Reason**: The owner found that Tanya could only record and not answer questions, so the name misled (2026-11-04). The page and its intent code are retired (`refine-workspace-layout` D10).
**Migration**: Typed transactions are recorded with Catat. Free-form questions wait for a separately approved `add-tanya-llm-answers`.
