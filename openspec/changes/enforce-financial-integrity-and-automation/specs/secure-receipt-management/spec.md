# Spec Delta

## Purpose

Defines safe, authenticated receipt attachment behavior so uploaded evidence cannot bypass ownership, storage, type, or size boundaries.

## ADDED Requirements

### Requirement: Validated Receipt Upload
The system SHALL verify transaction ownership before persisting receipt bytes, SHALL enforce the configured maximum size, and SHALL accept only content whose bytes validate as a supported image or PDF type. Client filenames and declared MIME types SHALL NOT determine stored type or path.

#### Scenario: Rejecting an invalid receipt without storage residue
- **WHEN** an authenticated user uploads unsupported or oversized content to a transaction
- **THEN** the system rejects the upload with an actionable validation response
- **AND** no file or receipt metadata is created

#### Scenario: Rejecting upload to another user's transaction
- **WHEN** a user uploads a valid receipt using a transaction ID they do not own
- **THEN** the system returns not found without writing a file

### Requirement: Receipt Replacement and Cleanup
The system SHALL store receipts under generated safe paths, replace receipt metadata atomically, remove the superseded file only after a successful replacement, and remove owned receipt files when their transaction is deleted.

#### Scenario: Replacing an existing receipt
- **WHEN** a user uploads a second valid receipt for the same transaction
- **THEN** the new receipt becomes the only active attachment
- **AND** the old stored file is removed after the replacement commits
