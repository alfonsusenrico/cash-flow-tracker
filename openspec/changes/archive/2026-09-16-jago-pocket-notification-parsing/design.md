## Context

Bank Jago produces multiple notification formats for pocket transactions depending on whether money is moved between two child pockets or moved in/out of a child pocket from/to the main account balance.
In the Android companion app, notifications are filtered by a strict whitelist of known patterns. In the backend, `parse_notification` and `routers/ingest.py` extract transaction metadata and reconcile pocket names against the user's accounts table.

## Goals / Non-Goals

**Goals:**
- Recognize Bank Jago single-pocket movements ("out of your <Pocket> Pocket" and "into your <Pocket> Pocket") in both Kotlin and Python parsers.
- Retain strict whitelist policy: do not re-introduce blacklists or catch-all noise matchers.
- Intelligently map pocket names using Indonesian/English synonyms (e.g. "Emergency Fund" -> "Dana Darurat") and exact/substring matches.
- In `routers/ingest.py`, route single-pocket outbound moves as a transfer from the child pocket to the parent Jago account, and single-pocket inbound moves as a transfer from the parent Jago account to the child pocket.

**Non-Goals:**
- Modifying database schemas or tables (existing schema supports accounts, child pockets, and transfers).
- Creating AI-slop or heuristic-based fuzzy LLM categorization when deterministic matching suffices.

## Decisions

### 1. Regex Expansion in Kotlin (`NotificationProcessor.kt`)
Replace single dual-pocket regex with 3 explicit whitelist patterns:
1. `jagoDualPocketRegex`: handles `[You've moved] Rp<amt> [has been moved] from your <Source> Pocket to your <Target> Pocket`
2. `jagoOutPocketRegex`: handles `[You've moved] Rp<amt> [has been moved] out of your <Source> Pocket` (target = "Kantong Utama", direction = "internal")
3. `jagoInPocketRegex`: handles `[You've moved] Rp<amt> [has been moved] into your <Target> Pocket` (source = "Kantong Utama", direction = "internal")

### 2. Backend Pattern Matching in `notification_parser.py`
Add corresponding compiled regex patterns `JAGO_DUAL_POCKET_PATTERN`, `JAGO_OUT_POCKET_PATTERN`, and `JAGO_IN_POCKET_PATTERN`. Set `source_pocket` and `target_pocket` appropriately with "Kantong Utama" representing the parent account.

### 3. Pocket Resolution and Account Linking in `routers/ingest.py`
Enhance `_match_pocket_account` to:
1. Match child accounts by exact and substring match.
2. Apply synonym dictionary (e.g. `emergency fund` / `emergency` -> `dana darurat`).
3. Fallback "Kantong Utama" or "Main" to parent account.
4. When transferring out of a pocket (`source_pocket`), destination defaults to `matched_account["id"]`. When transferring into a pocket (`target_pocket`), source defaults to `matched_account["id"]`.

## Risks / Trade-offs

- [Risk: Pocket name mismatch] If user creates a pocket with a completely unmapped name → Mitigation: Fallback to parent Jago account so the transaction is never dropped, or dynamically match closest child pocket.
- [Risk: Multiple notifications for one transfer] Bank Jago only issues one push notification per move. Deduplication is enforced by `payload_hash`.
