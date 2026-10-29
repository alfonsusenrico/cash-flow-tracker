## 1. Suggestions

- [x] 1.1 `services/capture_suggestions.py` candidates, grouping and score (D1, D2). Evidence: tests for the morning habit, notification exclusion, undone skip, the 90-day window and the 6-item cap.
- [x] 1.2 `GET /api/capture/suggestions` route with validation (D3). Evidence: 422/401 tests and an owner-isolation test.

Expected Result: the capture screen can prefill from the owner's habits.

## 2. Verify and deliver

- [x] 2.1 Full backend suite on a fresh database, `openspec validate --all --strict`, and verification.md.
- [ ] 2.2 Archive, and push `feat/ai-text-capture` (same PR as the capture backend).
