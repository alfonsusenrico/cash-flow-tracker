-- Habit learning (learn-habits-to-ask-less): a habit classifier for transactions, and the classifier
-- and review state of each labelled example so that only the owner's own answers become evidence.
-- Additive and repeatable; Flyway applies the file in one transaction. Adding columns and replacing a
-- check constraint never updates a feedback row, so the append-only trigger is not involved.

ALTER TABLE transactions DROP CONSTRAINT IF EXISTS chk_transactions_classified_by;
ALTER TABLE transactions ADD CONSTRAINT chk_transactions_classified_by CHECK (
    classified_by IS NULL OR classified_by IN ('rule', 'ai', 'owner', 'system', 'habit')
);

-- NULL on rows written before this migration; the habit evidence query handles them conservatively.
ALTER TABLE classification_feedback ADD COLUMN IF NOT EXISTS before_classified_by VARCHAR(10) NULL;
ALTER TABLE classification_feedback ADD COLUMN IF NOT EXISTS after_classified_by VARCHAR(10) NULL;
ALTER TABLE classification_feedback ADD COLUMN IF NOT EXISTS after_review_status VARCHAR(20) NULL;

CREATE INDEX IF NOT EXISTS idx_classification_feedback_habit_key
    ON classification_feedback (user_id, account_id, text_signature);
CREATE INDEX IF NOT EXISTS idx_classification_feedback_transaction
    ON classification_feedback (user_id, transaction_id, created_at DESC);
