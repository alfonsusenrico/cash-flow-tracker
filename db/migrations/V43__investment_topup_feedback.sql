-- The owner's answers to top-up suggestions (clarify-review-and-learn-topups D6). Confirmations themselves
-- are derived from investment_topups.converted_from, so only what exists nowhere else is stored here.
-- Additive: older code ignores the table; dropping it loses only the stored answers.
CREATE TABLE IF NOT EXISTS investment_topup_feedback (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    -- An answer has no meaning without the debit it was about.
    transaction_id UUID NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
    source_account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    amount BIGINT NOT NULL CHECK (amount > 0),
    outcome VARCHAR(10) NOT NULL CHECK (outcome IN ('accepted', 'corrected', 'rejected')),
    suggested_target_account_id UUID NULL REFERENCES accounts(id) ON DELETE CASCADE,
    chosen_target_account_id UUID NULL REFERENCES accounts(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Accepting or correcting chose a product; a rejection chose none.
    CHECK ((outcome = 'rejected') = (chosen_target_account_id IS NULL)),
    CHECK (outcome <> 'accepted' OR suggested_target_account_id = chosen_target_account_id),
    CHECK (outcome <> 'corrected' OR (suggested_target_account_id IS NOT NULL
                                      AND suggested_target_account_id <> chosen_target_account_id))
);

CREATE INDEX IF NOT EXISTS idx_investment_topup_feedback_pattern
    ON investment_topup_feedback (user_id, source_account_id, amount);
CREATE INDEX IF NOT EXISTS idx_investment_topup_feedback_transaction
    ON investment_topup_feedback (user_id, transaction_id, created_at DESC);
