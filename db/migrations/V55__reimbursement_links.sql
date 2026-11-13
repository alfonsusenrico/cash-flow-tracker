-- A reimbursement's links to the exact expenses it repays (link-reimbursements-to-expenses D1).
-- `amount` is the part of the reimbursement allocated to the expense, recomputed whenever either side changes.
CREATE TABLE IF NOT EXISTS reimbursement_links (
    reimbursement_id UUID NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
    expense_id UUID NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    amount BIGINT NOT NULL DEFAULT 0 CHECK (amount >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (reimbursement_id, expense_id),
    CHECK (reimbursement_id <> expense_id)
);

CREATE INDEX IF NOT EXISTS idx_reimbursement_links_expense ON reimbursement_links (expense_id);
CREATE INDEX IF NOT EXISTS idx_reimbursement_links_user ON reimbursement_links (user_id);
